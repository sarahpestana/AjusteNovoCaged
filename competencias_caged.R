# Realizar ajuste do Novo caged para o Maranhão

setwd()
rm(list=ls())

## carregar pacotes
library(tidyverse)
library(archive)
library(janitor)
library(readr)
library(dplyr)
--------------------------------------------------------------------------------
## Aplicando competência e período
competencias <- c('MOV', 'FOR', 'EXC')
anos <- 2020:2024 
meses <- formatC(1:12, width = 2, flag = '0')
--------------------------------------------------------------------------------
## Aplicando Loop de comando, rodar somente se não tiver todos os arquivos

  for(i in seq_along(competencias)){
    
    for(j in seq_along(anos)){
      
      for(k in seq_along(meses)){
        
        cat(competencias[i], anos[j], 'mês', meses[k], '\n')
        
        # Nome do arquivo a ser baixado
        nome_arquivo <- paste0("CAGED", competencias[i], anos[j], meses[k], '.7z')
        
        # Verificar se o arquivo já existe na pasta
        if (!file.exists(nome_arquivo)) {
          tryCatch({
            download.file(
              paste0("ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/",
                     anos[j], '/', anos[j], meses[k],
                     "/CAGED", competencias[i], anos[j],
                     meses[k],
                     ".7z"),
              quiet = TRUE,
              destfile = nome_arquivo,
              mode = "wb")
          },
          error = function(err) { warning("file could not be downloaded") })
        } else {
          cat("O arquivo", nome_arquivo, "já existe na pasta. Pulando o download.\n")
        }
      }
    }
  }
--------------------------------------------------------------------------------
## Unindo os arquivos de mesma natureza 

for(k in seq_along(competencias)){
  
  assign(tolower(paste0('caged', competencias[k], '_baixadas')),
         fs::dir_ls(glob = paste0('CAGED', competencias[k], '*.7z$')))
  
  assign(tolower(paste0('caged', competencias[k], '_lista')),
         list())
}
--------------------------------------------------------------------------------
## Arquivos

cagedmov_baixadas <-
  fs::dir_ls(glob = "CAGEDMOV*.7z$")

cagedmov_lista <- data.frame()            

for(i in seq_along(cagedmov_baixadas)){
  
  cat('Lendo periodo', i, '\n')
  readr::read_csv2(archive::archive_read(cagedmov_baixadas[i])) |>
    janitor::clean_names() |>
    dplyr::filter(uf == 21) ->
    cagedfor_lista[[i]]
}
--------------------------------------------------------------------------------
## Consolidar em 1 tiblle MOV FOR EXC e filtrar MA

arquivos_caged <- function(entrada) {
  
  if(!any(entrada == c('MOV', 'FOR', 'EXC'))) {
    stop("Competencia deve ser FOR, MOV ou EXC", call. = FALSE)
  }
  
  caminho_dos_arquivos <- get(paste0('caged', tolower(entrada), '_baixadas'))
  lista_arquivos_periodo <- vector(mode = 'list', length = length(caminho_dos_arquivos))
  
  for(l in seq_along(caminho_dos_arquivos)) {
    
    cat('Carregando arquivo', caminho_dos_arquivos[l],
        ' | loop', l, 'de', length(caminho_dos_arquivos), '\n')
    
    arquivo <- readr::read_csv2(archive::archive_read(caminho_dos_arquivos[l])) |>
      janitor::clean_names() |>
      dplyr::filter(uf == 21) %>% 
      mutate(salario = as.numeric(salario),
             horascontratuais = as.numeric(horascontratuais),
             valorsalariofixo = as.numeric(valorsalariofixo))
    
    lista_arquivos_periodo[[l]] <- arquivo
  }
  
# Concatenar os dataframes da lista em um único dataframe
df_final <- dplyr::bind_rows(lista_arquivos_periodo)
  return(df_final)
}
--------------------------------------------------------------------------------
##Extrair competências

# Competência 'MOV'
df_mov <- arquivos_caged('MOV')

# Competência 'FOR'
df_for <- arquivos_caged('FOR')

# Competência 'EXC'
df_exc <- arquivos_caged('EXC')
--------------------------------------------------------------------------------
## Obter saldo de movimentação
saldo_mov <- df_mov %>%
  group_by(competenciamov,municipio,secao,subclasse,cbo2002ocupacao,graudeinstrucao,
           idade,racacor,sexo,tamestabjan, tipodedeficiencia) %>%
  summarise(saldo = sum(saldomovimentacao))

#Obter saldo de fora do prazo 
saldo_for <- df_for %>%
  group_by(competenciamov,municipio,secao,subclasse,cbo2002ocupacao,graudeinstrucao,
           idade,racacor,sexo,tamestabjan,tipodedeficiencia) %>% 
  summarise(saldo = sum(saldomovimentacao))

#Obter saldo de excluidos 
saldo_exc <- df_exc %>%
  group_by(competenciamov,municipio,secao,subclasse,cbo2002ocupacao,graudeinstrucao,
           idade,racacor,sexo,tamestabjan,tipodedeficiencia) %>% 
  summarise(saldo = sum(saldomovimentacao)) 
--------------------------------------------------------------------------------
# Somar os saldos de movimentação e fora do prazo pela competência
saldo_soma <- bind_rows(
  mutate(saldo_mov, competencia = as.character(competenciamov)),
  mutate(saldo_for, competencia = as.character(competenciamov)),
) %>%
  group_by(competenciamov,municipio,secao,subclasse,cbo2002ocupacao,graudeinstrucao,
           idade,racacor,sexo,tamestabjan,tipodedeficiencia) %>%
  summarise(saldo = sum(saldo, na.rm = TRUE))
--------------------------------------------------------------------------------
## Calcular saldo ajustado
saldo_ajustado <- left_join(saldo_soma, saldo_exc, 
                            by = c("competenciamov","municipio","secao",
                                   "subclasse","cbo2002ocupacao","graudeinstrucao",
                                   "idade","racacor","sexo","tamestabjan", "tipodedeficiencia")) %>%
  mutate(saldo_ajuste = saldo.x - coalesce(saldo.y, 0)) %>%
  select(competenciamov,municipio, secao,subclasse,cbo2002ocupacao,graudeinstrucao,
         idade,racacor,sexo,tamestabjan,tipodedeficiencia,saldo_ajuste)

# Calcular saldo ajustado - serie histórica para o Maranhão
saldo_serie <- saldo_ajustado %>%
  group_by(competenciamov) %>%
  summarise(saldo_serie = sum(saldo_ajuste))





