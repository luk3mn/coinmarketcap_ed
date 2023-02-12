# coinmarketcap_ed
Projeto de engenharia de dados desenvolvido no bootcamp de engenharia de dados da stack academy

### Cenário 1 - Coleta e Ingestão

1. Consumir dados reais (API)
    
    Foi utilizado o Python para receber esses dados da API e transformá-los em uma database relacional
    
    - Processar os dados → Recebe os dados e cria as tabelas para estruturar os dados.
    - Estruturar os dados → Organiza os dados em linhas e colunas para fazer a ingestão dos dados no database.
2. Gerir/Persistir os dados em um banco de dados relacional → RDS
    - Foi criado uma banco de dados relacional que recebe os dados estruturados e armazena eles utilizando o RDS.

### Cenário 2 - Armazenamento (Data Lake)

1. **`Raw`** → Armazena os dados no formato bruto
    1. Não é feito nenhum processamento nessa camada
    2. Não será entregue para os cientistas de dados
2. **`Processed`** → Será aplicado alguns processamentos
    1. Dados que serão entregue para os cientistas de dados consumirem os dados e trabalharem em novos processamentos
3. **`Curated`** → Dados acurados
    1. Dados que serão entregues para os analistas de negócio, stakeholders e afins…

### Cenário 3 - Processamento

- Levar os dados do RDS (postgresql) para os buckets S3
    - Carregamento dos dados brutos no **`Raw`**
        - Foi usado o DMS para realizar a migração dos dados persistidos no banco de dados para o bucket s3 no formato bruto.
    - Carregamento dos dados processados
        - Escrever o código com os processamentos necessários em apache spark
        - Criar o cluster com EMR para processar os dados e carregar nos buckets **`Processed`** e **`Curated`**.
            - Nesse caso, o código em spark indica qual bucket recebe os dados em diferentes níveis de processamento.

### Cenário 4 - Entrega dos dados

Nessa etapa, os dados serão entregues para os stakeholders em um formato legível para trabalharem análises de negócios

- O mesmo código de processamento escrito para carregar os dados nos buckets **`Processed`** e **`Curated`** será utilizado para carregar os dados no Data WareHouse utilizando o redshift. Nesse caso, será acrescentado uma função responsável por migrar esses dados processados dos buckets para o redshift usando os clusters criados com EMR
- o ***Redshift*** armazena esses dados processados para visualização mais otimizadas por analistas de negócio.
