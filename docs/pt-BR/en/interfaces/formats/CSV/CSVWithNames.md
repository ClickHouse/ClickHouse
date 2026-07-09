---
alias: []
description: 'Documentação do formato CSV'
input_format: true
keywords: ['CSVWithNames']
output_format: true
slug: /interfaces/formats/CSVWithNames
title: 'CSVWithNames'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Também imprime a linha de cabeçalho com os nomes das colunas, semelhante a [TabSeparatedWithNames](/pt-BR/interfaces/formats/TabSeparatedWithNames).

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="inserting-data">
  ### Inserindo dados
</div>

:::tip
A partir da [versão](https://github.com/ClickHouse/ClickHouse/releases) 23.1, o ClickHouse detecta automaticamente os cabeçalhos em arquivos CSV ao usar o formato `CSV`, portanto não é necessário usar `CSVWithNames` nem `CSVWithNamesAndTypes`.
:::

Use o seguinte arquivo CSV, chamado `football.csv`:

```csv
date,season,home_team,away_team,home_team_goals,away_team_goals
2022-04-30,2021,Sutton United,Bradford City,1,4
2022-04-30,2021,Swindon Town,Barrow,2,1
2022-04-30,2021,Tranmere Rovers,Oldham Athletic,2,0
2022-05-02,2021,Salford City,Mansfield Town,2,2
2022-05-02,2021,Port Vale,Newport County,1,2
2022-05-07,2021,Barrow,Northampton Town,1,3
2022-05-07,2021,Bradford City,Carlisle United,2,0
2022-05-07,2021,Bristol Rovers,Scunthorpe United,7,0
2022-05-07,2021,Exeter City,Port Vale,0,1
2022-05-07,2021,Harrogate Town A.F.C.,Sutton United,0,2
2022-05-07,2021,Hartlepool United,Colchester United,0,2
2022-05-07,2021,Leyton Orient,Tranmere Rovers,0,1
2022-05-07,2021,Mansfield Town,Forest Green Rovers,2,2
2022-05-07,2021,Newport County,Rochdale,0,2
2022-05-07,2021,Oldham Athletic,Crawley Town,3,3
2022-05-07,2021,Stevenage Borough,Salford City,4,2
2022-05-07,2021,Walsall,Swindon Town,0,3
```

Crie uma tabela:

```sql
CREATE TABLE football
(
    `date` Date,
    `season` Int16,
    `home_team` LowCardinality(String),
    `away_team` LowCardinality(String),
    `home_team_goals` Int8,
    `away_team_goals` Int8
)
ENGINE = MergeTree
ORDER BY (date, home_team);
```

Insira os dados usando o formato `CSVWithNames`:

```sql
INSERT INTO football FROM INFILE 'football.csv' FORMAT CSVWithNames;
```

<div id="reading-data">
  ### Leitura de dados
</div>

Leia os dados usando o formato `CSVWithNames`:

```sql
SELECT *
FROM football
FORMAT CSVWithNames
```

A saída será um CSV com uma única linha de cabeçalho:

```csv
"date","season","home_team","away_team","home_team_goals","away_team_goals"
"2022-04-30",2021,"Sutton United","Bradford City",1,4
"2022-04-30",2021,"Swindon Town","Barrow",2,1
"2022-04-30",2021,"Tranmere Rovers","Oldham Athletic",2,0
"2022-05-02",2021,"Port Vale","Newport County",1,2
"2022-05-02",2021,"Salford City","Mansfield Town",2,2
"2022-05-07",2021,"Barrow","Northampton Town",1,3
"2022-05-07",2021,"Bradford City","Carlisle United",2,0
"2022-05-07",2021,"Bristol Rovers","Scunthorpe United",7,0
"2022-05-07",2021,"Exeter City","Port Vale",0,1
"2022-05-07",2021,"Harrogate Town A.F.C.","Sutton United",0,2
"2022-05-07",2021,"Hartlepool United","Colchester United",0,2
"2022-05-07",2021,"Leyton Orient","Tranmere Rovers",0,1
"2022-05-07",2021,"Mansfield Town","Forest Green Rovers",2,2
"2022-05-07",2021,"Newport County","Rochdale",0,2
"2022-05-07",2021,"Oldham Athletic","Crawley Town",3,3
"2022-05-07",2021,"Stevenage Borough","Salford City",4,2
"2022-05-07",2021,"Walsall","Swindon Town",0,3
```

<div id="format-settings">
  ## Configurações de formato
</div>

:::note
Se a configuração [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) estiver definida como `1`,
as colunas dos dados de entrada serão mapeadas para as colunas da tabela pelos respectivos nomes, e as colunas com nomes desconhecidos serão ignoradas se a configuração [input&#95;format&#95;skip&#95;unknown&#95;fields](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) estiver definida como `1`.
Caso contrário, a primeira linha será ignorada.
:::