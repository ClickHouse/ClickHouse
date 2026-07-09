---
description: 'Documentação do motor de tabela Log'
slug: /engines/table-engines/log-family/log
toc_priority: 33
toc_title: 'Log'
title: 'Motor de tabela Log'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine">
  # Motor de tabela Log
</div>

<CloudNotSupportedBadge />

O motor pertence à família de motores `Log`. Consulte as propriedades comuns dos motores `Log` e suas diferenças no artigo [Família de motores Log](../../../engines/table-engines/log-family/index.md).

`Log` difere de [TinyLog](../../../engines/table-engines/log-family/tinylog.md) por manter um pequeno arquivo de &quot;marcas&quot; junto aos arquivos de coluna. Essas marcas são gravadas em cada bloco de dados e contêm deslocamentos que indicam onde começar a ler o arquivo para pular o número especificado de linhas. Isso possibilita ler os dados da tabela em múltiplas threads.
Para acesso concorrente aos dados, as operações de leitura podem ser executadas simultaneamente, enquanto as operações de gravação bloqueiam as leituras e umas às outras.
O motor `Log` não oferece suporte a índices. Da mesma forma, se a gravação em uma tabela falhar, a tabela fica comprometida, e a leitura dela retorna um erro. O motor `Log` é apropriado para dados temporários, tabelas de gravação única e fins de teste ou demonstração.

<div id="table_engines-log-creating-a-table">
  ## Criando uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Log
```

Consulte a descrição detalhada da consulta [CREATE TABLE](/pt-BR/sql-reference/statements/create/table).

<div id="table_engines-log-writing-the-data">
  ## Gravando os dados
</div>

O motor `Log` armazena os dados com eficiência, gravando cada coluna em seu próprio arquivo.  Para cada tabela, o motor Log grava os seguintes arquivos no caminho de armazenamento especificado:

* `<column>.bin`: um arquivo de dados para cada coluna, contendo os dados serializados e comprimidos.
  `__marks.mrk`: um arquivo de marcas, que armazena offsets e contagens de linhas de cada bloco de dados inserido. As marcas são usadas para tornar a execução de consultas mais eficiente, permitindo que o motor ignore blocos de dados irrelevantes durante as leituras.

<div id="writing-process">
  ### Processo de gravação
</div>

Quando os dados são gravados em uma tabela `Log`:

1. Os dados são serializados e comprimidos em blocos.
2. Para cada coluna, os dados comprimidos são anexados ao respectivo arquivo `<column>.bin`.
3. Entradas correspondentes são adicionadas ao arquivo `__marks.mrk` para registrar o offset e a contagem de linhas dos dados recém-inseridos.

<div id="table_engines-log-reading-the-data">
  ## Lendo os dados
</div>

O arquivo de marcas permite que o ClickHouse paralelize a leitura dos dados. Isso significa que uma consulta `SELECT` retorna linhas em uma ordem imprevisível. Use a cláusula `ORDER BY` para ordenar as linhas.

<div id="table_engines-log-example-of-use">
  ## Exemplo de uso
</div>

Criação de uma tabela:

```sql
CREATE TABLE log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = Log
```

Inserção de dados:

```sql
INSERT INTO log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Usamos duas consultas `INSERT` para criar dois blocos de dados nos arquivos `<column>.bin`.

O ClickHouse usa várias threads ao selecionar dados. Cada thread lê um bloco de dados separado e retorna as linhas resultantes de forma independente, assim que termina. Como resultado, a ordem dos blocos de linhas na saída pode não corresponder à ordem desses mesmos blocos na entrada. Por exemplo:

```sql
SELECT * FROM log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

Classificando os resultados (em ordem crescente por padrão):

```sql
SELECT * FROM log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```