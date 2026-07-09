---
description: 'Documentação do motor de tabela StripeLog'
slug: /engines/table-engines/log-family/stripelog
toc_priority: 32
toc_title: 'StripeLog'
title: 'Motor de tabela StripeLog'
doc_type: 'referência'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="stripelog-table-engine">
  # Motor de tabela StripeLog
</div>

<CloudNotSupportedBadge />

Este motor pertence à família de motores de log. Veja as propriedades comuns dos motores de log e suas diferenças no artigo [Família de motores Log](../../../engines/table-engines/log-family/index.md).

Use este motor em cenários em que você precisa gravar muitas tabelas com uma pequena quantidade de dados (menos de 1 milhão de linhas). Por exemplo, esta tabela pode ser usada para armazenar lotes de dados de entrada para transformação quando o processamento atômico deles for necessário. 100 mil instâncias desse tipo de tabela são viáveis em um servidor ClickHouse. Este motor de tabela deve ser preferido em vez de [Log](./log.md) quando for necessário um grande número de tabelas. Isso reduz a eficiência de leitura.

<div id="table_engines-stripelog-creating-a-table">
  ## Criando uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

Consulte a descrição detalhada da consulta [CREATE TABLE](/pt-BR/sql-reference/statements/create/table).

<div id="table_engines-stripelog-writing-the-data">
  ## Gravando os dados
</div>

O motor `StripeLog` armazena todas as colunas em um único arquivo. Para cada consulta `INSERT`, o ClickHouse anexa o bloco de dados ao final do arquivo da tabela, gravando as colunas uma a uma.

Para cada tabela, o ClickHouse grava os arquivos:

* `data.bin` — Arquivo de dados.
* `index.mrk` — Arquivo com marcas. As marcas contêm os deslocamentos de cada coluna de cada bloco de dados inserido.

O motor `StripeLog` não oferece suporte às operações `ALTER UPDATE` e `ALTER DELETE`.

<div id="table_engines-stripelog-reading-the-data">
  ## Lendo os dados
</div>

O arquivo com marcas permite que o ClickHouse paralelize a leitura dos dados. Isso significa que uma consulta `SELECT` retorna linhas em uma ordem imprevisível. Use a cláusula `ORDER BY` para ordenar as linhas.

<div id="table_engines-stripelog-example-of-use">
  ## Exemplo de uso
</div>

Criando uma tabela:

```sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

Inserção de dados:

```sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Usamos duas consultas `INSERT` para criar dois blocos de dados no arquivo `data.bin`.

O ClickHouse usa várias threads ao consultar dados. Cada thread lê um bloco de dados separado e retorna as linhas resultantes de forma independente, à medida que conclui a execução. Com isso, na maioria dos casos, a ordem dos blocos de linhas na saída não corresponde à ordem desses mesmos blocos na entrada. Por exemplo:

```sql
SELECT * FROM stripe_log_table
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

Ordenando os resultados (em ordem crescente por padrão):

```sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```