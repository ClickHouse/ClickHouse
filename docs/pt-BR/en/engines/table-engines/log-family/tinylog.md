---
description: 'Documentação do motor de tabela TinyLog'
slug: /engines/table-engines/log-family/tinylog
toc_priority: 34
toc_title: 'TinyLog'
title: 'Motor de tabela TinyLog'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="tinylog-table-engine">
  # Motor de tabela TinyLog
</div>

<CloudNotSupportedBadge />

Esse motor pertence à família de motores Log. Consulte [Família de motores Log](../../../engines/table-engines/log-family/index.md) para ver as propriedades comuns dos motores Log e suas diferenças.

Esse motor de tabela normalmente é usado com o método de gravação única: os dados são gravados uma vez e depois lidos quantas vezes forem necessárias. Por exemplo, você pode usar tabelas do tipo `TinyLog` para dados intermediários processados em pequenos lotes. Observe que armazenar dados em um grande número de tabelas pequenas é ineficiente.

As consultas são executadas em um único fluxo. Em outras palavras, esse motor foi projetado para tabelas relativamente pequenas (até cerca de 1.000.000 de linhas). Faz sentido usar esse motor de tabela se você tiver muitas tabelas pequenas, já que ele é mais simples que o motor [Log](../../../engines/table-engines/log-family/log.md) (é preciso abrir menos arquivos).

<div id="characteristics">
  ## Características
</div>

* **Estrutura mais simples**: Ao contrário do motor Log, o TinyLog não usa arquivos de marcação. Isso reduz a complexidade, mas também limita as otimizações de desempenho para datasets grandes.
* **Consultas em fluxo único**: As consultas em tabelas TinyLog são executadas em um único fluxo, o que o torna adequado para tabelas relativamente pequenas, normalmente com até 1.000.000 linhas.
* **Eficiente para tabelas pequenas**: A simplicidade do motor TinyLog o torna vantajoso para gerenciar muitas tabelas pequenas, pois exige menos operações em arquivos em comparação com o motor Log.

Ao contrário do motor Log, o TinyLog não usa arquivos de marcação. Isso reduz a complexidade, mas também limita as otimizações de desempenho para datasets maiores.

<div id="table_engines-tinylog-creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = TinyLog
```

Veja a descrição detalhada da consulta [CREATE TABLE](/pt-BR/sql-reference/statements/create/table).

<div id="table_engines-tinylog-writing-the-data">
  ## Gravando os dados
</div>

O motor `TinyLog` armazena todas as colunas em um único arquivo. Para cada consulta `INSERT`, o ClickHouse anexa o bloco de dados ao final de um arquivo da tabela, gravando as colunas uma a uma.

Para cada tabela, o ClickHouse grava os arquivos:

* `<column>.bin`: um arquivo de dados para cada coluna, contendo os dados serializados e comprimidos.

O motor `TinyLog` não oferece suporte às operações `ALTER UPDATE` e `ALTER DELETE`.

<div id="table_engines-tinylog-example-of-use">
  ## Exemplo de uso
</div>

Criando uma tabela:

```sql
CREATE TABLE tiny_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = TinyLog
```

Inserção de dados:

```sql
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Usamos duas consultas `INSERT` para criar dois blocos de dados dentro dos arquivos `<column>.bin`.

O ClickHouse usa um único fluxo para selecionar os dados. Como resultado, a ordem dos blocos de linhas na saída corresponde à ordem desses mesmos blocos na entrada. Por exemplo:

```sql
SELECT * FROM tiny_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2024-12-10 13:11:58 │ REGULAR      │ The first regular message  │
│ 2024-12-10 13:12:12 │ REGULAR      │ The second regular message │
│ 2024-12-10 13:12:12 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```