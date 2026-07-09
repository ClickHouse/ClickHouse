---
description: 'Documentação para DESCRIBE TABLE'
sidebar_label: 'DESCRIBE TABLE'
sidebar_position: 42
slug: /sql-reference/statements/describe-table
title: 'DESCRIBE TABLE'
doc_type: 'reference'
---

Retorna informações sobre as colunas da tabela.

**Sintaxe**

```sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

A instrução `DESCRIBE` retorna uma linha para cada coluna da tabela com os seguintes valores [String](../../sql-reference/data-types/string.md):

* `name` — O nome da coluna.
* `type` — O tipo da coluna.
* `default_type` — Uma cláusula usada na [expressão padrão](/pt-BR/sql-reference/statements/create/table) da coluna: `DEFAULT`, `MATERIALIZED` ou `ALIAS`. Se não houver expressão padrão, será retornada uma string vazia.
* `default_expression` — Uma expressão especificada após a cláusula `DEFAULT`.
* `comment` — Um [comentário de coluna](/pt-BR/sql-reference/statements/alter/column#comment-column).
* `codec_expression` — Um [codec](/pt-BR/sql-reference/statements/create/table#column_compression_codec) aplicado à coluna.
* `ttl_expression` — Uma expressão [TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
* `is_subcolumn` — Um sinalizador com valor `1` para subcolunas internas. Ele é incluído no resultado somente se a descrição de subcolunas estiver habilitada pela configuração [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

Todas as colunas em estruturas de dados [Nested](../../sql-reference/data-types/nested-data-structures/index.md) são descritas separadamente. O nome de cada coluna recebe como prefixo o nome da coluna pai e um ponto.

Para mostrar subcolunas internas de outros tipos de dados, use a configuração [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

**Exemplo**

```sql title="Query"
CREATE TABLE describe_example (
    id UInt64, text String DEFAULT 'unknown' CODEC(ZSTD),
    user Tuple (name String, age UInt8)
) ENGINE = MergeTree() ORDER BY id;

DESCRIBE TABLE describe_example;
DESCRIBE TABLE describe_example SETTINGS describe_include_subcolumns=1;
```

```text title="Response"
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id   │ UInt64                        │              │                    │         │                  │                │
│ text │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │
│ user │ Tuple(name String, age UInt8) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

A segunda consulta também exibe subcolunas:

```text title="Response"
┌─name──────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┬─is_subcolumn─┐
│ id        │ UInt64                        │              │                    │         │                  │                │            0 │
│ text      │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │            0 │
│ user      │ Tuple(name String, age UInt8) │              │                    │         │                  │                │            0 │
│ user.name │ String                        │              │                    │         │                  │                │            1 │
│ user.age  │ UInt8                         │              │                    │         │                  │                │            1 │
└───────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┴──────────────┘
```

A instrução DESCRIBE também pode ser usada com subconsultas ou expressões escalares:

```SQL
DESCRIBE SELECT 1 FORMAT TSV;
```

ou

```SQL
DESCRIBE (SELECT 1) FORMAT TSV;
```

```text title="Response"
1       UInt8
```

Essa forma de uso retorna metadados sobre as colunas de resultado da consulta ou subconsulta especificada. É útil para entender a estrutura de consultas complexas antes da execução.

**Veja também**

* configuração [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).