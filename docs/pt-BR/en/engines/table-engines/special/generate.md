---
description: 'O motor de tabela GenerateRandom produz dados aleatórios para o
  esquema de tabela especificado.'
sidebar_label: 'GenerateRandom'
sidebar_position: 140
slug: /engines/table-engines/special/generate
title: 'Motor de tabela GenerateRandom'
doc_type: 'referência'
---

O motor de tabela GenerateRandom produz dados aleatórios para o esquema de tabela especificado.

Exemplos de uso:

* Use em testes para popular, de forma reproduzível, uma tabela grande.
* Gere dados de entrada aleatórios para testes de fuzzing.

<div id="usage-in-clickhouse-server">
  ## Uso no ClickHouse Server
</div>

```sql
ENGINE = GenerateRandom([random_seed [,max_string_length [,max_array_length]]])
```

Os parâmetros `max_array_length` e `max_string_length` especificam, respectivamente, o comprimento máximo de todas as
colunas de array ou map e das strings nos dados gerados.

O motor de tabela Generate oferece suporte apenas a consultas `SELECT`.

Ele oferece suporte a todos os [tipos de dados](../../../sql-reference/data-types/index.md) que podem ser armazenados em uma tabela, exceto `AggregateFunction`.

<div id="example">
  ## Exemplo
</div>

**1.** Crie a tabela `generate_engine_table`:

```sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** Consulte os dados:

```sql
SELECT * FROM generate_engine_table LIMIT 3
```

```text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

<div id="details-of-implementation">
  ## Detalhes da implementação
</div>

* Não há suporte para:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * `INSERT`
  * Índices
  * Replicação