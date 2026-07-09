---
slug: /sql-reference/statements/create/dictionary/sources/null
title: 'Origem de dicionário Null'
sidebar_position: 14
sidebar_label: 'Null'
description: 'Configure uma origem de dicionário Null (vazia) no ClickHouse para testes.'
doc_type: 'reference'
---

Uma origem especial que pode ser usada para criar dicionários de teste (vazios).
Dicionários de teste podem ser úteis para fins de teste ou em configurações com nós de dados e de consulta separados, com tabelas distribuídas.

```sql
CREATE DICTIONARY null_dict (
    id              UInt64,
    val             UInt8,
    default_val     UInt8 DEFAULT 123,
    nullable_val    Nullable(UInt8)
)
PRIMARY KEY id
SOURCE(NULL())
LAYOUT(FLAT())
LIFETIME(0);
```