---
slug: /sql-reference/statements/create/dictionary/sources/null
title: 'Fuente de diccionario Null'
sidebar_position: 14
sidebar_label: 'Null'
description: 'Configure una fuente de diccionario Null (vacía) en ClickHouse para realizar pruebas.'
doc_type: 'reference'
---

Una fuente especial que puede usarse para crear diccionarios ficticios (vacíos).
Los diccionarios ficticios pueden ser útiles para realizar pruebas o en configuraciones con nodos de datos y de consulta separados, con tablas distribuidas.

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