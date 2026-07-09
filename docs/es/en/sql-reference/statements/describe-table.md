---
description: 'Documentación de DESCRIBE TABLE'
sidebar_label: 'DESCRIBE TABLE'
sidebar_position: 42
slug: /sql-reference/statements/describe-table
title: 'DESCRIBE TABLE'
doc_type: 'reference'
---

Devuelve información sobre las columnas de la tabla.

**Sintaxis**

```sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

La sentencia `DESCRIBE` devuelve una fila por cada columna de la tabla con los siguientes valores [String](../../sql-reference/data-types/string.md):

* `name` — El nombre de una columna.
* `type` — El tipo de una columna.
* `default_type` — Una cláusula que se usa en la [expresión por defecto](/es/sql-reference/statements/create/table) de la columna: `DEFAULT`, `MATERIALIZED` o `ALIAS`. Si no hay una expresión por defecto, se devuelve una cadena vacía.
* `default_expression` — Una expresión especificada después de la cláusula `DEFAULT`.
* `comment` — Un [comentario de columna](/es/sql-reference/statements/alter/column#comment-column).
* `codec_expression` — Un [codec](/es/sql-reference/statements/create/table#column_compression_codec) que se aplica a la columna.
* `ttl_expression` — Una expresión [TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
* `is_subcolumn` — Un indicador que vale `1` para las subcolumnas internas. Solo se incluye en el resultado si la descripción de subcolumnas está habilitada mediante el ajuste [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

Todas las columnas de las estructuras de datos [Nested](../../sql-reference/data-types/nested-data-structures/index.md) se describen por separado. El nombre de cada columna va precedido del nombre de la columna padre y un punto.

Para mostrar las subcolumnas internas de otros tipos de datos, use el ajuste [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

**Ejemplo**

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

La segunda consulta también muestra subcolumnas:

```text title="Response"
┌─name──────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┬─is_subcolumn─┐
│ id        │ UInt64                        │              │                    │         │                  │                │            0 │
│ text      │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │            0 │
│ user      │ Tuple(name String, age UInt8) │              │                    │         │                  │                │            0 │
│ user.name │ String                        │              │                    │         │                  │                │            1 │
│ user.age  │ UInt8                         │              │                    │         │                  │                │            1 │
└───────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┴──────────────┘
```

La instrucción DESCRIBE también se puede usar con subconsultas o expresiones escalares:

```SQL
DESCRIBE SELECT 1 FORMAT TSV;
```

o

```SQL
DESCRIBE (SELECT 1) FORMAT TSV;
```

```text title="Response"
1       UInt8
```

Este uso proporciona metadatos sobre las columnas de resultado de la consulta o subconsulta especificada. Es útil para comprender la estructura de consultas complejas antes de ejecutarlas.

**Véase también**

* El ajuste [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).