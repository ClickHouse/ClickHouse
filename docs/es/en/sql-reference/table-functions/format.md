---
description: 'Analiza los datos de los argumentos según el formato de entrada especificado. Si no se especifica el argumento de estructura, se extrae de los datos.'
slug: /sql-reference/table-functions/format
sidebar_position: 65
sidebar_label: 'format'
title: 'format'
doc_type: 'reference'
---

Analiza los datos de los argumentos según el formato de entrada especificado. Si no se especifica el argumento de estructura, se extrae de los datos.

<div id="syntax">
  ## Sintaxis
</div>

```sql
format(format_name, [structure], data)
```

<div id="arguments">
  ## Argumentos
</div>

* `format_name` — El [formato](/es/sql-reference/formats) de los datos.
* `structure` - Estructura de la tabla. Opcional. Formato: &#39;column1&#95;name column1&#95;type, column2&#95;name column2&#95;type, ...&#39;.
* `data` — Literal de cadena o expresión constante que devuelve una cadena con datos en el formato especificado

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla con los datos del argumento `data`, analizados según el formato especificado y la estructura especificada o extraída.

<div id="examples">
  ## Ejemplos
</div>

Sin el argumento `structure`:

```sql title="Query"
SELECT * FROM format(JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

```response title="Response"
┌───b─┬─a─────┐
│ 111 │ Hello │
│ 123 │ World │
│ 112 │ Hello │
│ 124 │ World │
└─────┴───────┘
```

```sql title="Query"
DESC format(JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

```response title="Response"
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ b    │ Nullable(Float64) │              │                    │         │                  │                │
│ a    │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Con el argumento `structure`:

```sql title="Query"
SELECT * FROM format(JSONEachRow, 'a String, b UInt32',
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

```response title="Response"
┌─a─────┬───b─┐
│ Hello │ 111 │
│ World │ 123 │
│ Hello │ 112 │
│ World │ 124 │
└───────┴─────┘
```

<div id="related">
  ## Contenido relacionado
</div>

* [Formatos](../../interfaces/formats.md)