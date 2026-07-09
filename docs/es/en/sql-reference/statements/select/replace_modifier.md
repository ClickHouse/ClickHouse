---
description: 'Documentación que describe el modificador APPLY, que permite invocar una función para cada fila devuelta por una expresión de tabla externa de una consulta.'
sidebar_label: 'REPLACE'
slug: /sql-reference/statements/select/replace-modifier
title: 'Modificador REPLACE'
keywords: ['REPLACE', 'modificador']
doc_type: 'referencia'
---

> Permite especificar uno o varios [alias de expresión](/es/sql-reference/syntax#expression-aliases).

Cada alias debe coincidir con el nombre de una columna de la sentencia `SELECT *`. En la lista de columnas de salida, la columna que coincide
con el alias se sustituye por la expresión de ese `REPLACE`.

Este modificador no cambia los nombres ni el orden de las columnas. Sin embargo, puede cambiar el valor y el tipo de valor.

**Sintaxis:**

```sql
SELECT <expr> REPLACE( <expr> AS col_name) from [db.]table_name
```

**Ejemplo:**

```sql
SELECT * REPLACE(i + 1 AS i) from columns_transformers;
```

```response
┌───i─┬──j─┬───k─┐
│ 101 │ 10 │ 324 │
│ 121 │  8 │  23 │
└─────┴────┴─────┘
```