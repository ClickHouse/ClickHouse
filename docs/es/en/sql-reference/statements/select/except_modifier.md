---
description: 'Documentación que describe el modificador EXCEPT, que especifica los nombres de una o más columnas que se excluirán del resultado. Todos los nombres de columna correspondientes se omiten de la salida.'
sidebar_label: 'EXCEPT'
slug: /sql-reference/statements/select/except-modifier
title: 'Modificador EXCEPT'
keywords: ['EXCEPT', 'modificador']
doc_type: 'reference'
---

> Especifica los nombres de una o más columnas que se excluirán del resultado. Todos los nombres de columna correspondientes se omiten de la salida.

<div id="syntax">
  ## Sintaxis
</div>

```sql
SELECT <expr> EXCEPT ( col_name1 [, col_name2, col_name3, ...] ) FROM [db.]table_name
```

<div id="examples">
  ## Ejemplos
</div>

```sql title="Query"
SELECT * EXCEPT (i) from columns_transformers;
```

```response title="Response"
┌──j─┬───k─┐
│ 10 │ 324 │
│  8 │  23 │
└────┴─────┘
```