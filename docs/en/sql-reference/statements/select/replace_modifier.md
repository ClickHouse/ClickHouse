---
description: 'Documentation describing the APPLY modifier which allows you to invoke some function for each row returned by an outer table expression of a query.'
sidebar_label: 'REPLACE'
slug: /sql-reference/statements/select/replace-modifier
title: 'Replace modifier'
keywords: ['REPLACE', 'modifier']
doc_type: 'reference'
---

# Replace modifier {#replace}

> Allows you to specify one or more [expression aliases](/sql-reference/syntax#expression-aliases). 

Each alias must match a column name from the `SELECT *` statement. In the output column list, the column that matches 
the alias is replaced by the expression in that `REPLACE`.

This modifier does not change the names or order of columns. However, it can change the value and the value type.

**Syntax:**

```sql
SELECT <expr> REPLACE( <expr> AS col_name) from [db.]table_name
```

**Example:**

```sql
SELECT * REPLACE(i + 1 AS i) from columns_transformers;
```

```response
┌───i─┬──j─┬───k─┐
│ 101 │ 10 │ 324 │
│ 121 │  8 │  23 │
└─────┴────┴─────┘
```
