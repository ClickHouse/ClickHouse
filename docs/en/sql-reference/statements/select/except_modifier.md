---
description: 'Documentation describing the EXCEPT modifier which specifies the names of one or more columns to exclude from the result. All matching column names are omitted from the output.'
sidebar_label: 'EXCEPT'
slug: /sql-reference/statements/select/except-modifier
title: 'EXCEPT modifier'
keywords: ['EXCEPT', 'modifier']
doc_type: 'reference'
---

# EXCEPT modifier {#except}

> Specifies the names of one or more columns to exclude from the result. All matching column names are omitted from the output.

## Syntax {#syntax}

```sql
SELECT <expr> EXCEPT ( col_name1 [, col_name2, col_name3, ...] ) FROM [db.]table_name
```

## Examples {#examples}

```sql title="Query"
SELECT * EXCEPT (i) from columns_transformers;
```

```response title="Response"
┌──j─┬───k─┐
│ 10 │ 324 │
│  8 │  23 │
└────┴─────┘
```
