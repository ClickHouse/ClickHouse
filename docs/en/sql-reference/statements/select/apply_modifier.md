---
description: 'Documentation describing the APPLY modifier which allows you to invoke some function for each row returned by an outer table expression of a query.'
sidebar_label: 'APPLY'
slug: /sql-reference/statements/select/apply-modifier
title: 'APPLY modifier'
keywords: ['APPLY', 'modifier']
doc_type: 'reference'
---

# APPLY modifier {#apply}

> Allows you to invoke some function for each row returned by an outer table expression of a query.

## Syntax {#syntax}

```sql
SELECT <expr> APPLY( <func> ) FROM [db.]table_name
```

## Example {#example}

```sql
CREATE TABLE columns_transformers (i Int64, j Int16, k Int64) ENGINE = MergeTree ORDER by (i);
INSERT INTO columns_transformers VALUES (100, 10, 324), (120, 8, 23);
SELECT * APPLY(sum) FROM columns_transformers;
```

```response
┌─sum(i)─┬─sum(j)─┬─sum(k)─┐
│    220 │     18 │    347 │
└────────┴────────┴────────┘
```
