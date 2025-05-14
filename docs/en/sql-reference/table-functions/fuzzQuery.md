---
description: 'Perturbs the given query string with random variations.'
sidebar_label: 'fuzzQuery'
sidebar_position: 75
slug: /sql-reference/table-functions/fuzzQuery
title: 'fuzzQuery'
---

# fuzzQuery Table Function

Perturbs the given query string with random variations.

```sql
fuzzQuery(query[, max_query_length[, random_seed]])
```

**Arguments**

- `query` (String) - The source query to perform the fuzzing on.
- `max_query_length` (UInt64) - A maximum length the query can get during the fuzzing process.
- `random_seed` (UInt64) - A random seed for producing stable results.

**Returned Value**

A table object with a single column containing perturbed query strings.

## Usage Example {#usage-example}

```sql
SELECT * FROM fuzzQuery('SELECT materialize(\'a\' AS key) GROUP BY key') LIMIT 2;
```

```response
   ┌─query──────────────────────────────────────────────────────────┐
1. │ SELECT 'a' AS key GROUP BY key                                 │
2. │ EXPLAIN PIPELINE compact = true SELECT 'a' AS key GROUP BY key │
   └────────────────────────────────────────────────────────────────┘
```
