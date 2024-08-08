---
slug: /en/sql-reference/aggregate-functions/reference/entropy
sidebar_position: 131
---

# entropy

Calculates [Shannon entropy](https://en.wikipedia.org/wiki/Entropy_(information_theory)) of a column of values.

**Syntax**

``` sql
entropy(val)
```

**Arguments**

- `val` — Column of values of any type.

**Returned value**

- Shannon entropy.

Type: [Float64](../../../sql-reference/data-types/float.md).

**Example**

Query:

``` sql
CREATE TABLE entropy (`vals` UInt32,`strings` String) ENGINE = Memory;

INSERT INTO entropy VALUES (1, 'A'), (1, 'A'), (1,'A'), (1,'A'), (2,'B'), (2,'B'), (2,'C'), (2,'D');

SELECT entropy(vals), entropy(strings) FROM entropy;
```

Result:

``` text
┌─entropy(vals)─┬─entropy(strings)─┐
│             1 │             1.75 │
└───────────────┴──────────────────┘
```
