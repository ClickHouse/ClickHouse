---
toc_priority: 66
toc_title: Tuple
---

# Tuple Functions {#tuple-functions}

## Untuple {#untuple}

Performs syntactic substitution of [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2) elements in the call location.

**Syntax**

``` sql
untuple(Tuple x)
```

**Parameters**

-   Element of the `Tuple` type. [Tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2).
It can be a `tuple` function, column, or tuple of elements.

**Returned value**

-   None.

**Examples**

Input table:

``` text
┌─key─┬─v1─┬─v2─┬─v3─┬─v4─┬─v5─┬─v6────────┐
│   1 │ 10 │ 20 │ 40 │ 30 │ 15 │ (33,'ab') │
│   2 │ 25 │ 65 │ 70 │ 40 │  6 │ (44,'cd') │
│   3 │ 57 │ 30 │ 20 │ 10 │  5 │ (55,'ef') │
│   4 │ 55 │ 12 │  7 │ 80 │ 90 │ (66,'gh') │
│   5 │ 30 │ 50 │ 70 │ 25 │ 55 │ (77,'kl') │
└─────┴────┴────┴────┴────┴────┴───────────┘
```

Example of using a `tuple` function as a parameter `untuple` function:

Query:

``` sql
SELECT untuple(tuple(v2,v3)) FROM kv;
```

Result:

``` text
┌─v2─┬─v3─┐
│ 20 │ 40 │
│ 65 │ 70 │
│ 30 │ 20 │
│ 12 │  7 │
│ 50 │ 70 │
└────┴────┘
```

Example of using a column `Tuple` type as a parameter `untuple` function:

Query:

``` sql
SELECT untuple(v6) FROM kv;
```

Result:

``` text
┌─_ut_1─┬─_ut_2─┐
│    33 │ ab    │
│    44 │ cd    │
│    55 │ ef    │
│    66 │ gh    │
│    77 │ kl    │
└───────┴───────┘
```

You can use the `except` expression in an `untuple` function to skip columns as a result of the query. Example of using an `except` expression:

Query:

``` sql
SELECT untuple(argMax((* except (v3, v5),), v1)) FROM kv GROUP BY key ORDER BY key;
```

Result:

``` text
┌─key─┬─v1─┬─v2─┬─v4─┬─v6────────┐
│   1 │ 10 │ 20 │ 30 │ (33,'ab') │
│   2 │ 25 │ 65 │ 40 │ (44,'cd') │
│   3 │ 57 │ 30 │ 10 │ (55,'ef') │
│   4 │ 55 │ 12 │ 80 │ (66,'gh') │
│   5 │ 30 │ 50 │ 25 │ (77,'kl') │
└─────┴────┴────┴────┴───────────┘
```

**See Also**

-   [Tuple](../../sql-reference/data-types/tuple.md)

[Original article](https://clickhouse.tech/docs/en/sql-reference/functions/tuple-functions/) <!--hide-->
