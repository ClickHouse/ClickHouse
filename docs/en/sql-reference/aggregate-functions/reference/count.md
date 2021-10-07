---
toc_priority: 1
---

# count {#agg_function-count}

Counts the number of rows or not-NULL values.

ClickHouse supports the following syntaxes for `count`:

-   `count(expr)` or `COUNT(DISTINCT expr)`.
-   `count()` or `COUNT(*)`. The `count()` syntax is ClickHouse-specific.

**Arguments**

The function can take:

-   Zero parameters.
-   One [expression](../../../sql-reference/syntax.md#syntax-expressions).

**Returned value**

-   If the function is called without parameters it counts the number of rows.
-   If the [expression](../../../sql-reference/syntax.md#syntax-expressions) is passed, then the function counts how many times this expression returned not null. If the expression returns a [Nullable](../../../sql-reference/data-types/nullable.md)-type value, then the result of `count` stays not `Nullable`. The function returns 0 if the expression returned `NULL` for all the rows.

In both cases the type of the returned value is [UInt64](../../../sql-reference/data-types/int-uint.md).

**Details**

ClickHouse supports the `COUNT(DISTINCT ...)` syntax. The behavior of this construction depends on the [count_distinct_implementation](../../../operations/settings/settings.md#settings-count_distinct_implementation) setting. It defines which of the [uniq\*](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) functions is used to perform the operation. The default is the [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact) function.

The `SELECT count() FROM table` query is not optimized, because the number of entries in the table is not stored separately. It chooses a small column from the table and counts the number of values in it.

However `SELECT count(nullable_column) FROM table` query can be optimized by enabling the [optimize_functions_to_subcolumns](../../../operations/settings/settings.md#optimize-functions-to-subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [null](../../../sql-reference/data-types/nullable.md#finding-null) subcolumn instead of reading and processing the whole column data. The query `SELECT count(n) FROM table` transforms to `SELECT sum(NOT n.null) FROM table`.

**Examples**

Example 1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

Example 2:

``` sql
SELECT name, value FROM system.settings WHERE name = 'count_distinct_implementation'
```

``` text
┌─name──────────────────────────┬─value─────┐
│ count_distinct_implementation │ uniqExact │
└───────────────────────────────┴───────────┘
```

``` sql
SELECT count(DISTINCT num) FROM t
```

``` text
┌─uniqExact(num)─┐
│              3 │
└────────────────┘
```

This example shows that `count(DISTINCT num)` is performed by the `uniqExact` function according to the `count_distinct_implementation` setting value.
