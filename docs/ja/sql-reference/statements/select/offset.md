---
toc_title: OFFSET
---

# OFFSET FETCH Clause {#offset-fetch}

`OFFSET` and `FETCH` allow you to retrieve data by portions. They specify a row block which you want to get by a single query.

``` sql
OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]
```

The `offset_row_count` or `fetch_row_count` value can be a number or a literal constant. You can omit `fetch_row_count`; by default, it equals to 1.

`OFFSET` specifies the number of rows to skip before starting to return rows from the query result set.

The `FETCH` specifies the maximum number of rows that can be in the result of a query.

The `ONLY` option is used to return rows that immediately follow the rows omitted by the `OFFSET`. In this case the `FETCH` is an alternative to the [LIMIT](../../../sql-reference/statements/select/limit.md) clause. For example, the following query

``` sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

is identical to the query

``` sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

The `WITH TIES` option is used to return any additional rows that tie for the last place in the result set according to the `ORDER BY` clause. For example, if `fetch_row_count` is set to 5 but two additional rows match the values of the `ORDER BY` columns in the fifth row, the result set will contain seven rows.

!!! note "Note"
    According to the standard, the `OFFSET` clause must come before the `FETCH` clause if both are present.

!!! note "Note"
    The real offset can also depend on the [offset](../../../operations/settings/settings.md#offset) setting.

## Examples {#examples}

Input table:

``` text
┌─a─┬─b─┐
│ 1 │ 1 │
│ 2 │ 1 │
│ 3 │ 4 │
│ 1 │ 3 │
│ 5 │ 4 │
│ 0 │ 6 │
│ 5 │ 7 │
└───┴───┘
```

Usage of the `ONLY` option:

``` sql
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

Result:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

Usage of the `WITH TIES` option:

``` sql
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

Result:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```
