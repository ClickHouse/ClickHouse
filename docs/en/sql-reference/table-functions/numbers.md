---
slug: /en/sql-reference/table-functions/numbers
sidebar_position: 145
sidebar_label: numbers
---

# numbers

`numbers(N)` – Returns a table with the single ‘number’ column (UInt64) that contains integers from 0 to N-1.
`numbers(N, M)` - Returns a table with the single ‘number’ column (UInt64) that contains integers from N to (N + M - 1).
`numbers(N, M, S)` - Returns a table with the single ‘number’ column (UInt64) that contains integers from N to (N + M - 1) with step S.

Similar to the `system.numbers` table, it can be used for testing and generating successive values, `numbers(N, M)` more efficient than `system.numbers`.

The following queries are equivalent:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM system.numbers LIMIT 10;
SELECT * FROM system.numbers WHERE number BETWEEN 0 AND 9;
SELECT * FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
```

And the following queries are equivalent:

``` sql
SELECT number * 2 FROM numbers(10);
SELECT (number - 10) * 2 FROM numbers(10, 10);
SELECT * FROM numbers(0, 20, 2);
```


Examples:

``` sql
-- Generate a sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```
