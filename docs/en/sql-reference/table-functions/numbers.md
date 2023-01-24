---
sidebar_position: 39
sidebar_label: numbers
---

# numbers

`numbers(N)` – Returns a table with the single ‘number’ column (UInt64) that contains integers from 0 to N-1.
`numbers(N, M)` - Returns a table with the single ‘number’ column (UInt64) that contains integers from N to (N + M - 1).

Similar to the `system.numbers` table, it can be used for testing and generating successive values, `numbers(N, M)` more efficient than `system.numbers`.

The following queries are equivalent:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM system.numbers LIMIT 10;
```

Examples:

``` sql
-- Generate a sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```

