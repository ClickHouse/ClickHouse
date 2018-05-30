# numbers

`numbers(N)` – Returns a table with the single 'number' column (UInt64) that contains integers from 0 to N-1.

Similar to the `system.numbers` table, it can be used for testing and generating successive values.

The following two queries are equivalent:

```sql
SELECT * FROM numbers(10);
SELECT * FROM system.numbers LIMIT 10;
```

Examples:

```sql
-- Generate a sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```

