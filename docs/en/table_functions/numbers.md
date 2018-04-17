# numbers

`numbers(N)` - returns the table with one column named `number` (UInt64 type), containing integer numbers from 0 to N-1.

`numbers(N)` (like a table `system.numbers`) can be used in tests or for sequences generation.

Two following queries are equal:
```sql
SELECT * FROM numbers(10);
SELECT * FROM system.numbers LIMIT 10;
```

Samples:
```sql
-- generation of sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```
