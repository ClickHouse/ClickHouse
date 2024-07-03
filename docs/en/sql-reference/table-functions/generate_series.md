---
slug: /en/sql-reference/table-functions/generate_series
sidebar_position: 146
sidebar_label: generate_series
---

# generate_series

`generate_series(START, STOP)` - Returns a table with the single ‘generate_series’ column (UInt64) that contains integers from start to stop inclusively.

`generate_series(START, STOP, STEP)` - Returns a table with the single ‘generate_series’ column (UInt64) that contains integers from start to stop inclusively with spacing between values given by STEP. 

The following queries return tables with the same content but different column names:

``` sql
SELECT * FROM numbers(10, 5);
SELECT * FROM generate_series(10, 14);
```

And the following queries return tables with the same content but different column names (but the second option is more efficient):

``` sql
SELECT * FROM numbers(10, 11) WHERE number % 3 == (10 % 3);
SELECT * FROM generate_series(10, 20, 3) ;
```