---
---

# GROUPING

## GROUPING

[ROLLUP](../statements/select/group-by.md/#rollup-modifier) and [CUBE](../statements/select/group-by.md/#cube-modifier) are modifiers to GROUP BY. Both of these calculate subtotals. ROLLUP takes an ordered list of columns, for example `(day, month, year)`, and calculates subtotals at each level of the aggregation and then a grand total. CUBE calculates subtotals across all possible combinations of the columns specified.  GROUPING identifies which rows returned by ROLLUP or CUBE are superaggregates, and which are rows that would be returned by an unmodified GROUP BY.

The GROUPING function takes a column as an argument, and returns a 1 or a 0. 
- `1` indicates that a row returned by a `ROLLUP` or `CUBE` modifier to `GROUP BY` is a subtotal
- `0` indicates that a row returned by a `ROLLUP` or `CUBE` is a row that is not a subtotal

## GROUPING SETS

By default, the CUBE modifier calculates subtotals for all possible combinations of the columns passed to CUBE.  GROUPING SETS allows you to specify the specific combinations to calculate.

```sql
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;
```

Response
```response
┌─number─┬─gr─┐
│      0 │  1 │
│      0 │  1 │
└────────┴────┘
┌─number─┬─gr─┐
│      0 │  2 │
│      1 │  2 │
│      2 │  2 │
│      3 │  2 │
│      4 │  2 │
│      5 │  2 │
│      6 │  2 │
│      7 │  2 │
│      8 │  2 │
│      9 │  2 │
└────────┴────┘

12 rows in set. Elapsed: 0.005 sec.
```

```sql
SELECT
    number,
    grouping(number % 2, number) AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;
```

Response
```response
┌─number─┬─gr─┐
│      0 │  1 │
└────────┴────┘
┌─number─┬─gr─┐
│      0 │  2 │
│      0 │  2 │
└────────┴────┘
┌─number─┬─gr─┐
│      1 │  1 │
│      2 │  1 │
│      3 │  1 │
│      4 │  1 │
│      5 │  1 │
│      6 │  1 │
│      7 │  1 │
│      8 │  1 │
│      9 │  1 │
└────────┴────┘

12 rows in set. Elapsed: 0.005 sec.
```

```sql
SELECT
    number,
    grouping(number, number % 2) = 1 AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;
```

Response
```response
┌─number─┬─gr─┐
│      0 │  0 │
└────────┴────┘
┌─number─┬─gr─┐
│      0 │  1 │
│      0 │  1 │
└────────┴────┘
┌─number─┬─gr─┐
│      1 │  0 │
│      2 │  0 │
│      3 │  0 │
│      4 │  0 │
│      5 │  0 │
│      6 │  0 │
│      7 │  0 │
│      8 │  0 │
│      9 │  0 │
└────────┴────┘

12 rows in set. Elapsed: 0.004 sec.
```

```sql
SELECT
    number
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, grouping(number, number % 2) = 1;
```

Response
```response
┌─number─┐
│      0 │
└────────┘
┌─number─┐
│      0 │
│      0 │
└────────┘
┌─number─┐
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘

12 rows in set. Elapsed: 0.002 sec.
```

```sql
SELECT
    number,
    count(),
    grouping(number, number % 2) AS gr
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number, number % 2),
        ()
    )
ORDER BY (gr, number);
```

Response
```response
┌─number─┬─count()─┬─gr─┐
│      0 │      10 │  0 │
└────────┴─────────┴────┘
┌─number─┬─count()─┬─gr─┐
│      0 │       1 │  2 │
│      1 │       1 │  2 │
│      2 │       1 │  2 │
│      3 │       1 │  2 │
│      4 │       1 │  2 │
│      5 │       1 │  2 │
│      6 │       1 │  2 │
│      7 │       1 │  2 │
│      8 │       1 │  2 │
│      9 │       1 │  2 │
└────────┴─────────┴────┘
┌─number─┬─count()─┬─gr─┐
│      0 │       1 │  3 │
│      1 │       1 │  3 │
│      2 │       1 │  3 │
│      3 │       1 │  3 │
│      4 │       1 │  3 │
│      5 │       1 │  3 │
│      6 │       1 │  3 │
│      7 │       1 │  3 │
│      8 │       1 │  3 │
│      9 │       1 │  3 │
└────────┴─────────┴────┘

21 rows in set. Elapsed: 0.015 sec.
```

```sql
SELECT
    number
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
HAVING grouping(number, number % 2) = 2
ORDER BY number
```

Response
```response
Received exception from server (version 22.7.1):
Code: 47. DB::Exception: Received from localhost:9000. DB::Exception: Unknown identifier: __grouping_set: While processing grouping(number, number % 2) = 2. (UNKNOWN_IDENTIFIER)
```

```sql
SELECT
    number
FROM numbers(10)
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
HAVING grouping(number, number % 2) = 1
ORDER BY number
```

Response
```response
Received exception from server (version 22.7.1):
Code: 47. DB::Exception: Received from localhost:9000. DB::Exception: Unknown identifier: __grouping_set: While processing grouping(number, number % 2) = 1. (UNKNOWN_IDENTIFIER)
```

```sql
SELECT
    number,
    GROUPING(number, number % 2) = 1 as gr
FROM remote('172.21.0.{2,4}', numbers(10))
GROUP BY
    GROUPING SETS (
    (number),
    (number % 2))
ORDER BY number, gr;
```

Response
```response
┌─number─┬─gr─┐
│      0 │  0 │
│      0 │  1 │
│      0 │  1 │
│      1 │  0 │
│      2 │  0 │
│      3 │  0 │
│      4 │  0 │
│      5 │  0 │
│      6 │  0 │
│      7 │  0 │
│      8 │  0 │
│      9 │  0 │
└────────┴────┘

12 rows in set. Elapsed: 0.008 sec.
```


```sql
SELECT
    number,
    grouping(number, number % 2) = 3
FROM remote('172.21.0.{2,4}', numbers(10))
GROUP BY
    number,
    number % 2
ORDER BY number;
```

Response
```response
┌─number─┬─equals(grouping(number, modulo(number, 2)), 3)─┐
│      0 │                                              1 │
│      1 │                                              1 │
│      2 │                                              1 │
│      3 │                                              1 │
│      4 │                                              1 │
│      5 │                                              1 │
│      6 │                                              1 │
│      7 │                                              1 │
│      8 │                                              1 │
│      9 │                                              1 │
└────────┴────────────────────────────────────────────────┘

10 rows in set. Elapsed: 0.009 sec.
```

```sql
SELECT
    number,
    grouping(number),
    GROUPING(number % 2)
FROM remote('172.21.0.{2,4}', numbers(10))
GROUP BY
    number,
    number % 2
ORDER BY number;
```

Response
```response
┌─number─┬─grouping(number)─┬─grouping(modulo(number, 2))─┐
│      0 │                1 │                           1 │
│      1 │                1 │                           1 │
│      2 │                1 │                           1 │
│      3 │                1 │                           1 │
│      4 │                1 │                           1 │
│      5 │                1 │                           1 │
│      6 │                1 │                           1 │
│      7 │                1 │                           1 │
│      8 │                1 │                           1 │
│      9 │                1 │                           1 │
└────────┴──────────────────┴─────────────────────────────┘

10 rows in set. Elapsed: 0.010 sec.
```

```sql
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('172.21.0.{2,4}', numbers(10))
GROUP BY
    number,
    number % 2
    WITH ROLLUP
ORDER BY
    number, gr;
```

Response
```response
┌─number─┬─gr─┐
│      0 │  0 │
│      0 │  2 │
│      0 │  3 │
│      1 │  2 │
│      1 │  3 │
│      2 │  2 │
│      2 │  3 │
│      3 │  2 │
│      3 │  3 │
│      4 │  2 │
│      4 │  3 │
│      5 │  2 │
│      5 │  3 │
│      6 │  2 │
│      6 │  3 │
│      7 │  2 │
│      7 │  3 │
│      8 │  2 │
│      8 │  3 │
│      9 │  2 │
│      9 │  3 │
└────────┴────┘

21 rows in set. Elapsed: 0.005 sec.
```

```sql
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('172.21.0.{2,4}', numbers(10))
GROUP BY
    ROLLUP(number, number % 2)
ORDER BY
    number, gr;
```

Response
```response
┌─number─┬─gr─┐
│      0 │  0 │
│      0 │  2 │
│      0 │  3 │
│      1 │  2 │
│      1 │  3 │
│      2 │  2 │
│      2 │  3 │
│      3 │  2 │
│      3 │  3 │
│      4 │  2 │
│      4 │  3 │
│      5 │  2 │
│      5 │  3 │
│      6 │  2 │
│      6 │  3 │
│      7 │  2 │
│      7 │  3 │
│      8 │  2 │
│      8 │  3 │
│      9 │  2 │
│      9 │  3 │
└────────┴────┘

21 rows in set. Elapsed: 0.008 sec.
```

```sql
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('172.21.0.{2,4}', numbers(10))
GROUP BY
    number,
    number % 2
    WITH CUBE
ORDER BY
    number, gr;
```

Response
```response
┌─number─┬─gr─┐
│      0 │  0 │
│      0 │  1 │
│      0 │  1 │
│      0 │  2 │
│      0 │  3 │
│      1 │  2 │
│      1 │  3 │
│      2 │  2 │
│      2 │  3 │
│      3 │  2 │
│      3 │  3 │
│      4 │  2 │
│      4 │  3 │
│      5 │  2 │
│      5 │  3 │
│      6 │  2 │
│      6 │  3 │
│      7 │  2 │
│      7 │  3 │
│      8 │  2 │
│      8 │  3 │
│      9 │  2 │
│      9 │  3 │
└────────┴────┘

23 rows in set. Elapsed: 0.006 sec.
```

```sql
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('172.21.0.{2,4}', numbers(10))
GROUP BY
    CUBE(number, number % 2)
ORDER BY
    number, gr;
```

Response
```response
┌─number─┬─gr─┐
│      0 │  0 │
│      0 │  1 │
│      0 │  1 │
│      0 │  2 │
│      0 │  3 │
│      1 │  2 │
│      1 │  3 │
│      2 │  2 │
│      2 │  3 │
│      3 │  2 │
│      3 │  3 │
│      4 │  2 │
│      4 │  3 │
│      5 │  2 │
│      5 │  3 │
│      6 │  2 │
│      6 │  3 │
│      7 │  2 │
│      7 │  3 │
│      8 │  2 │
│      8 │  3 │
│      9 │  2 │
│      9 │  3 │
└────────┴────┘

23 rows in set. Elapsed: 0.004 sec.
```

```sql
SELECT
    number,
    grouping(number, number % 2) + 3 as gr
FROM remote('172.21.0.{2,4}', numbers(10))
GROUP BY
    CUBE(number, number % 2)
HAVING grouping(number) != 0
ORDER BY
    number, gr;
```

Response
```response

┌─number─┬─gr─┐
│      0 │  5 │
│      0 │  6 │
│      1 │  5 │
│      1 │  6 │
│      2 │  5 │
│      2 │  6 │
│      3 │  5 │
│      3 │  6 │
│      4 │  5 │
│      4 │  6 │
│      5 │  5 │
│      5 │  6 │
│      6 │  5 │
│      6 │  6 │
│      7 │  5 │
│      7 │  6 │
│      8 │  5 │
│      8 │  6 │
│      9 │  5 │
│      9 │  6 │
└────────┴────┘

20 rows in set. Elapsed: 0.006 sec.
```

```sql
SELECT
    number,
    grouping(number, number % 2) as gr
FROM remote('172.21.0.{2,4}', numbers(10))
GROUP BY
    CUBE(number, number % 2) WITH TOTALS
ORDER BY
    number, gr;
```

Response
```response
┌─number─┬─gr─┐
│      0 │  0 │
│      0 │  1 │
│      0 │  1 │
│      0 │  2 │
│      0 │  3 │
│      1 │  2 │
│      1 │  3 │
│      2 │  2 │
│      2 │  3 │
│      3 │  2 │
│      3 │  3 │
│      4 │  2 │
│      4 │  3 │
│      5 │  2 │
│      5 │  3 │
│      6 │  2 │
│      6 │  3 │
│      7 │  2 │
│      7 │  3 │
│      8 │  2 │
│      8 │  3 │
│      9 │  2 │
│      9 │  3 │
└────────┴────┘

Totals:
┌─number─┬─gr─┐
│      0 │  0 │
└────────┴────┘

23 rows in set. Elapsed: 0.006 sec.
```

```sql
SELECT
    number,
    grouping(number, number % 2) as gr
FROM remote('172.21.0.{2,4}', numbers(10))
GROUP BY
    ROLLUP(number, number % 2) WITH TOTALS
ORDER BY
    number, gr;
```

Response
```response
┌─number─┬─gr─┐
│      0 │  0 │
│      0 │  2 │
│      0 │  3 │
│      1 │  2 │
│      1 │  3 │
│      2 │  2 │
│      2 │  3 │
│      3 │  2 │
│      3 │  3 │
│      4 │  2 │
│      4 │  3 │
│      5 │  2 │
│      5 │  3 │
│      6 │  2 │
│      6 │  3 │
│      7 │  2 │
│      7 │  3 │
│      8 │  2 │
│      8 │  3 │
│      9 │  2 │
│      9 │  3 │
└────────┴────┘

Totals:
┌─number─┬─gr─┐
│      0 │  0 │
└────────┴────┘

21 rows in set. Elapsed: 0.006 sec.
```
