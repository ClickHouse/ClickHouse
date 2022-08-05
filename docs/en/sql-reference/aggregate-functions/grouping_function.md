---
---

# GROUPING

## GROUPING

[ROLLUP](../statements/select/group-by.md/#rollup-modifier) and [CUBE](../statements/select/group-by.md/#cube-modifier) are modifiers to GROUP BY. Both of these calculate subtotals. ROLLUP takes an ordered list of columns, for example `(day, month, year)`, and calculates subtotals at each level of the aggregation and then a grand total. CUBE calculates subtotals across all possible combinations of the columns specified. GROUPING identifies which rows returned by ROLLUP or CUBE are superaggregates, and which are rows that would be returned by an unmodified GROUP BY.

The GROUPING function takes multiple columns as an argument, and returns a bitmask. 
- `1` indicates that a row returned by a `ROLLUP` or `CUBE` modifier to `GROUP BY` is a subtotal
- `0` indicates that a row returned by a `ROLLUP` or `CUBE` is a row that is not a subtotal

## GROUPING SETS

By default, the CUBE modifier calculates subtotals for all possible combinations of the columns passed to CUBE. GROUPING SETS allows you to specify the specific combinations to calculate.

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
```sql
CREATE TABLE servers ( datacenter VARCHAR(255),
                         distro VARCHAR(255) NOT NULL,
                         version VARCHAR(50) NOT NULL,
                         quantity INT
                       )
                        ORDER BY (datacenter, distro, version)
```

```sql
INSERT INTO servers(datacenter, distro, version, quantity)
VALUES ('Schenectady', 'Arch','2022.08.05',50),
       ('Westport', 'Arch','2022.08.05',40),
       ('Schenectady','Arch','2021.09.01',30),
       ('Westport', 'Arch','2021.09.01',20),
       ('Schenectady','Arch','2020.05.01',10),
       ('Westport', 'Arch','2020.05.01',5),
       ('Schenectady','RHEL','9',60),
       ('Westport','RHEL','9',70),
       ('Westport','RHEL','7',80),
       ('Schenectady','RHEL','7',80)
```

```sql
SELECT 
    *
FROM
    servers;
```
```┌─datacenter──┬─distro─┬─version────┬─quantity─┐
│ Schenectady │ Arch   │ 2020.05.01 │       10 │
│ Schenectady │ Arch   │ 2021.09.01 │       30 │
│ Schenectady │ Arch   │ 2022.08.05 │       50 │
│ Schenectady │ RHEL   │ 7          │       80 │
│ Schenectady │ RHEL   │ 9          │       60 │
│ Westport    │ Arch   │ 2020.05.01 │        5 │
│ Westport    │ Arch   │ 2021.09.01 │       20 │
│ Westport    │ Arch   │ 2022.08.05 │       40 │
│ Westport    │ RHEL   │ 7          │       80 │
│ Westport    │ RHEL   │ 9          │       70 │
└─────────────┴────────┴────────────┴──────────┘

10 rows in set. Elapsed: 0.409 sec.
```

```sql
SELECT
    datacenter,
    distro, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    datacenter,
    distro;
```
```response
┌─datacenter──┬─distro─┬─qty─┐
│ Schenectady │ RHEL   │ 140 │
│ Westport    │ Arch   │  65 │
│ Schenectady │ Arch   │  90 │
│ Westport    │ RHEL   │ 150 │
└─────────────┴────────┴─────┘

4 rows in set. Elapsed: 0.212 sec.
```

```sql
SELECT
    datacenter, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    datacenter;
```
```response
┌─datacenter──┬─qty─┐
│ Westport    │ 215 │
│ Schenectady │ 230 │
└─────────────┴─────┘

2 rows in set. Elapsed: 0.277 sec. 
```


```sql
SELECT
    distro, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    distro;
```

```response

┌─distro─┬─qty─┐
│ Arch   │ 155 │
│ RHEL   │ 290 │
└────────┴─────┘

2 rows in set. Elapsed: 0.352 sec. 
```


```sql
SELECT
    SUM(quantity) qty
FROM
    servers;
```
```response
┌─qty─┐
│ 445 │
└─────┘

1 row in set. Elapsed: 0.244 sec. 
```

```sql
SELECT
    datacenter,
    distro, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    datacenter,
    distro
UNION ALL
SELECT
    datacenter, 
    null,
    SUM (quantity) qty
FROM
    servers
GROUP BY
    datacenter
UNION ALL
SELECT
    null,
    distro, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    distro
UNION ALL
SELECT
    null,
    null,
    SUM(quantity) qty
FROM
    servers;
```
```response
┌─datacenter─┬─distro─┬─qty─┐
│ ᴺᵁᴸᴸ       │ ᴺᵁᴸᴸ   │ 445 │
└────────────┴────────┴─────┘
┌─datacenter──┬─distro─┬─qty─┐
│ Westport    │ ᴺᵁᴸᴸ   │ 215 │
│ Schenectady │ ᴺᵁᴸᴸ   │ 230 │
└─────────────┴────────┴─────┘
┌─datacenter──┬─distro─┬─qty─┐
│ Schenectady │ RHEL   │ 140 │
│ Westport    │ Arch   │  65 │
│ Schenectady │ Arch   │  90 │
│ Westport    │ RHEL   │ 150 │
└─────────────┴────────┴─────┘
┌─datacenter─┬─distro─┬─qty─┐
│ ᴺᵁᴸᴸ       │ Arch   │ 155 │
│ ᴺᵁᴸᴸ       │ RHEL   │ 290 │
└────────────┴────────┴─────┘

9 rows in set. Elapsed: 0.527 sec. 
```

```sql
SELECT
    datacenter,
    distro, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    GROUPING SETS(
        (datacenter,distro),
        (datacenter),
        (distro),
        ()
    )
```
```response
┌─datacenter──┬─distro─┬─qty─┐
│ Schenectady │ RHEL   │ 140 │
│ Westport    │ Arch   │  65 │
│ Schenectady │ Arch   │  90 │
│ Westport    │ RHEL   │ 150 │
└─────────────┴────────┴─────┘
┌─datacenter──┬─distro─┬─qty─┐
│ Westport    │        │ 215 │
│ Schenectady │        │ 230 │
└─────────────┴────────┴─────┘
┌─datacenter─┬─distro─┬─qty─┐
│            │        │ 445 │
└────────────┴────────┴─────┘
┌─datacenter─┬─distro─┬─qty─┐
│            │ Arch   │ 155 │
│            │ RHEL   │ 290 │
└────────────┴────────┴─────┘

9 rows in set. Elapsed: 0.427 sec.
```

```sql
SELECT
    datacenter, 
    SUM (quantity) qty
FROM
    servers
GROUP BY
    datacenter;
```
```response
┌─datacenter──┬─qty─┐
│ Westport    │ 215 │
│ Schenectady │ 230 │
└─────────────┴─────┘

2 rows in set. Elapsed: 0.318 sec.
```

```sql
SELECT 
    datacenter, SUM(quantity)
FROM
    servers
GROUP BY datacenter;
```
```response
┌─datacenter──┬─sum(quantity)─┐
│ Westport    │           215 │
│ Schenectady │           230 │
└─────────────┴───────────────┘

2 rows in set. Elapsed: 0.299 sec. 
```

```sql
SELECT 
    datacenter, SUM(quantity)
FROM
    servers
GROUP BY ROLLUP (datacenter);
```
```response
┌─datacenter──┬─sum(quantity)─┐
│ Westport    │           215 │
│ Schenectady │           230 │
└─────────────┴───────────────┘
┌─datacenter─┬─sum(quantity)─┐
│            │           445 │
└────────────┴───────────────┘

3 rows in set. Elapsed: 0.253 sec. 
```

```sql
SELECT 
    datacenter, distro, SUM(quantity)
FROM
    servers
GROUP BY datacenter, distro;
```
```response
┌─datacenter──┬─distro─┬─sum(quantity)─┐
│ Schenectady │ RHEL   │           140 │
│ Westport    │ Arch   │            65 │
│ Schenectady │ Arch   │            90 │
│ Westport    │ RHEL   │           150 │
└─────────────┴────────┴───────────────┘

4 rows in set. Elapsed: 0.328 sec.
```

```sql
SELECT 
    datacenter, distro, SUM(quantity)
FROM
    servers
GROUP BY ROLLUP (datacenter , distro);
```
```response
┌─datacenter──┬─distro─┬─sum(quantity)─┐
│ Schenectady │ RHEL   │           140 │
│ Westport    │ Arch   │            65 │
│ Schenectady │ Arch   │            90 │
│ Westport    │ RHEL   │           150 │
└─────────────┴────────┴───────────────┘
┌─datacenter──┬─distro─┬─sum(quantity)─┐
│ Schenectady │        │           230 │
│ Westport    │        │           215 │
└─────────────┴────────┴───────────────┘
┌─datacenter─┬─distro─┬─sum(quantity)─┐
│            │        │           445 │
└────────────┴────────┴───────────────┘

7 rows in set. Elapsed: 0.408 sec.
```

```sql
SELECT 
    datacenter, distro, version, SUM(quantity)
FROM
    servers
GROUP BY ROLLUP (datacenter , distro, version);
```

```response
┌─datacenter──┬─distro─┬─version────┬─sum(quantity)─┐
│ Westport    │ RHEL   │ 9          │            70 │
│ Schenectady │ Arch   │ 2022.08.05 │            50 │
│ Schenectady │ Arch   │ 2021.09.01 │            30 │
│ Schenectady │ RHEL   │ 7          │            80 │
│ Westport    │ Arch   │ 2020.05.01 │             5 │
│ Westport    │ RHEL   │ 7          │            80 │
│ Westport    │ Arch   │ 2021.09.01 │            20 │
│ Westport    │ Arch   │ 2022.08.05 │            40 │
│ Schenectady │ RHEL   │ 9          │            60 │
│ Schenectady │ Arch   │ 2020.05.01 │            10 │
└─────────────┴────────┴────────────┴───────────────┘
┌─datacenter──┬─distro─┬─version─┬─sum(quantity)─┐
│ Schenectady │ Arch   │         │            90 │
│ Westport    │ RHEL   │         │           150 │
│ Westport    │ Arch   │         │            65 │
│ Schenectady │ RHEL   │         │           140 │
└─────────────┴────────┴─────────┴───────────────┘
┌─datacenter──┬─distro─┬─version─┬─sum(quantity)─┐
│ Schenectady │        │         │           230 │
│ Westport    │        │         │           215 │
└─────────────┴────────┴─────────┴───────────────┘
┌─datacenter─┬─distro─┬─version─┬─sum(quantity)─┐
│            │        │         │           445 │
└────────────┴────────┴─────────┴───────────────┘

17 rows in set. Elapsed: 0.355 sec. 
```

```sql
SELECT
   datacenter,
   SUM(quantity)
FROM
   servers
GROUP BY
   datacenter;
```
```response
┌─datacenter──┬─sum(quantity)─┐
│ Westport    │           215 │
│ Schenectady │           230 │
└─────────────┴───────────────┘

2 rows in set. Elapsed: 0.409 sec. 
```

```sql
SELECT
   datacenter,
   SUM(quantity)
FROM
   servers
GROUP BY
   CUBE(datacenter)
ORDER BY
   datacenter;  
```
```response
┌─datacenter──┬─sum(quantity)─┐
│             │           445 │
│ Schenectady │           230 │
│ Westport    │           215 │
└─────────────┴───────────────┘

3 rows in set. Elapsed: 0.223 sec. 
```

```sql
SELECT
   datacenter,
   distro,
   version,
   SUM(quantity)
FROM
   servers
GROUP BY
   datacenter,distro,version
ORDER BY
   datacenter,
   distro;
```
```response
┌─datacenter──┬─distro─┬─version────┬─sum(quantity)─┐
│ Schenectady │ Arch   │ 2022.08.05 │            50 │
│ Schenectady │ Arch   │ 2021.09.01 │            30 │
│ Schenectady │ Arch   │ 2020.05.01 │            10 │
│ Schenectady │ RHEL   │ 7          │            80 │
│ Schenectady │ RHEL   │ 9          │            60 │
│ Westport    │ Arch   │ 2020.05.01 │             5 │
│ Westport    │ Arch   │ 2021.09.01 │            20 │
│ Westport    │ Arch   │ 2022.08.05 │            40 │
│ Westport    │ RHEL   │ 9          │            70 │
│ Westport    │ RHEL   │ 7          │            80 │
└─────────────┴────────┴────────────┴───────────────┘

10 rows in set. Elapsed: 0.211 sec.
```

```sql
SELECT
   datacenter,
   distro,
   version,
   SUM(quantity)
FROM
   servers
GROUP BY
   CUBE(datacenter,distro,version)
ORDER BY
   datacenter,
   distro;
```
```response
┌─datacenter──┬─distro─┬─version────┬─sum(quantity)─┐
│             │        │ 7          │           160 │
│             │        │ 2020.05.01 │            15 │
│             │        │ 2021.09.01 │            50 │
│             │        │ 2022.08.05 │            90 │
│             │        │ 9          │           130 │
│             │        │            │           445 │
│             │ Arch   │ 2021.09.01 │            50 │
│             │ Arch   │ 2022.08.05 │            90 │
│             │ Arch   │ 2020.05.01 │            15 │
│             │ Arch   │            │           155 │
│             │ RHEL   │ 9          │           130 │
│             │ RHEL   │ 7          │           160 │
│             │ RHEL   │            │           290 │
│ Schenectady │        │ 9          │            60 │
│ Schenectady │        │ 2021.09.01 │            30 │
│ Schenectady │        │ 7          │            80 │
│ Schenectady │        │ 2022.08.05 │            50 │
│ Schenectady │        │ 2020.05.01 │            10 │
│ Schenectady │        │            │           230 │
│ Schenectady │ Arch   │ 2022.08.05 │            50 │
│ Schenectady │ Arch   │ 2021.09.01 │            30 │
│ Schenectady │ Arch   │ 2020.05.01 │            10 │
│ Schenectady │ Arch   │            │            90 │
│ Schenectady │ RHEL   │ 7          │            80 │
│ Schenectady │ RHEL   │ 9          │            60 │
│ Schenectady │ RHEL   │            │           140 │
│ Westport    │        │ 9          │            70 │
│ Westport    │        │ 2020.05.01 │             5 │
│ Westport    │        │ 2022.08.05 │            40 │
│ Westport    │        │ 7          │            80 │
│ Westport    │        │ 2021.09.01 │            20 │
│ Westport    │        │            │           215 │
│ Westport    │ Arch   │ 2020.05.01 │             5 │
│ Westport    │ Arch   │ 2021.09.01 │            20 │
│ Westport    │ Arch   │ 2022.08.05 │            40 │
│ Westport    │ Arch   │            │            65 │
│ Westport    │ RHEL   │ 9          │            70 │
│ Westport    │ RHEL   │ 7          │            80 │
│ Westport    │ RHEL   │            │           150 │
└─────────────┴────────┴────────────┴───────────────┘

39 rows in set. Elapsed: 0.355 sec. 
```
:::note
Version in the above example may not make sense when it is not associated with a distro, if we were tracking the kernel version it might make sense because the kernel version can be associated with either distro.  Using GROUPING SETS, as in the next example, may be a better choice.
:::

```sql
SELECT
    datacenter,
    distro,
    version,
    SUM(quantity)
FROM servers
GROUP BY
    GROUPING SETS (
        (datacenter, distro, version),
        (datacenter, distro))
```
```response
┌─datacenter──┬─distro─┬─version────┬─sum(quantity)─┐
│ Westport    │ RHEL   │ 9          │            70 │
│ Schenectady │ Arch   │ 2022.08.05 │            50 │
│ Schenectady │ Arch   │ 2021.09.01 │            30 │
│ Schenectady │ RHEL   │ 7          │            80 │
│ Westport    │ Arch   │ 2020.05.01 │             5 │
│ Westport    │ RHEL   │ 7          │            80 │
│ Westport    │ Arch   │ 2021.09.01 │            20 │
│ Westport    │ Arch   │ 2022.08.05 │            40 │
│ Schenectady │ RHEL   │ 9          │            60 │
│ Schenectady │ Arch   │ 2020.05.01 │            10 │
└─────────────┴────────┴────────────┴───────────────┘
┌─datacenter──┬─distro─┬─version─┬─sum(quantity)─┐
│ Schenectady │ RHEL   │         │           140 │
│ Westport    │ Arch   │         │            65 │
│ Schenectady │ Arch   │         │            90 │
│ Westport    │ RHEL   │         │           150 │
└─────────────┴────────┴─────────┴───────────────┘

14 rows in set. Elapsed: 1.036 sec. 
```

```sql
SELECT
  datacenter,
  distro,
   SUM(quantity) 
FROM
   servers
GROUP BY
   CUBE(datacenter,distro)
ORDER BY
   datacenter,
   distro; 
```
```response
┌─datacenter──┬─distro─┬─sum(quantity)─┐
│             │        │           445 │
│             │ Arch   │           155 │
│             │ RHEL   │           290 │
│ Schenectady │        │           230 │
│ Schenectady │ Arch   │            90 │
│ Schenectady │ RHEL   │           140 │
│ Westport    │        │           215 │
│ Westport    │ Arch   │            65 │
│ Westport    │ RHEL   │           150 │
└─────────────┴────────┴───────────────┘

9 rows in set. Elapsed: 0.206 sec. 
```




