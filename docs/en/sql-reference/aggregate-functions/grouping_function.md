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
