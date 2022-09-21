---
sidebar_label: EXCEPT
---

# EXCEPT Clause

The `EXCEPT` clause returns only those rows that result from the first query without the second. The queries must match the number of columns, order, and type. The result of `EXCEPT` can contain duplicate rows.

Multiple `EXCEPT` statements are executed left to right if parenthesis are not specified. The `EXCEPT` operator has the same priority as the `UNION` clause and lower priority than the `INTERSECT` clause.

``` sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

EXCEPT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```
The condition could be any expression based on your requirements.

**Examples**

Query:

``` sql
SELECT number FROM numbers(1,10) EXCEPT SELECT number FROM numbers(3,6);
```

Result:

``` text
┌─number─┐
│      1 │
│      2 │
│      9 │
│     10 │
└────────┘
```

Query:

``` sql
CREATE TABLE t1(one String, two String, three String) ENGINE=Memory();
CREATE TABLE t2(four String, five String, six String) ENGINE=Memory();

INSERT INTO t1 VALUES ('q', 'm', 'b'), ('s', 'd', 'f'), ('l', 'p', 'o'), ('s', 'd', 'f'), ('s', 'd', 'f'), ('k', 't', 'd'), ('l', 'p', 'o');
INSERT INTO t2 VALUES ('q', 'm', 'b'), ('b', 'd', 'k'), ('s', 'y', 't'), ('s', 'd', 'f'), ('m', 'f', 'o'), ('k', 'k', 'd');

SELECT * FROM t1 EXCEPT SELECT * FROM t2;
```

Result:

``` text
┌─one─┬─two─┬─three─┐
│ l   │ p   │ o     │
│ k   │ t   │ d     │
│ l   │ p   │ o     │
└─────┴─────┴───────┘
```

**See Also**

-   [UNION](union.md#union-clause)
-   [INTERSECT](intersect.md#intersect-clause)
