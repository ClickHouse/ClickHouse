---
sidebar_label: INTERSECT
---

# INTERSECT Clause {#intersect-clause}

The `INTERSECT` clause returns only those rows that result from both the first and the second queries. The queries must match the number of columns, order, and type. The result of `INTERSECT` can contain duplicate rows.

Multiple `INTERSECT` statements are executes left to right if parenthesis are not specified. The `INTERSECT` operator has a higher priority than the `UNION` and `EXCEPT` clause.


``` sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

INTERSECT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```
The condition could be any expression based on your requirements.

**Examples**

Query:

``` sql
SELECT number FROM numbers(1,10) INTERSECT SELECT number FROM numbers(3,6);
```

Result:

``` text
┌─number─┐
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
└────────┘
```

Query:

``` sql
CREATE TABLE t1(one String, two String, three String) ENGINE=Memory();
CREATE TABLE t2(four String, five String, six String) ENGINE=Memory();

INSERT INTO t1 VALUES ('q', 'm', 'b'), ('s', 'd', 'f'), ('l', 'p', 'o'), ('s', 'd', 'f'), ('s', 'd', 'f'), ('k', 't', 'd'), ('l', 'p', 'o');
INSERT INTO t2 VALUES ('q', 'm', 'b'), ('b', 'd', 'k'), ('s', 'y', 't'), ('s', 'd', 'f'), ('m', 'f', 'o'), ('k', 'k', 'd');

SELECT * FROM t1 INTERSECT SELECT * FROM t2;
```

Result:

``` text
┌─one─┬─two─┬─three─┐
│ q   │ m   │ b     │
│ s   │ d   │ f     │
│ s   │ d   │ f     │
│ s   │ d   │ f     │
└─────┴─────┴───────┘
```

**See Also**

-   [UNION](union.md#union-clause)
-   [EXCEPT](except.md#except-clause)
