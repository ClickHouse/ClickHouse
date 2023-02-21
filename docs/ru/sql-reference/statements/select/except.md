---
sidebar_label: EXCEPT
---

# Секция EXCEPT {#except-clause}

`EXCEPT` возвращает только те строки, которые являются результатом первого запроса без результатов второго. В запросах количество, порядок следования и типы столбцов должны совпадать. Результат `EXCEPT` может содержать повторяющиеся строки.

Если используется несколько `EXCEPT`, и в выражении не указаны скобки, `EXCEPT` выполняется по порядку слева направо. `EXCEPT` имеет такой же приоритет выполнения, как `UNION`, и приоритет ниже, чем у `INTERSECT`.

``` sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

EXCEPT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```
Условие в секции `WHERE` может быть любым в зависимости от ваших требований.

**Примеры**

Запрос:

``` sql
SELECT number FROM numbers(1,10) EXCEPT SELECT number FROM numbers(3,6);
```

Результат:

``` text
┌─number─┐
│      1 │
│      2 │
│      9 │
│     10 │
└────────┘
```

Запрос:

``` sql
CREATE TABLE t1(one String, two String, three String) ENGINE=Memory();
CREATE TABLE t2(four String, five String, six String) ENGINE=Memory();

INSERT INTO t1 VALUES ('q', 'm', 'b'), ('s', 'd', 'f'), ('l', 'p', 'o'), ('s', 'd', 'f'), ('s', 'd', 'f'), ('k', 't', 'd'), ('l', 'p', 'o');
INSERT INTO t2 VALUES ('q', 'm', 'b'), ('b', 'd', 'k'), ('s', 'y', 't'), ('s', 'd', 'f'), ('m', 'f', 'o'), ('k', 'k', 'd');

SELECT * FROM t1 EXCEPT SELECT * FROM t2;
```

Результат:

``` text
┌─one─┬─two─┬─three─┐
│ l   │ p   │ o     │
│ k   │ t   │ d     │
│ l   │ p   │ o     │
└─────┴─────┴───────┘
```

**См. также**

-   [UNION](union.md#union-clause)
-   [INTERSECT](intersect.md#intersect-clause)