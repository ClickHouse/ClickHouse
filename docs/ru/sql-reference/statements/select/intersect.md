---
toc_title: INTERSECT
---

# Секция INTERSECT {#intersect-clause}

`INTERSECT` возвращает строки, которые есть только в результатах первого и второго запросов. В запросах должны совпадать количество столбцов, их порядок и тип. Результат `INTERSECT` может содержать повторяющиеся строки.

Если используется несколько `INTERSECT` и скобки не указаны, пересечение выполняется слева направо. У `INTERSECT` более высокий приоритет выполнения, чем у `UNION` и `EXCEPT`.


``` sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

INTERSECT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```
Условие может быть любым в зависимости от ваших требований.

**Примеры**

Запрос:

``` sql
SELECT number FROM numbers(1,10) INTERSECT SELECT number FROM numbers(3,6);
```

Результат:

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

Запрос:

``` sql
CREATE TABLE t1(one String, two String, three String) ENGINE=Memory();
CREATE TABLE t2(four String, five String, six String) ENGINE=Memory();

INSERT INTO t1 VALUES ('q', 'm', 'b'), ('s', 'd', 'f'), ('l', 'p', 'o'), ('s', 'd', 'f'), ('s', 'd', 'f'), ('k', 't', 'd'), ('l', 'p', 'o');
INSERT INTO t2 VALUES ('q', 'm', 'b'), ('b', 'd', 'k'), ('s', 'y', 't'), ('s', 'd', 'f'), ('m', 'f', 'o'), ('k', 'k', 'd');

SELECT * FROM t1 INTERSECT SELECT * FROM t2;
```

Результат:

``` text
┌─one─┬─two─┬─three─┐
│ q   │ m   │ b     │
│ s   │ d   │ f     │
│ s   │ d   │ f     │
│ s   │ d   │ f     │
└─────┴─────┴───────┘
```

**См. также**

-   [UNION](union.md#union-clause)
-   [EXCEPT](except.md#except-clause)
