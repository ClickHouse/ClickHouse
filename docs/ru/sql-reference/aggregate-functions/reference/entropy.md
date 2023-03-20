---
toc_priority: 302
---

# entropy {#entropy}

Вычисляет [информационную энтропию](https://ru.wikipedia.org/wiki/%D0%98%D0%BD%D1%84%D0%BE%D1%80%D0%BC%D0%B0%D1%86%D0%B8%D0%BE%D0%BD%D0%BD%D0%B0%D1%8F_%D1%8D%D0%BD%D1%82%D1%80%D0%BE%D0%BF%D0%B8%D1%8F) столбца данных.

**Синтаксис**

``` sql
entropy(val)
```

**Аргументы**

-   `val` — столбец значений любого типа

**Возвращаемое значение**

-   Информационная энтропия.

Тип: [Float64](../../../sql-reference/data-types/float.md).

**Пример**

Запрос:

``` sql
CREATE TABLE entropy (`vals` UInt32,`strings` String) ENGINE = Memory;

INSERT INTO entropy VALUES (1, 'A'), (1, 'A'), (1,'A'), (1,'A'), (2,'B'), (2,'B'), (2,'C'), (2,'D');

SELECT entropy(vals), entropy(strings) FROM entropy;
```

Результат:

``` text
┌─entropy(vals)─┬─entropy(strings)─┐
│             1 │             1.75 │
└───────────────┴──────────────────┘
```
