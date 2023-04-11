---
slug: /ru/sql-reference/aggregate-functions/reference/anyheavy
sidebar_position: 103
---

# anyHeavy {#anyheavyx}

Выбирает часто встречающееся значение с помощью алгоритма «[heavy hitters](https://doi.org/10.1145/762471.762473)». Если существует значение, которое встречается чаще, чем в половине случаев, в каждом потоке выполнения запроса, то возвращается данное значение. В общем случае, результат недетерминирован.

``` sql
anyHeavy(column)
```

**Аргументы**

-   `column` — имя столбца.

**Пример**

Возьмём набор данных [OnTime](../../../getting-started/example-datasets/ontime.md) и выберем произвольное часто встречающееся значение в столбце `AirlineID`.

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

``` text
┌───res─┐
│ 19690 │
└───────┘
```
