---
toc_priority: 170
---

# timeSeriesGroupSum {#agg-function-timeseriesgroupsum}

Синтаксис: `timeSeriesGroupSum(uid, timestamp, value)`

`timeSeriesGroupSum` агрегирует временные ряды в которых не совпадают моменты.
Функция использует линейную интерполяцию между двумя значениями времени, а затем суммирует значения для одного и того же момента (как измеренные так и интерполированные) по всем рядам.

-   `uid` уникальный идентификатор временного ряда, `UInt64`.
-   `timestamp` имеет тип `Int64` чтобы можно было учитывать милли и микросекунды.
-   `value` представляет собой значение метрики.

Функция возвращает массив кортежей с парами `(timestamp, aggregated_value)`.

Временные ряды должны быть отсортированы по возрастанию `timestamp`.

Пример:

``` text
┌─uid─┬─timestamp─┬─value─┐
│ 1   │     2     │   0.2 │
│ 1   │     7     │   0.7 │
│ 1   │    12     │   1.2 │
│ 1   │    17     │   1.7 │
│ 1   │    25     │   2.5 │
│ 2   │     3     │   0.6 │
│ 2   │     8     │   1.6 │
│ 2   │    12     │   2.4 │
│ 2   │    18     │   3.6 │
│ 2   │    24     │   4.8 │
└─────┴───────────┴───────┘
```

``` sql
CREATE TABLE time_series(
    uid       UInt64,
    timestamp Int64,
    value     Float64
) ENGINE = Memory;
INSERT INTO time_series VALUES
    (1,2,0.2),(1,7,0.7),(1,12,1.2),(1,17,1.7),(1,25,2.5),
    (2,3,0.6),(2,8,1.6),(2,12,2.4),(2,18,3.6),(2,24,4.8);

SELECT timeSeriesGroupSum(uid, timestamp, value)
FROM (
    SELECT * FROM time_series order by timestamp ASC
);
```

И результат будет:

``` text
[(2,0.2),(3,0.9),(7,2.1),(8,2.4),(12,3.6),(17,5.1),(18,5.4),(24,7.2),(25,2.5)]
```

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/timeseriesgroupsum/) <!--hide-->
