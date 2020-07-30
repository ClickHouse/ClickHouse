---
toc_priority: 141
---

# sumMap {#agg_functions-summap}

Синтаксис: `sumMap(key, value)` или `sumMap(Tuple(key, value))`

Производит суммирование массива ‘value’ по соответствующим ключам заданным в массиве ‘key’.
Количество элементов в ‘key’ и ‘value’ должно быть одинаковым для каждой строки, для которой происходит суммирование.
Возвращает кортеж из двух массивов - ключи в отсортированном порядке и значения, просуммированные по соответствующим ключам.

Пример:

``` sql
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    )
) ENGINE = Log;
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests)
FROM sum_map
GROUP BY timeslot
```

``` text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │
└─────────────────────┴──────────────────────────────────────────────┘
```

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/summap/) <!--hide-->
