---
description: 'Документация по оконной функции nonNegativeDerivative'
sidebar_label: 'nonNegativeDerivative'
sidebar_position: 12
slug: /sql-reference/window-functions/nonNegativeDerivative
title: 'nonNegativeDerivative'
doc_type: 'reference'
---

Вычисляет неотрицательную производную `metric_column` по `timestamp_column`.
Это оконная функция, специфичная для ClickHouse и не входящая в стандарт SQL.

Для каждой строки производная вычисляется относительно *предыдущей строки в порядке вычисления окна*, который определяется предложением `ORDER BY` окна, а не `timestamp_column`.
Аргумент `timestamp_column` используется исключительно для измерения времени, прошедшего между текущей и предыдущей строками; он не влияет на порядок строк.

:::warning
`nonNegativeDerivative` не упорядочивает строки по `timestamp_column` — это делает предложение `ORDER BY` окна.
Чтобы приведённая ниже формула применялась корректно, `timestamp_column` должен строго возрастать в порядке вычисления окна, поэтому окно, как правило, следует упорядочивать по `timestamp_column` по возрастанию (например, `... OVER (ORDER BY ts ASC)` совместно с `nonNegativeDerivative(metric, ts)`).
Если время, прошедшее между текущей и предыдущей строками, неположительно — что происходит при `ORDER BY timestamp_column DESC` или при дублирующихся (одинаковых) временных метках — функция возвращает `0` для этой строки вместо применения формулы.
:::

Результатом является скорость изменения метрики за `INTERVAL`, при этом отрицательные значения обнуляются.
Это полезно для монотонно возрастающих метрик, таких как счётчики (Counter), где уменьшение значения обычно означает сброс, а не реальную отрицательную скорость изменения.

**Syntax**

```sql
nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL X UNITS])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_within_the_group]] | [window_name])
FROM table_name
WINDOW window_name AS ([PARTITION BY grouping_column] [ORDER BY sorting_column] [ROWS or RANGE expression_to_bound_rows_within_the_group])
```

Подробнее о синтаксисе оконных функций см.: [Оконные функции — Синтаксис](./index.md/#syntax).

**Arguments**

- `metric_column` — Столбец, производная которого вычисляется. [(U)Int*](../data-types/int-uint.md) или [Float*](../data-types/float.md).
- `timestamp_column` — Столбец, используемый для измерения времени, прошедшего между текущей и предыдущей строками в порядке окна. Он не задаёт порядок строк — это делает предложение `ORDER BY` окна, в котором, как правило, следует использовать этот же столбец. [DateTime](../data-types/datetime.md) или [DateTime64](../data-types/datetime64.md).
- `INTERVAL X UNITS` — Необязательный параметр. Единица времени, к которой масштабируется результат. По умолчанию — `INTERVAL 1 SECOND`. Поддерживаются только единицы фиксированной длины (`NANOSECOND`, `MICROSECOND`, `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`); единицы переменной длины (`MONTH`, `QUARTER`, `YEAR`) приводят к генерации исключения.

**Returned value**

Для каждой строки значение вычисляется следующим образом:

- `0` для первой строки;
- `0` для любой строки, у которой время, прошедшее с момента предыдущей строки, неположительно (то есть $\text{timestamp}_i - \text{timestamp}_{i-1} \le 0$, что происходит при убывающем порядке или дублирующихся временных метках);
- ${\text{metric}_i - \text{metric}_{i-1} \over \text{timestamp}_i - \text{timestamp}_{i-1}} * \text{interval}$ во всех остальных случаях.

Если вычисленное значение отрицательно, оно обнуляется. Возвращаемый тип — [Float64](../data-types/float.md).

**Example**

Следующий пример вычисляет скорость изменения показаний датчика в секунду.
Обратите внимание, что в третьей строке значение падает со `110` до `105`, поэтому её производная обнуляется.

```sql title="Query"
CREATE TABLE sensor_readings
(
    `sensor_id` UInt32,
    `ts`        DateTime,
    `reading`   Float64
)
ENGINE = Memory;

INSERT INTO sensor_readings VALUES
    (1, '2024-01-01 00:00:00', 100),
    (1, '2024-01-01 00:00:10', 110),
    (1, '2024-01-01 00:00:20', 105),
    (1, '2024-01-01 00:00:30', 130);
```

```sql title="Query"
SELECT
    ts,
    reading,
    nonNegativeDerivative(reading, ts) OVER (ORDER BY ts ASC) AS deriv_per_second
FROM sensor_readings
ORDER BY ts ASC;
```

```response title="Response"
   ┌──────────────────ts─┬─reading─┬─deriv_per_second─┐
1. │ 2024-01-01 00:00:00 │     100 │                0 │
2. │ 2024-01-01 00:00:10 │     110 │                1 │
3. │ 2024-01-01 00:00:20 │     105 │                0 │
4. │ 2024-01-01 00:00:30 │     130 │              2.5 │
   └─────────────────────┴─────────┴──────────────────┘
```