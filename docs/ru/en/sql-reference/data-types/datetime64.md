---
description: 'Документация по типу данных DateTime64 в ClickHouse, который хранит
  временные метки с точностью до долей секунды'
sidebar_label: 'DateTime64'
sidebar_position: 18
slug: /sql-reference/data-types/datetime64
title: 'DateTime64'
doc_type: 'reference'
---

Позволяет хранить момент времени, представленный в виде календарной даты и времени суток, с заданной точностью до долей секунды

Размер тика (precision): 10<sup>-precision</sup> секунды. Допустимый диапазон: [ 0 : 9 ].
Обычно используются значения 3 (миллисекунды), 6 (микросекунды), 9 (наносекунды).

Значение по умолчанию: 3 (миллисекунды).

**Синтаксис:**

```sql
DateTime64(precision, [timezone])
```

Внутренне хранит данные как количество &#39;тиков&#39; с начала эпохи (1970-01-01 00:00:00 UTC) в формате Int64. Разрешение тика определяется параметром precision. Кроме того, тип `DateTime64` может хранить часовой пояс, общий для всего столбца, который влияет на то, как значения типа `DateTime64` отображаются в текстовом формате и как разбираются значения, заданные в виде строк (&#39;2020-01-01 05:00:01.000&#39;). Часовой пояс не хранится в строках таблицы (или в результирующем наборе), а сохраняется в метаданных столбца. Подробности см. в [DateTime](../../sql-reference/data-types/datetime.md).

Поддерживаемый диапазон значений: [1900-01-01 00:00:00, 2299-12-31 23:59:59.999999999]

Количество цифр после десятичной точки зависит от параметра precision.

Примечание: точность максимального значения составляет 8 знаков. Если используется максимальная точность в 9 знаков (наносекунды), максимальное поддерживаемое значение в UTC — `2262-04-11 23:47:16`.

<div id="examples">
  ## Примеры
</div>

1. Создание таблицы со столбцом типа `DateTime64` и вставка данных в неё:

```sql
CREATE TABLE dt64
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = MergeTree;
```

```sql
-- Parse DateTime
-- - from an integer interpreted as the number of milliseconds (because of precision 3) since 1970-01-01,
-- - from a decimal interpreted as the number of seconds before the decimal part, and based on the precision after the decimal point,
-- - from a string.

INSERT INTO dt64
VALUES
(1546300800123, 1),
(1546300800.123, 2),
('2019-01-01 00:00:00', 3);

SELECT * FROM dt64;
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

* При вставке `datetime` в виде целого числа оно интерпретируется как Unix-временная метка (UTC) с соответствующим масштабом. `1546300800000` (с точностью 3) соответствует `'2019-01-01 00:00:00'` UTC. Однако поскольку для столбца `timestamp` указан часовой пояс `Asia/Istanbul` (UTC+3), при выводе в виде строки значение будет показано как `'2019-01-01 03:00:00'`. При вставке `datetime` в виде десятичного числа оно обрабатывается аналогично целому числу, за исключением того, что значение до десятичной точки представляет собой Unix-временную метку с точностью до секунд включительно, а значение после десятичной точки интерпретируется как точность.
* При вставке строкового значения в `datetime` оно интерпретируется как значение в часовом поясе столбца. `'2019-01-01 00:00:00'` будет интерпретировано как значение в часовом поясе `Asia/Istanbul` и сохранено как `1546290000000`.

2. Фильтрация значений `DateTime64`

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul');
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

В отличие от `DateTime`, значения `DateTime64` не преобразуются автоматически из `String`.

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64(1546300800.123, 3);
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
└─────────────────────────┴──────────┘
```

В отличие от вставки, функция `toDateTime64` обрабатывает все значения как десятичные числа, поэтому точность нужно
указывать количеством знаков после десятичной точки.

3. Получение часового пояса для значения типа `DateTime64`:

```sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x;
```

```text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2023-06-05 00:09:52.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

4. Преобразование часового пояса

```sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') AS lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') AS istanbul_time
FROM dt64;
```

```text
┌────────────────lon_time─┬───────────istanbul_time─┐
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

**См. также**

* [Функции преобразования типов](../../sql-reference/functions/type-conversion-functions.md)
* [Функции для работы с датами и временем](../../sql-reference/functions/date-time-functions.md)
* [Настройка `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [Настройка `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [Параметр конфигурации сервера `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [Настройка `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [Операторы для работы с датами и временем](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [Тип данных `Date`](../../sql-reference/data-types/date.md)
* [Тип данных `DateTime`](../../sql-reference/data-types/datetime.md)