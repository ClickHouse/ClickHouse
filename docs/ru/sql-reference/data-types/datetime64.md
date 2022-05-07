---
sidebar_position: 49
sidebar_label: DateTime64
---

# DateTime64 {#data_type-datetime64}

Позволяет хранить момент времени, который может быть представлен как календарная дата и время, с заданной суб-секундной точностью.

Размер тика (точность, precision): 10<sup>-precision</sup> секунд, где precision - целочисленный параметр. Возможные значения: [ 0 : 9 ].
Обычно используются - 3 (миллисекунды), 6 (микросекунды), 9 (наносекунды).

**Синтаксис:**

``` sql
DateTime64(precision, [timezone])
```

Данные хранятся в виде количества ‘тиков’, прошедших с момента начала эпохи (1970-01-01 00:00:00 UTC), в Int64. Размер тика определяется параметром precision. Дополнительно, тип `DateTime64` позволяет хранить часовой пояс, единый для всей колонки, который влияет на то, как будут отображаться значения типа `DateTime64` в текстовом виде и как будут парситься значения заданные в виде строк (‘2020-01-01 05:00:01.000’). Часовой пояс не хранится в строках таблицы (выборки), а хранится в метаданных колонки. Подробнее см. [DateTime](datetime.md).

Диапазон значений: \[1925-01-01 00:00:00, 2283-11-11 23:59:59.99999999\] (Примечание: Точность максимального значения составляет 8).

## Примеры {#examples}

1. Создание таблицы со столбцом типа `DateTime64` и вставка данных в неё:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO dt Values (1546300800000, 1), ('2019-01-01 00:00:00', 2);
```

``` sql
SELECT * FROM dt;
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.000 │        1 │
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

-   При вставке даты-времени как числа (аналогично ‘Unix timestamp’), время трактуется как UTC. Unix timestamp `1546300800` в часовом поясе `Europe/London (UTC+0)` представляет время `'2019-01-01 00:00:00'`. Однако, столбец `timestamp` имеет тип `DateTime('Europe/Moscow (UTC+3)')`, так что при выводе в виде строки время отобразится как `2019-01-01 03:00:00`.
-   При вставке даты-времени в виде строки, время трактуется соответственно часовому поясу установленному для колонки. `'2019-01-01 00:00:00'` трактуется как время по Москве (и в базу сохраняется `'2018-12-31 21:00:00'` в виде Unix Timestamp).

2. Фильтрация по значениям даты и времени

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Europe/Moscow');
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

В отличие от типа `DateTime`, `DateTime64` не конвертируется из строк автоматически.

3. Получение часового пояса для значения типа `DateTime64`:

``` sql
SELECT toDateTime64(now(), 3, 'Europe/Moscow') AS column, toTypeName(column) AS x;
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Europe/Moscow') │
└─────────────────────────┴────────────────────────────────┘
```

4. Конвертация часовых поясов

``` sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') as lon_time,
toDateTime64(timestamp, 3, 'Europe/Moscow') as mos_time
FROM dt;
```

``` text
┌───────────────lon_time──┬────────────────mos_time─┐
│ 2019-01-01 00:00:00.000 │ 2019-01-01 03:00:00.000 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

**See Also**

-   [Функции преобразования типов](../../sql-reference/functions/type-conversion-functions.md)
-   [Функции для работы с датой и временем](../../sql-reference/functions/date-time-functions.md)
-   [Функции для работы с массивами](../../sql-reference/functions/array-functions.md)
-   [Настройка `date_time_input_format`](../../operations/settings/settings.md#settings-date_time_input_format)
-   [Настройка `date_time_output_format`](../../operations/settings/settings.md)
-   [Конфигурационный параметр сервера `timezone`](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [Операторы для работы с датой и временем](../../sql-reference/operators/index.md#operators-datetime)
-   [Тип данных `Date`](date.md)
-   [Тип данных `DateTime`](datetime.md)
