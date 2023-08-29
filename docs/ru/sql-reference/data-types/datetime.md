---
sidebar_position: 48
sidebar_label: DateTime
---

# DateTime {#data_type-datetime}

Позволяет хранить момент времени, который может быть представлен как календарная дата и время.

Синтаксис:

``` sql
DateTime([timezone])
```

Диапазон значений: \[1970-01-01 00:00:00, 2106-02-07 06:28:15\].

Точность: 1 секунда.

## Использование {#ispolzovanie}

Момент времени сохраняется как [Unix timestamp](https://ru.wikipedia.org/wiki/Unix-%D0%B2%D1%80%D0%B5%D0%BC%D1%8F), независимо от часового пояса и переходов на летнее/зимнее время. Дополнительно, тип `DateTime` позволяет хранить часовой пояс, единый для всей колонки, который влияет на то, как будут отображаться значения типа `DateTime` в текстовом виде и как будут парситься значения заданные в виде строк (‘2020-01-01 05:00:01’). Часовой пояс не хранится в строках таблицы (выборки), а хранится в метаданных колонки.
Список поддерживаемых часовых поясов можно найти в [IANA Time Zone Database](https://www.iana.org/time-zones) или получить из базы данных, выполнив запрос `SELECT * FROM system.time_zones`. Также [список](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) есть в Википедии.

Часовой пояс для столбца типа `DateTime` можно в явном виде установить при создании таблицы. Если часовой пояс не установлен, то ClickHouse использует значение параметра [timezone](../../sql-reference/data-types/datetime.md#server_configuration_parameters-timezone), установленное в конфигурации сервера или в настройках операционной системы на момент запуска сервера.

Консольный клиент ClickHouse по умолчанию использует часовой пояс сервера, если для значения `DateTime` часовой пояс не был задан в явном виде при инициализации типа данных. Чтобы использовать часовой пояс клиента, запустите [clickhouse-client](../../interfaces/cli.md) с параметром `--use_client_time_zone`.

ClickHouse отображает значения в зависимости от значения параметра [date\_time\_output\_format](../../operations/settings/#settings-date_time_output_format). Текстовый формат по умолчанию `YYYY-MM-DD hh:mm:ss`. Кроме того, вы можете поменять отображение с помощью функции [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatdatetime).

При вставке данных в ClickHouse, можно использовать различные форматы даты и времени в зависимости от значения настройки [date_time_input_format](../../operations/settings/#settings-date_time_input_format).

## Примеры {#primery}

**1.** Создание таблицы с столбцом типа `DateTime` и вставка данных в неё:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime('Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2);
```

``` sql
SELECT * FROM dt;
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

-   При вставке даты-времени как целого числа, оно трактуется как Unix Timestamp (UTC). Unix timestamp `1546300800` в часовом поясе `Europe/London (UTC+0)` представляет время `'2019-01-01 00:00:00'`. Однако, столбец `timestamp` имеет тип `DateTime('Europe/Moscow (UTC+3)')`, так что при выводе в виде строки время отобразится как `2019-01-01 03:00:00`.
-   При вставке даты-времени в виде строки, время трактуется соответственно часовому поясу установленному для колонки. `'2019-01-01 00:00:00'` трактуется как время по Москве (и в базу сохраняется `1546290000`)

**2.** Фильтрация по значениям даты-времени

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Europe/Moscow')
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

Фильтровать по колонке типа `DateTime` можно, указывая строковое значение в фильтре `WHERE`. Конвертация будет выполнена автоматически:

``` sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** Получение часового пояса для колонки типа `DateTime`:

``` sql
SELECT toDateTime(now(), 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Europe/Moscow') │
└─────────────────────┴───────────────────────────┘
```

**4.** Конвертация часовых поясов

``` sql
SELECT
toDateTime(timestamp, 'Europe/London') as lon_time,
toDateTime(timestamp, 'Europe/Moscow') as mos_time
FROM dt
```

``` text
┌───────────lon_time──┬────────────mos_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

## See Also {#see-also}

-   [Функции преобразования типов](../../sql-reference/functions/type-conversion-functions.md)
-   [Функции для работы с датой и временем](../../sql-reference/functions/date-time-functions.md)
-   [Функции для работы с массивами](../../sql-reference/functions/array-functions.md)
-   [Настройка `date_time_input_format`](../../operations/settings/#settings-date_time_input_format)
-   [Настройка `date_time_output_format`](../../operations/settings/)
-   [Конфигурационный параметр сервера `timezone`](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [Операторы для работы с датой и временем](../../sql-reference/operators/index.md#operators-datetime)
-   [Тип данных `Date`](date.md)
-   [Тип данных `DateTime64`](datetime64.md)

