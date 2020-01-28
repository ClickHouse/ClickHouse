# DateTime {#data_type-datetime}

Позволяет хранить момент времени, который может быть представлен как календарная дата и время. `DateTime` позволяет учесть часовой пояс для хранимых значений.

Синтаксис:

```sql
DateTime([timezone])
```

Диапазон значений: [1970-01-01 00:00:00, 2105-12-31 23:59:59].

Точность: 1 секунда.

## Использование

Момент времени сохраняется как Unix timestamp, независимо от часового пояса и переходов на летнее/зимнее время. Дополнительно, `DateTime` позволяет хранить часовой пояс, который влияет на то, как буду отображаться значения типа `DateTime` в текстовом виде и как будут парситься входные строки. Список поддержанных временных зон можно найти в [IANA Time Zone Database](https://www.iana.org/time-zones).

Часовой пояс для столбца типа `DateTime` можно в явном виде установить при создании таблицы. Если часовой пояс не установлен, то ClickHouse использует значение параметра [timezone](../operations/server_settings/settings.md#server_settings-timezone), установленное в конфигурации сервера или в настройках операционной системы на момент запуска сервера.

Консольный клиент ClickHouse по умолчанию использует часовой пояс сервера, если для значения `DateTime` часовой пояс не был задан в явном виде при инициализации типа данных. Чтобы использовать часовой пояс клиента, запустите [clickhouse-client](../interfaces/cli.md) с параметром `--use_client_time_zone`.

ClickHouse по умолчанию выводит значение в формате `YYYY-MM-DD hh:mm:ss`. Формат можно поменять с помощь функции [formatDateTime](../query_language/functions/date_time_functions.md#formatdatetime).

При вставке данных в ClickHouse, можно использовать различные форматы даты и времени в зависимости от значения настройки [date_time_input_format](../operations/settings/settings.md#settings-date_time_input_format).

## Примеры

**1.** Создание таблицы с столбцом типа `DateTime` и вставка данных в неё:

CREATE TABLE dt
(
    `timestamp` DateTime('Europe/Moscow'), 
    `event_id` UInt8
)
ENGINE = TinyLog
```
```sql
INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2)
```
```sql
SELECT * FROM dt
```
```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

Unix timestamp `1546300800` в часовом поясе `Europe/London (UTC+0)` представляет время `'2019-01-01 00:00:00'`, однако столбец `timestamp` хранит время в часовом поясе `Europe/Moscow (UTC+3)`, таким образом значение, вставленное в виде Unix timestamp, представляет время `2019-01-01 03:00:00`.

```sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Europe/Moscow')
```
```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

**2.** Получение часового пояса для значения типа `DateTime`:

```sql
SELECT toDateTime(now(), 'Europe/Moscow') AS column, toTypeName(column) AS x
```
```text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Europe/Moscow') │
└─────────────────────┴───────────────────────────┘
```

## See Also

- [Функции преобразования типов](../query_language/functions/type_conversion_functions.md)
- [Функции для работы с датой и временем](../query_language/functions/date_time_functions.md)
- [Функции для работы с массивами](../query_language/functions/array_functions.md)
- [Настройка `date_time_input_format`](../operations/settings/settings.md#settings-date_time_input_format)
- [Конфигурационный параметр сервера `timezone`](../operations/server_settings/settings.md#server_settings-timezone)
- [Операторы для работы с датой и временем](../query_language/operators.md#operators-datetime)
- [Тип данных `Date`](date.md)

[Оригинальная статья](https://clickhouse.yandex/docs/ru/data_types/datetime/) <!--hide-->
