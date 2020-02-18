# DateTime64 {#data_type-datetime64}

Позволяет хранить момент времени, аналогично DateTime, но с двумя ключевыми отличиями:
* Данные хранятся в виде "тиков" (долей секунды)
* Данные хранятся в `Int64` вместо `Int32` в DateTime

Синтаксис:

```sql
DateTime64(precision, [timezone])
```

Разрядность "тика" (точность) указывается как параметр типа

Запрос:
```sql
select toDateTime64('2020-01-01 11:22:33.123456', 3)
```
Результат:
`2020-01-01 11:22:33.123` - 3 означает миллисекундную точность


## Пример

**1.** Создание таблицы с столбцом типа `DateTime64` и вставка данных в неё:

```sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3,'Europe/Moscow'), 
    `event_id` UInt8
)
ENGINE = TinyLog
```
```sql
INSERT INTO dt Values (1546300800000, 1), ('2019-01-01 00:00:00', 2)
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

Unix timestamp `1546300800000` в часовом поясе `Europe/London (UTC+0)` представляет время `'2019-01-01 00:00:00'`, однако столбец `timestamp` хранит время в часовом поясе `Europe/Moscow (UTC+3)`, таким образом значение, вставленное в виде Unix timestamp, представляет время `2019-01-01 03:00:00`.


## See Also

- [Функции преобразования типов](../query_language/functions/type_conversion_functions.md)
- [Функции для работы с датой и временем](../query_language/functions/date_time_functions.md)
- [Функции для работы с массивами](../query_language/functions/array_functions.md)
- [Настройка `date_time_input_format`](../operations/settings/settings.md#settings-date_time_input_format)
- [Конфигурационный параметр сервера `timezone`](../operations/server_settings/settings.md#server_settings-timezone)
- [Операторы для работы с датой и временем](../query_language/operators.md#operators-datetime)
- [Тип данных `Date`](date.md)
- [Тип данных `DateTime`](datetime.md)

