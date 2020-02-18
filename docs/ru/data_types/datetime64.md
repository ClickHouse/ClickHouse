# DateTime64 {#data_type-datetime64}

Хранит информацию о моменте времени, включая дату, время и часовой пояс, с заданной суб-секундной точностью. 

Размер тика/точность: 10<sup>-precision</sup> секунд, где precision - целочисленный параметр типа.

Синтаксис:
```sql
DateTime64(precision, [timezone])
```

Данные хранятся в виде количества 'тиков', прошедших с момента начала эпохи (1 яндваря 1970 года), в UInt64. Размер тика определяется параметром precision.

## Пример

**1.** Парсинг

```sql
select toDateTime64('2020-01-01 11:22:33.123456', 3)
```
```text
2020-01-01 11:22:33.123
```

**2.** Создание таблицы с столбцом типа `DateTime64` и вставка данных в неё:

```sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Europe/Moscow'), 
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
┌────────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.000 │        1 │
│ 2019-01-01 00:00:00.000 │        2 │
└──────────────────────────┴──────────┘
```

Unix timestamp `1546300800000` (в миллисекундах) в часовом поясе `Europe/London (UTC+0)` представляет время `'2019-01-01 00:00:00'`, однако столбец `timestamp` хранит время в часовом поясе `Europe/Moscow (UTC+3)`, таким образом значение, вставленное в виде Unix timestamp, представляет время `2019-01-01 03:00:00`.

```sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Europe/Moscow')
```
```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

**3.** Получение часового пояса для значения типа `DateTime64`:

```sql
SELECT toDateTime64(now(), 3, 'Europe/Moscow') AS column, toTypeName(column) AS x
```
```text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Europe/Moscow') │
└─────────────────────────┴────────────────────────────────┘
```

**4.** Конвертация часовых поясов

```sql
SELECT 
toDateTime64(timestamp, 3, 'Europe/London') as lon_time, 
toDateTime64(timestamp, 3, 'Europe/Moscow') as mos_time
FROM dt
```
┌───────────────lon_time──┬────────────────mos_time─┐
│ 2019-01-01 00:00:00.000 │ 2019-01-01 03:00:00.000 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘


## See Also

- [Функции преобразования типов](../query_language/functions/type_conversion_functions.md)
- [Функции для работы с датой и временем](../query_language/functions/date_time_functions.md)
- [Функции для работы с массивами](../query_language/functions/array_functions.md)
- [Настройка `date_time_input_format`](../operations/settings/settings.md#settings-date_time_input_format)
- [Конфигурационный параметр сервера `timezone`](../operations/server_settings/settings.md#server_settings-timezone)
- [Операторы для работы с датой и временем](../query_language/operators.md#operators-datetime)
- [Тип данных `Date`](date.md)
- [Тип данных `DateTime`](datetime.md)

