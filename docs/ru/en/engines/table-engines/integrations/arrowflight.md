---
description: 'Движок позволяет выполнять запросы к удалённым датасетам и вставлять в них данные через протокол Apache Arrow Flight.'
sidebar_label: 'ArrowFlight'
sidebar_position: 186
slug: /engines/table-engines/integrations/arrowflight
title: 'Движок таблицы ArrowFlight'
doc_type: 'reference'
---

Движок таблицы ArrowFlight позволяет ClickHouse читать данные из удалённых датасетов и записывать данные в них через протокол [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html).
Эта интеграция позволяет ClickHouse взаимодействовать с внешними серверами с поддержкой Flight, используя столбцовый формат Arrow и обеспечивая высокую производительность.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**Параметры движка**

* `host:port` — Адрес удалённого сервера Arrow Flight. Если порт не указан, используется порт по умолчанию `8815`. [String](../../../sql-reference/data-types/string.md).
* `dataset_name` — Идентификатор набора данных на сервере Flight (используется как дескриптор PATH или в запросе `SELECT *` в зависимости от настройки `arrow_flight_request_descriptor_type`). [String](../../../sql-reference/data-types/string.md).
* `username` — Имя пользователя для базовой HTTP-аутентификации. [String](../../../sql-reference/data-types/string.md).
* `password` — Пароль для базовой HTTP-аутентификации. [String](../../../sql-reference/data-types/string.md).

Если `username` и `password` не указаны, аутентификация не используется (это работает, только если сервер Arrow Flight разрешает доступ без аутентификации).

Список столбцов необязателен — если он не указан, схема определяется на удалённом сервере Arrow Flight через `GetSchema`.

<div id="named-collections">
  ## Именованные коллекции
</div>

Этот движок поддерживает [именованные коллекции](/ru/operations/named-collections) для хранения параметров подключения:

```sql
CREATE TABLE remote_flight_data
    ENGINE = ArrowFlight(named_collection_name);
```

Параметры именованной коллекции:

| Параметр                   | Обязательно                  | По умолчанию | Описание                                                |
| -------------------------- | ---------------------------- | ------------ | ------------------------------------------------------- |
| `host` or `hostname`       | Нет                          | `""`         | Имя хоста сервера.                                      |
| `port`                     | Да                           | —            | Порт сервера.                                           |
| `dataset`                  | Нет                          | `""`         | Имя набора данных или дескриптор.                       |
| `use_basic_authentication` | Нет                          | `true`       | Включить базовую аутентификацию.                        |
| `user` or `username`       | Если аутентификация включена | —            | Имя пользователя для аутентификации.                    |
| `password`                 | Нет                          | `""`         | Пароль для аутентификации.                              |
| `enable_ssl`               | Нет                          | `false`      | Включить шифрование TLS.                                |
| `ssl_ca`                   | Нет                          | `""`         | Путь к файлу CA‑сертификата для проверки TLS.           |
| `ssl_override_hostname`    | Нет                          | `""`         | Переопределяет имя хоста, проверяемое при проверке TLS. |

<div id="settings">
  ## Настройки
</div>

* `arrow_flight_request_descriptor_type` — Определяет, как имя набора данных передаётся на сервер Flight. Возможные значения: `path` (по умолчанию, передаётся как дескриптор PATH) или `command` (передаётся как дескриптор CMD с `SELECT * FROM <dataset>`). Используйте `command` для серверов Flight, которые ожидают SQL-команды (например, Dremio).

<div id="usage-example">
  ## Пример использования
</div>

Чтение данных с удалённого сервера Arrow Flight:

```sql
CREATE TABLE remote_flight_data
(
    id UInt32,
    name String,
    value Float64
) ENGINE = ArrowFlight('127.0.0.1:9005', 'sample_dataset');

SELECT * FROM remote_flight_data ORDER BY id;
```

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Вставка данных в удалённый сервер Arrow Flight:

```sql
INSERT INTO remote_flight_data VALUES (4, 'qux', 99.9);
```

<div id="notes">
  ## Примечания
</div>

* Если столбцы указаны в операторе `CREATE TABLE`, они должны соответствовать схеме, возвращаемой сервером Flight.
* Если столбцы не указаны, схема автоматически определяется на основе данных удалённого сервера.
* Поддерживаются как чтение (`SELECT`), так и запись (`INSERT`).
* Параметр `arrow_flight_request_descriptor_type` определяет, отправляется ли имя набора данных как дескриптор PATH или как дескриптор CMD, содержащий запрос `SELECT *`.

<div id="see-also">
  ## См. также
</div>

* [табличная функция arrowFlight](/ru/sql-reference/table-functions/arrowflight)
* [интерфейс Arrow Flight](/ru/interfaces/arrowflight)
* [спецификация Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
* [формат Arrow в ClickHouse](/ru/interfaces/formats/Arrow)