---
description: 'Позволяет читать и записывать данные, доступные через сервер Apache Arrow Flight.'
sidebar_label: 'arrowFlight'
sidebar_position: 186
slug: /sql-reference/table-functions/arrowflight
title: 'arrowFlight'
doc_type: 'reference'
---

Позволяет читать и записывать данные, доступные через сервер [Apache Arrow Flight](/ru/interfaces/arrowflight).

**Синтаксис**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**Аргументы**

* `host:port` — Адрес сервера Arrow Flight. Если порт не указан, используется порт по умолчанию `8815`. [String](../../sql-reference/data-types/string.md).
* `dataset_name` — Имя набора данных или дескриптора, доступного на сервере Arrow Flight. [String](../../sql-reference/data-types/string.md).
* `username` — Имя пользователя для базовой HTTP-аутентификации. [String](../../sql-reference/data-types/string.md).
* `password` — Пароль для базовой HTTP-аутентификации. [String](../../sql-reference/data-types/string.md).

Если `username` и `password` не указаны, аутентификация не используется (это работает только в том случае, если сервер Arrow Flight разрешает доступ без аутентификации).

Функция также поддерживает [именованные коллекции](/ru/operations/named-collections) — список поддерживаемых параметров см. в разделе [движок таблицы ArrowFlight](/ru/engines/table-engines/integrations/arrowflight#named-collections).

**Возвращаемое значение**

Табличный объект, представляющий удаленный набор данных. Схема определяется автоматически на сервере Arrow Flight.

**Настройки**

* `arrow_flight_request_descriptor_type` — Управляет тем, как имя набора данных отправляется на сервер Flight. Значения: `path` (по умолчанию) или `command`. Подробности см. в разделе [движок таблицы ArrowFlight](/ru/engines/table-engines/integrations/arrowflight#settings).

**Примеры**

Чтение с удаленного сервера Arrow Flight:

```sql title="Query"
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

```text title="Response"
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Вставка данных в удалённый сервер Arrow Flight:

```sql
INSERT INTO FUNCTION arrowFlight('127.0.0.1:9005', 'sample_dataset') VALUES (4, 'qux', 99.9);
```

Использование именованной коллекции:

```sql
SELECT * FROM arrowFlight(named_collection_name);
```

**См. также**

* [Движок таблицы ArrowFlight](/ru/engines/table-engines/integrations/arrowflight)
* [Интерфейс Arrow Flight](/ru/interfaces/arrowflight)
* [Спецификация Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)