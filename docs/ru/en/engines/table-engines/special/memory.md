---
description: 'Движок Memory хранит данные в оперативной памяти, в несжатом виде. Данные
  хранятся ровно в том виде, в котором поступили. Иными словами, чтение
  из этой таблицы практически ничего не стоит.'
sidebar_label: 'Memory'
sidebar_position: 110
slug: /engines/table-engines/special/memory
title: 'Движок таблицы Memory'
doc_type: 'справка'
---

:::note
При использовании движка таблицы Memory в ClickHouse Cloud данные не реплицируются между всеми узлами (это сделано намеренно). Чтобы гарантировать, что все запросы направляются на один и тот же узел и что движок таблицы Memory работает как ожидается, можно сделать одно из следующего:

* Выполнять все операции в рамках одного сеанса
* Использовать клиент, работающий через TCP или нативный интерфейс (что обеспечивает поддержку sticky-соединений), например [clickhouse-client](/ru/interfaces/client)
  :::

Движок Memory хранит данные в оперативной памяти, в несжатом виде. Данные хранятся ровно в том виде, в котором поступили. Иными словами, чтение из этой таблицы практически ничего не стоит.
Одновременный доступ к данным синхронизируется. Блокировки удерживаются недолго: операции чтения и записи не блокируют друг друга.
Индексы не поддерживаются. Чтение распараллеливается.

Максимальная производительность (более 10 ГБ/с) достигается на простых запросах, поскольку отсутствуют чтение с диска, распаковка и десериализация данных. (Следует отметить, что во многих случаях производительность движка MergeTree почти такая же высокая.)
При перезапуске сервера данные исчезают из таблицы, и таблица становится пустой.
Как правило, использовать этот движок таблицы нецелесообразно. Однако его можно использовать для тестов и задач, где требуется максимальная скорость при относительно небольшом числе строк (примерно до 100 000 000).

Движок Memory используется системой для временных таблиц с внешними данными запроса (см. раздел &quot;Внешние данные для обработки запроса&quot;), а также для реализации `GLOBAL IN` (см. раздел &quot;Операторы IN&quot;).

Чтобы ограничить размер таблицы движка Memory, можно задать верхний и нижний пределы, что фактически позволяет использовать её как кольцевой буфер (см. [Параметры движка](#engine-parameters)).

<div id="engine-parameters">
  ## Параметры движка
</div>

* `min_bytes_to_keep` — Минимальное количество байт, которое нужно сохранять, если размер таблицы Memory ограничен.
  * Значение по умолчанию: `0`
  * Требует `max_bytes_to_keep`
* `max_bytes_to_keep` — Максимальное количество байт, сохраняемых в таблице Memory, в которой при каждой вставке удаляются самые старые строки (то есть используется кольцевой буфер). Максимальное количество байт может превышать указанный предел, если при добавлении большого блока самый старый батч строк, подлежащий удалению, оказывается меньше ограничения `min_bytes_to_keep`.
  * Значение по умолчанию: `0`
* `min_rows_to_keep` — Минимальное количество строк, которое нужно сохранять, если размер таблицы Memory ограничен.
  * Значение по умолчанию: `0`
  * Требует `max_rows_to_keep`
* `max_rows_to_keep` — Максимальное количество строк, сохраняемых в таблице Memory, в которой при каждой вставке удаляются самые старые строки (то есть используется кольцевой буфер). Максимальное количество строк может превышать указанный предел, если при добавлении большого блока самый старый батч строк, подлежащий удалению, оказывается меньше ограничения `min_rows_to_keep`.
  * Значение по умолчанию: `0`
* `compress` - Нужно ли сжимать данные в памяти.
  * Значение по умолчанию: `false`

<div id="usage">
  ## Использование
</div>

**Инициализация настроек**

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**Изменение настроек**

```sql
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**Примечание:** Параметры ограничения `bytes` и `rows` можно задать одновременно, однако будут использоваться меньшие значения `max` и `min`.

<div id="examples">
  ## Примеры
</div>

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 8'192 bytes

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 1'024 bytes

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 8'192 bytes

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 65'536 bytes

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```

а также для строк:

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 4000, max_rows_to_keep = 10000;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 1'600 rows

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 100 rows

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 1'000 rows

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 10'000 rows

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```