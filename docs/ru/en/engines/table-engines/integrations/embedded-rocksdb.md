---
description: 'Этот движок позволяет интегрировать ClickHouse с RocksDB'
sidebar_label: 'EmbeddedRocksDB'
sidebar_position: 50
slug: /engines/table-engines/integrations/embedded-rocksdb
title: 'Движок таблицы EmbeddedRocksDB'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="embeddedrocksdb-table-engine">
  # Движок таблицы EmbeddedRocksDB
</div>

<CloudNotSupportedBadge />

Этот движок позволяет интегрировать ClickHouse с [RocksDB](http://rocksdb.org/).

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(primary_key_name)
[ SETTINGS name=value, ... ]
```

Параметры движка:

* `ttl` - время жизни значений. TTL задаётся в секундах. Если TTL равен 0, используется обычный экземпляр RocksDB (без TTL).
* `rocksdb_dir` - путь к каталогу существующей RocksDB или путь назначения для создаваемой RocksDB. Таблица открывается с указанным `rocksdb_dir`.
* `read_only` - если `read_only` имеет значение true, используется режим только для чтения. Для хранилища с TTL компакция не будет запускаться ни вручную, ни автоматически, поэтому устаревшие записи не удаляются.
* `primary_key_name` – любое имя столбца в списке столбцов.
* `primary key` должен быть указан; поддерживается только один столбец в первичном ключе. Первичный ключ будет сериализован в бинарном виде как `rocksdb key`.
* столбцы, кроме первичного ключа, будут сериализованы в бинарном виде как значение `rocksdb` в соответствующем порядке.
* запросы с фильтрацией по ключу через `equals` или `in` будут оптимизированы для поиска по нескольким ключам в `rocksdb`.

Настройки движка:

* `optimize_for_bulk_insert` – таблица оптимизирована для массовых вставок (конвейер вставки будет создавать SST-файлы и импортировать их в базу данных RocksDB вместо записи в memtables); значение по умолчанию: `1`.
* `bulk_insert_block_size` - минимальный размер SST-файлов (по числу строк), создаваемых при массовой вставке; значение по умолчанию: `1048449`.

Пример:

```sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

<div id="metrics">
  ## Метрики
</div>

Также есть таблица `system.rocksdb`, в которой представлена статистика RocksDB:

```sql
SELECT
    name,
    value
FROM system.rocksdb

┌─name──────────────────────┬─value─┐
│ no.file.opens             │     1 │
│ number.block.decompressed │     1 │
└───────────────────────────┴───────┘
```

<div id="configuration">
  ## Конфигурация
</div>

Вы также можете изменить любые [параметры RocksDB](https://github.com/facebook/rocksdb/wiki/Option-String-and-Option-Map) через `config`:

```xml
<rocksdb>
    <options>
        <max_background_jobs>8</max_background_jobs>
    </options>
    <column_family_options>
        <num_levels>2</num_levels>
    </column_family_options>
    <tables>
        <table>
            <name>TABLE</name>
            <options>
                <max_background_jobs>8</max_background_jobs>
            </options>
            <column_family_options>
                <num_levels>2</num_levels>
            </column_family_options>
        </table>
    </tables>
</rocksdb>
```

По умолчанию оптимизация для простого приблизительного подсчёта отключена, что может повлиять на производительность запросов `count()`. Чтобы включить эту
оптимизацию, задайте `optimize_trivial_approximate_count_query = 1`. Этот параметр также влияет на `system.tables` для движка EmbeddedRocksDB:
включите его, чтобы видеть приблизительные значения `total_rows` и `total_bytes`.

<div id="supported-operations">
  ## Поддерживаемые операции
</div>

<div id="inserts">
  ### Вставки
</div>

При вставке новых строк в `EmbeddedRocksDB`, если ключ уже существует, значение обновляется, иначе создаётся новый ключ.

Пример:

```sql
INSERT INTO test VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### Удаление
</div>

Строки можно удалять с помощью запроса `DELETE` или команды `TRUNCATE`.

```sql
DELETE FROM test WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE test DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE test;
```

<div id="updates">
  ### Обновления
</div>

Значения можно обновлять с помощью запроса `ALTER TABLE`. Первичный ключ обновлять нельзя.

```sql
ALTER TABLE test UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="joins">
  ### JOIN
</div>

Поддерживается специальный `прямой` JOIN с таблицами EmbeddedRocksDB.
Такой прямой JOIN позволяет не создавать хеш-таблицу в памяти и обращается
к данным напрямую из EmbeddedRocksDB.

При больших JOIN использование памяти при прямых JOIN может быть значительно ниже,
поскольку хеш-таблица не создаётся.

Чтобы включить прямые JOIN:

```sql
SET join_algorithm = 'direct, hash'
```

:::tip
Если для `join_algorithm` задано значение `direct, hash`, то по возможности будут использоваться прямые JOIN, а в остальных случаях — hash JOIN.
:::

<div id="example">
  #### Пример
</div>

<div id="create-and-populate-an-embeddedrocksdb-table">
  ##### Создайте и заполните таблицу EmbeddedRocksDB
</div>

```sql
CREATE TABLE rdb
(
    `key` UInt32,
    `value` Array(UInt32),
    `value2` String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

```sql
INSERT INTO rdb
    SELECT
        toUInt32(sipHash64(number) % 10) AS key,
        [key, key+1] AS value,
        ('val2' || toString(key)) AS value2
    FROM numbers_mt(10);
```

<div id="create-and-populate-a-table-to-join-with-table-rdb">
  ##### Создайте и заполните таблицу, которую можно использовать в JOIN с таблицей `rdb`
</div>

```sql
CREATE TABLE t2
(
    `k` UInt16
)
ENGINE = TinyLog
```

```sql
INSERT INTO t2 SELECT number AS k
FROM numbers_mt(10)
```

<div id="set-the-join-algorithm-to-direct">
  ##### Установите для JOIN алгоритм `direct`
</div>

```sql
SET join_algorithm = 'direct'
```

<div id="an-inner-join">
  ##### INNER JOIN
</div>

```sql
SELECT *
FROM
(
    SELECT k AS key
    FROM t2
) AS t2
INNER JOIN rdb ON rdb.key = t2.key
ORDER BY key ASC
```

```response
┌─key─┬─rdb.key─┬─value──┬─value2─┐
│   0 │       0 │ [0,1]  │ val20  │
│   2 │       2 │ [2,3]  │ val22  │
│   3 │       3 │ [3,4]  │ val23  │
│   6 │       6 │ [6,7]  │ val26  │
│   7 │       7 │ [7,8]  │ val27  │
│   8 │       8 │ [8,9]  │ val28  │
│   9 │       9 │ [9,10] │ val29  │
└─────┴─────────┴────────┴────────┘
```

<div id="more-information-on-joins">
  ### Подробнее о JOIN
</div>

* [настройка `join_algorithm`](/ru/operations/settings/settings.md#join_algorithm)
* [оператор JOIN](/ru/sql-reference/statements/select/join.md)