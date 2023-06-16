---
sidebar_position: 9
sidebar_label: EmbeddedRocksDB
---

# Движок EmbeddedRocksDB {#EmbeddedRocksDB-engine}

Этот движок позволяет интегрировать ClickHouse с [rocksdb](http://rocksdb.org/).

## Создание таблицы {#table_engine-EmbeddedRocksDB-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB
PRIMARY KEY(primary_key_name);
```

Обязательные параметры:

- `primary_key_name` может быть любое имя столбца из списка столбцов.
- Указание первичного ключа `primary key` является обязательным. Он будет сериализован в двоичном формате как ключ `rocksdb`.
- Поддерживается только один столбец в первичном ключе.
- Столбцы, которые отличаются от первичного ключа, будут сериализованы в двоичном формате как значение `rockdb` в соответствующем порядке.
- Запросы с фильтрацией по ключу `equals` или `in` оптимизируются для поиска по нескольким ключам из `rocksdb`.

Пример:

``` sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32,
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key;
```

