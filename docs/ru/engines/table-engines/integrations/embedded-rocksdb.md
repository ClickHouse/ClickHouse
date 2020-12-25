---
toc_priority: 6
toc_title: EmbeddedRocksDB
---

# EmbeddedRocksDB Engine {#EmbeddedRocksDB-engine}

Этот движок позволяет интегрировать ClickHouse с [rocksdb](http://rocksdb.org/).

`EmbeddedRocksDB` дает возможность:

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

-  `primary_key_name` – любое имя столбца из списка столбцов.

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

## Описание {#description}

- должен быть указан `primary key`, он поддерживает только один столбец в первичном ключе. Первичный ключ будет сериализован в двоичном формате как ключ rocksdb.
- столбцы, отличные от первичного ключа, будут сериализованы в двоичном формате как значение rockdb в соответствующем порядке.
- запросы с фильтрацией по ключу `equals` или `in` будут оптимизированы для поиска по нескольким ключам из rocksdb.
