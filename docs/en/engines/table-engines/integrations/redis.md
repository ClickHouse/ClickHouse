---
slug: /en/engines/table-engines/integrations/redis
sidebar_position: 175
sidebar_label: Redis
---

# Redis

This engine allows integrating ClickHouse with [Redis](https://redis.io/). For Redis takes kv model, we strongly recommend you only query it in a point way, such as `where k=xx` or `where k in (xx, xx)`.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = Redis({host:port[, db_index[, password[, pool_size]]] | named_collection[, option=value [,..]] })
PRIMARY KEY(primary_key_name);
```

**Engine Parameters**

- `host:port` — Redis server address, you can ignore port and default Redis port 6379 will be used.
- `db_index` — Redis db index range from 0 to 15, default is 0.
- `password` — User password, default is blank string.
- `pool_size` — Redis max connection pool size, default is 16.
- `primary_key_name` - any column name in the column list.

:::note Serialization
`PRIMARY KEY` supports only one column. The primary key will be serialized in binary as a Redis key.
Columns other than the primary key will be serialized in binary as Redis value in corresponding order.
:::

Arguments also can be passed using [named collections](/docs/en/operations/named-collections.md). In this case `host` and `port` should be specified separately. This approach is recommended for production environment. At this moment, all parameters passed using named collections to redis are required.

:::note Filtering
Queries with `key equals` or `in filtering` will be optimized to multi keys lookup from Redis. If queries without filtering key full table scan will happen which is a heavy operation.
:::

## Usage Example {#usage-example}

Create a table in ClickHouse using `Redis` engine with plain arguments:

``` sql
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis('redis1:6379') PRIMARY KEY(key);
```

Or using [named collections](/docs/en/operations/named-collections.md):

```
<named_collections>
    <redis_creds>
        <host>localhost</host>
        <port>6379</port>
        <password>****</password>
        <pool_size>16</pool_size>
        <db_index>s0</db_index>
    </redis_creds>
</named_collections>
```

```sql
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis(redis_creds) PRIMARY KEY(key);
```

Insert:

```sql
INSERT INTO redis_table Values('1', 1, '1', 1.0), ('2', 2, '2', 2.0);
```

Query:

``` sql
SELECT COUNT(*) FROM redis_table;
```

``` text
┌─count()─┐
│       2 │
└─────────┘
```

``` sql
SELECT * FROM redis_table WHERE key='1';
```

```text
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 1   │  1 │ 1  │  1 │
└─────┴────┴────┴────┘
```

``` sql
SELECT * FROM redis_table WHERE v1=2;
```

```text
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 2   │  2 │ 2  │  2 │
└─────┴────┴────┴────┘
```

Update:

Note that the primary key cannot be updated.

```sql
ALTER TABLE redis_table UPDATE v1=2 WHERE key='1';
```

Delete:

```sql
ALTER TABLE redis_table DELETE WHERE key='1';
```

Truncate:

Flush Redis db asynchronously. Also `Truncate` support SYNC mode.

```sql
TRUNCATE TABLE redis_table SYNC;
```

Join:

Join with other tables.

```
SELECT * FROM redis_table JOIN merge_tree_table ON merge_tree_table.key=redis_table.key;
```

## Limitations {#limitations}

Redis engine also supports scanning queries, such as `where k > xx`, but it has some limitations:
1. Scanning query may produce some duplicated keys in a very rare case when it is rehashing. See details in [Redis Scan](https://github.com/redis/redis/blob/e4d183afd33e0b2e6e8d1c79a832f678a04a7886/src/dict.c#L1186-L1269).
2. During the scanning, keys could be created and deleted, so the resulting dataset can not represent a valid point in time.
