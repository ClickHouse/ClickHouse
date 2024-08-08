---
slug: /en/sql-reference/table-functions/redis
sidebar_position: 170
sidebar_label: redis
---

# redis

This table function allows integrating ClickHouse with [Redis](https://redis.io/).

**Syntax**

```sql
redis(host:port, key, structure, primary[, db_index[, password[, pool_size]]])
```

**Arguments**

- `host:port` — Redis server address. If port is omitted, the default Redis port 6379 will be used.

- `key` — The name of the column to be used as the Redis key.

- `structure` — The schema for the ClickHouse table returned from this function.

- `primary` — The column to be used as the primary key. Only one column can be specified as the primary key. This column will be serialized in binary as the Redis key.

- `db_index` — Redis database index, ranging from 0 to 15, default is 0.

- `password` — User password, default is an empty string.

- `pool_size` — Redis max connection pool size, default is 16.

**Notes**

- Columns other than the primary key will be serialized in binary as Redis values in the corresponding order.
- Queries with key equality or 'in' filtering will be optimized for multi-key lookup from Redis.
- Queries without key filtering will result in a full table scan, which can be a heavy operation.

[Named collections](/docs/en/operations/named-collections.md) are not supported for `redis` table function at the moment.

**Returned Value**

A table object with key as Redis key, other columns packaged together as Redis value.

## Usage Example {#usage-example}

Read from Redis:

```sql
SELECT * FROM redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32'
)
```

Insert into Redis:

```sql
INSERT INTO TABLE FUNCTION redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32') values ('1', '1', 1);
```

**See Also**

- [The `Redis` table engine](/docs/en/engines/table-engines/integrations/redis.md)
- [Using redis as a dictionary source](/docs/en/sql-reference/dictionaries/index.md#redis)
