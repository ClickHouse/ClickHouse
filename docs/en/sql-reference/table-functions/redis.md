---
description: 'This table function allows integrating ClickHouse with Redis.'
sidebar_label: 'redis'
sidebar_position: 170
slug: /sql-reference/table-functions/redis
title: 'redis'
doc_type: 'reference'
---

# redis Table Function

This table function allows integrating ClickHouse with [Redis](https://redis.io/).

## Syntax {#syntax}

```sql
redis(host:port, key, structure[, db_index[, password[, pool_size]]])
```

## Arguments {#arguments}

| Argument    | Description                                                                                                |
|-------------|------------------------------------------------------------------------------------------------------------|
| `host:port` | Redis server address, you can ignore port and default Redis port 6379 will be used.                          |
| `key`       | any column name in the column list.                                                                        |
| `structure` | The schema for the ClickHouse table returned from this function.                                             |
| `db_index`  | Redis db index range from 0 to 15, default is 0.                                                             |
| `password`  | User password, default is blank string.                                                                    |
| `pool_size` | Redis max connection pool size, default is 16.                                                               |
| `primary`   | must be specified, it supports only one column in the primary key. The primary key will be serialized in binary as a Redis key. |

- columns other than the primary key will be serialized in binary as Redis value in corresponding order.
- queries with key equals or in filtering will be optimized to multi keys lookup from Redis. If queries without filtering key full table scan will happen which is a heavy operation.

[Named collections](/operations/named-collections.md) are not supported for `redis` table function at the moment.

## Returned value {#returned_value}

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

## Related {#related}

- [The `Redis` table engine](/engines/table-engines/integrations/redis.md)
- [Using redis as a dictionary source](/sql-reference/statements/create/dictionary/sources#redis)
