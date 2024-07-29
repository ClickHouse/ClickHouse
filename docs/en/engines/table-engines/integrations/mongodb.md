---
slug: /en/engines/table-engines/integrations/mongodb
sidebar_position: 135
sidebar_label: MongoDB
---

# MongoDB

MongoDB engine is read-only table engine which allows to read data from remote [MongoDB](https://www.mongodb.com/) collection.

Only MongoDB v3.6+ servers are supported.

**If you're facing troubles, please report the issue, and try to use [the legacy implementation](../../../operations/server-configuration-parameters/settings.md#use_legacy_mongodb_integration).
Keep in mind that it is deprecated, and will be removed in next releases.**

## Types mappings

| MongoDB            | ClickHouse                                                            |
|--------------------|-----------------------------------------------------------------------|
| bool, int32, int64 | *any numeric type*, String                                            |
| int32              | Int32, String                                                         |
| int64              | Int64, String                                                         |
| double             | Float64, String                                                       |
| date               | Date, Date32, DateTime, DateTime64, String                            |
| string             | String, UUID                                                          |
| document           | String(as JSON)                                                       |
| array              | Array, String(as JSON)                                                |
| oid                | String                                                                |
| binary             | String if in column, base64 encoded string if in an array or document |
| *any other*        | String                                                                |

If key not found in MongoDB document, default value or null(if the column is nullable) will be inserted.

## Supported clauses
**You can disable all these restriction, see [mongodb_fail_on_query_build_error](../../../operations/settings/settings.md#mongodb_fail_on_query_build_error).**\
*If `allow_experimental_analyzer=0`, ClickHouse will not try to build MongoDB query, sort and limit.*

#### You can use MongoDB table in CTE to perform any clauses, but be aware, that in some cases, performance will be significantly degraded.
For example, you want to query count() with GROUP BY(which is not supported by MongoDB engine):
```sql
SELECT count(), name FROM mongo_table WHERE name IN ('clickhouse', 'mongodb') GROUP BY name;
```

You can set `mongodb_fail_on_query_build_error=0`, but this will cause poor performance, because all data will be read from `mongo_table`
before filtering by `name`. \
So, there is a solution:
```sql
SELECT count(), name
FROM (SELECT name FROM mongo_table WHERE name in ('clickhouse', 'mongodb'))
GROUP BY name;
```

### WHERE
Only constant literals are allowed.

PREWHERE and HAVING are not supported.

#### Note:
It's always better to explicitly set type of literal because Mongo requires strict typed filters.\
For example you want to filter by `Date`:
```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'
```
This will not work because Mongo will not cast string to `Date`, so you need to cast it manually:
```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'::Date OR date = toDate('2024-01-01')
```
This applied for `Date`, `Date32`, `DateTime`, `Bool`, `UUID`.

### LIMIT and OFFSET
Only `LIMIT` is supported.

### ORDER BY
Simple expressions only are supported, without any modification like COLLATE, WITH, TO, etc.

### WINDOW
Not supported.

### GROUP BY
Not supported.

### Aggregation functions
Not supported.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, collection, user, password [, options]);
```

**Engine Parameters**

- `host:port` — MongoDB server address.

- `database` — Remote database name.

- `collection` — Remote collection name.

- `user` — MongoDB user.

- `password` — User password.

- `options` — MongoDB connection string options (optional parameter).

:::tip
If you are using the MongoDB Atlas cloud offering:

```
- connection url can be obtained from 'Atlas SQL' option
- use options: 'connectTimeoutMS=10000&ssl=true&authSource=admin'
```

:::

Also, you can simply pass a URI:
``` sql
ENGINE = MongoDB(uri, collection);
```

**Engine Parameters**

- `uri` — MongoDB server's connection URI

- `collection` — Remote collection name.

## Usage Example {#usage-example}

Create a table in ClickHouse which allows to read data from MongoDB collection:

``` sql
CREATE TABLE mongo_table
(
    key UInt64,
    data String
) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'testuser', 'password');
```
or
``` sql
ENGINE = MongoDB('mongodb://testuser:password@mongo1:27017/test', 'simple_table');
```

To read from an SSL secured MongoDB server:

``` sql
CREATE TABLE mongo_table_ssl
(
    key UInt64,
    data String
) ENGINE = MongoDB('mongo2:27017', 'test', 'simple_table', 'testuser', 'password', 'ssl=true');
```

Query:

``` sql
SELECT COUNT() FROM mongo_table;
```

``` text
┌─count()─┐
│       4 │
└─────────┘
```

You can also adjust connection timeout:

``` sql
CREATE TABLE mongo_table
(
    key UInt64,
    data String
) ENGINE = MongoDB('mongo2:27017', 'test', 'simple_table', 'testuser', 'password', 'connectTimeoutMS=100000');
```

## Troubleshooting
You can see the generated MongoDB query in DEBUG level logs.

Implementation details can be found in [mongocxx](https://github.com/mongodb/mongo-cxx-driver) and [mongoc](https://github.com/mongodb/mongo-c-driver) documentations.
