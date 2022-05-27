---
sidebar_position: 5
sidebar_label: MongoDB
---

# MongoDB {#mongodb}

MongoDB engine is read-only table engine which allows to read data (`SELECT` queries) from remote MongoDB collection. Engine supports only non-nested data types. `INSERT` queries are not supported.

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

-   `host:port` — MongoDB server address.

-   `database` — Remote database name.

-   `collection` — Remote collection name.

-   `user` — MongoDB user.

-   `password` — User password.

-   `options` — MongoDB connection string options (optional parameter).

## Usage Example {#usage-example}

Create a table in ClickHouse which allows to read data from MongoDB collection:

``` sql
CREATE TABLE mongo_table
(
    key UInt64,
    data String
) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'testuser', 'clickhouse');
```

To read from an SSL secured MongoDB server:

``` sql
CREATE TABLE mongo_table_ssl
(
    key UInt64,
    data String
) ENGINE = MongoDB('mongo2:27017', 'test', 'simple_table', 'testuser', 'clickhouse', 'ssl=true');
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
) ENGINE = MongoDB('mongo2:27017', 'test', 'simple_table', 'testuser', 'clickhouse', 'connectTimeoutMS=100000');
```

[Original article](https://clickhouse.com/docs/en/engines/table-engines/integrations/mongodb/) <!--hide-->
