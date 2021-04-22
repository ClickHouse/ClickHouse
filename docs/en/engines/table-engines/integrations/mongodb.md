---
toc_priority: 5
toc_title: MongoDB
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
) ENGINE = MongoDB(host:port, database, collection, user, password);
```

**Engine Parameters**

-   `host:port` — MongoDB server address.

-   `database` — Remote database name.

-   `collection` — Remote collection name.

-   `user` — MongoDB user.

-   `password` — User password.

## Usage Example {#usage-example}

Table in ClickHouse which allows to read data from MongoDB collection:

``` text
CREATE TABLE mongo_table
(
    key UInt64, 
    data String
) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'testuser', 'clickhouse');
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

[Original article](https://clickhouse.tech/docs/en/engines/table-engines/integrations/mongodb/) <!--hide-->
