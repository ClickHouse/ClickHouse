---
toc_priority: 7
toc_title: MongoDB
---

# MongoDB {#mongodb}

MongoDB engine is read-only, it allows to perform `SELECT` queries on data, stored on a remote MongoDB server. MongoDB engine supports onle flat fields (primitive, not nested types).

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

-   `table` — Remote table name.

-   `user` — MongoDB user.

-   `password` — User password.

## Usage Example {#usage-example}

Table in ClickHouse, retrieving data from the MongoDB table:

``` text
CREATE TABLE mongo_table
(
    key UInt64, 
    data String
) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'test', 'clickhouse');
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

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/integrations/mongodb/) <!--hide-->
