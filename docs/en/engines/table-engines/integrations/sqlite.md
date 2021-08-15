---
toc_priority: 7
toc_title: SQLite
---

# SQLite {#sqlite}

The engine provide to import and export data to SQLite and query SQLite tables directly in ClickHouse.

## Creating a Table {#creating-a-table}

``` sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2],
        ...
    ) 
    ENGINE = SQLite('db_path', 'table')
```

**Engine Parameters**

-   `name1, name2, ...` — The column names.
-   `type1, type2, ...` — The column types.
-   `db_path` — Path to SQLite file with the database.
-   `table` — The SQLite table name.

## Specifics and recommendations {#specifics-and-recommendations}

Algorithms
Specifics of read and write processes
Examples of tasks
Recommendations for usage
Specifics of data storage

## Usage Example {#usage-example}

The example must show usage and use cases. The following text contains the recommended parts of this section.

Input table:

``` text
```

Query:

``` sql
```

Result:

``` text
```

Follow up with any text to clarify the example.

**See Also**

-   [The `sqlite` table function](../../../sql-reference/table-functions/sqlite.md)