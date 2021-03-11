---
toc_priority: 31
toc_title: Atomic
---

# Atomic {#atomic}

-   What the Database/Table engine does.
-   Relations with other engines if they exist.

All tables in DatabaseAtomic have persistent UUID and store data in `/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/` where `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` is UUID of the table.
RENAMEs are performed without changing UUID and moving table data.
Tables in Atomic databases can be accessed by UUID through DatabaseCatalog.
On DROP TABLE no data is removed, DatabaseAtomic just marks table as dropped by moving metadata to /clickhouse_path/metadata_dropped/ and notifies DatabaseCatalog.
Running queries still may use dropped table. Table will be actually removed when it's not in use.
Allows to execute RENAME and DROP without IStorage-level RWLocks

## Creating a Database {#creating-a-database}
``` sql
    CREATE DATABASE ...
```

**Engine Parameters**

## Data Types Support {#data_types-support} 

|  EngineName           | ClickHouse                         |
|-----------------------|------------------------------------|
| NativeDataTypeName    | [ClickHouseDataTypeName](link#)    |


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


[Original article](https://clickhouse.tech/docs/en/database-engines/atomic/) <!--hide-->
