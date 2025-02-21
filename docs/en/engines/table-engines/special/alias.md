---
slug: /en/engines/table-engines/special/alias
sidebar_position: 140
sidebar_label:  Alias
title: "Alias Table Engine"
---

# Alias Table Engine

The Alias table engine is a reference to another table.

## Usage in ClickHouse Server {#usage-in-clickhouse-server}

```sql
ENGINE = Alias(database_name, table_name)
```

The `database_name` and `table_name` parameters specify the database and the name of the referenced table.

Don't need to specify the table schema of the alias table. If you want to specify the table schema, it should be totally the same (including field names) as the referenced table.

## Example {#example}

**1.** Create `ref_table` table and `alias_table` table as a alias of `ref_table`:

```sql
create table ref_table (id UInt32, name String) Engine=MergeTree order by id;
create table alias_table Engine=Alias(default, ref_table);
```

**2.** Insert data into `ref_table` or `alias_table`:

```sql
insert into ref_table values (1, 'one'), (2, 'two'), (3, 'three');
insert into alias_table values (4, 'four');
```

**3.** Query the data:

```sql
select * from alias_table order by id;
```

```text

```

## Details of Implementation {#details-of-implementation}

The storage is replaced with the storage of the referenced table after getting the table from the in-memory database catalog. For example: in the table-resolving phase of the planner (when `allow_experimental_analyzer=1`) or in the constructor of an interpreter (when `allow_experimental_analyzer=0`).

Currently, it only supports `SELECT` and `INSERT` queries. Other operations like `ALTER` will be supported later.