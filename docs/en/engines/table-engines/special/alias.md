---
description: 'Create an alias of a table.'
sidebar_label: 'Alias'
sidebar_position: 120
slug: /en/engines/table-engines/special/alias
title: 'Alias Table Engine'
---

# Alias Table Engine

The Alias table engine is a reference to another table.

## Usage in ClickHouse Server {#usage-in-clickhouse-server}

```sql
ENGINE = Alias(database_name.table_name)
-- or
ENGINE = Alias(database_name, table_name)
-- or
ENGINE = Alias(UUID)
```

- The `database_name` and `table_name` parameters specify the database and the name of the referenced table.
- The `UUID` parameter specifies the UUID of the referenced table.

The table schema definition is prohibited for Alias table as it should always be the same as the reference table.

## Example {#example}

**1.** Create `ref_table` table and `alias_table` table as a alias of `ref_table`:

```sql
create table ref_table (id UInt32, name String) Engine=MergeTree order by id;
create table alias_table Engine=Alias(default.ref_table);
create table alias_table_with_uuid Engine=Alias('5a39dc94-7b13-432a-b96e-b92cb12957d3');
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

## Details of Implementation {#details-of-implementation}

Operations on `Alias` storage will be directed to its reference table.
