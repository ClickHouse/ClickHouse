---
description: 'The Alias table engine creates a transparent proxy to another table. All operations are forwarded to the target table while the alias itself stores no data.'
sidebar_label: 'Alias'
sidebar_position: 5
slug: /engines/table-engines/special/alias
title: 'Alias table engine'
doc_type: 'reference'
---

# Alias table engine

The `Alias` engine creates a proxy to another table. All read and write operations are forwarded to the target table, while the alias itself stores no data and only maintains a reference to the target table.

## Creating a Table {#creating-a-table}

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_table)
```

Or with explicit database name:

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_db, target_table)
```

:::note
The `Alias` table does not support explicit column definitions. Columns are automatically inherited from the target table. This ensures that the alias always matches the target table's schema.
:::

## Engine Parameters {#engine-parameters}

- **`target_db (optional)`** — Name of the database containing the target table.
- **`target_table`** — Name of the target table.

## Supported Operations {#supported-operations}

The `Alias` table engine supports all major operations. 
### Operations on Target Table {#operations-on-target}

These operations are proxied to the target table:

| Operation | Support | Description |
|-----------|---------|-------------|
| `SELECT` | ✅ | Read data from target table |
| `INSERT` | ✅ | Write data to target table |
| `INSERT SELECT` | ✅ | Batch insert into target table |
| `ALTER TABLE ADD COLUMN` | ✅ | Add columns to target table |
| `ALTER TABLE MODIFY SETTING` | ✅ | Modify target table settings |
| `ALTER TABLE PARTITION` | ✅ | Partition operations (DETACH/ATTACH/DROP) on target |
| `ALTER TABLE UPDATE` | ✅ | Update rows in target table (mutation) |
| `ALTER TABLE DELETE` | ✅ | Delete rows from target table (mutation) |
| `OPTIMIZE TABLE` | ✅ | Optimize target table (merge parts) |
| `TRUNCATE TABLE` | ✅ | Truncate target table |

### Operations on Alias Itself {#operations-on-alias}

These operations only affect the alias, **not** the target table:

| Operation | Support | Description |
|-----------|---------|-------------|
| `DROP TABLE` | ✅ | Drop the alias only, target table remains unchanged |
| `RENAME TABLE` | ✅ | Rename the alias only, target table remains unchanged |

## Usage Examples {#usage-examples}

### Basic Alias Creation {#basic-alias-creation}

Create a simple alias in the same database:

```sql
-- Create source table
CREATE TABLE source_data (
    id UInt32,
    name String,
    value Float64
) ENGINE = MergeTree
ORDER BY id;

-- Insert some data
INSERT INTO source_data VALUES (1, 'one', 10.1), (2, 'two', 20.2);

-- Create alias
CREATE TABLE data_alias ENGINE = Alias('source_data');

-- Query through alias
SELECT * FROM data_alias;
```

```text
┌─id─┬─name─┬─value─┐
│  1 │ one  │  10.1 │
│  2 │ two  │  20.2 │
└────┴──────┴───────┘
```

### Cross-Database Alias {#cross-database-alias}

Create an alias pointing to a table in a different database:

```sql
-- Create databases
CREATE DATABASE db1;
CREATE DATABASE db2;

-- Create source table in db1
CREATE TABLE db1.events (
    timestamp DateTime,
    event_type String,
    user_id UInt32
) ENGINE = MergeTree
ORDER BY timestamp;

-- Create alias in db2 pointing to db1.events
CREATE TABLE db2.events_alias ENGINE = Alias('db1', 'events');

-- Or using database.table format
CREATE TABLE db2.events_alias2 ENGINE = Alias('db1.events');

-- Both aliases work identically
INSERT INTO db2.events_alias VALUES (now(), 'click', 100);
SELECT * FROM db2.events_alias2;
```

### Write Operations Through Alias {#write-operations}

All write operations are forwarded to the target table:

```sql
CREATE TABLE metrics (
    ts DateTime,
    metric_name String,
    value Float64
) ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE metrics_alias ENGINE = Alias('metrics');

-- Insert through alias
INSERT INTO metrics_alias VALUES 
    (now(), 'cpu_usage', 45.2),
    (now(), 'memory_usage', 78.5);

-- Insert with SELECT
INSERT INTO metrics_alias 
SELECT now(), 'disk_usage', number * 10 
FROM system.numbers 
LIMIT 5;

-- Verify data is in the target table
SELECT count() FROM metrics;  -- Returns 7
SELECT count() FROM metrics_alias;  -- Returns 7
```

### Schema Modification {#schema-modification}

Alter operations modify the target table schema:

```sql
CREATE TABLE users (
    id UInt32,
    name String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE users_alias ENGINE = Alias('users');

-- Add column through alias
ALTER TABLE users_alias ADD COLUMN email String DEFAULT '';

-- Column is added to target table
DESCRIBE users;
```

```text
┌─name──┬─type───┬─default_type─┬─default_expression─┐
│ id    │ UInt32 │              │                    │
│ name  │ String │              │                    │
│ email │ String │ DEFAULT      │ ''                 │
└───────┴────────┴──────────────┴────────────────────┘
```

### Data Mutations {#data-mutations}

UPDATE and DELETE operations are supported:

```sql
CREATE TABLE products (
    id UInt32,
    name String,
    price Float64,
    status String DEFAULT 'active'
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE products_alias ENGINE = Alias('products');

INSERT INTO products_alias VALUES 
    (1, 'item_one', 100.0, 'active'),
    (2, 'item_two', 200.0, 'active'),
    (3, 'item_three', 300.0, 'inactive');

-- Update through alias
ALTER TABLE products_alias UPDATE price = price * 1.1 WHERE status = 'active';

-- Delete through alias
ALTER TABLE products_alias DELETE WHERE status = 'inactive';

-- Changes are applied to target table
SELECT * FROM products ORDER BY id;
```

```text
┌─id─┬─name─────┬─price─┬─status─┐
│  1 │ item_one │ 110.0 │ active │
│  2 │ item_two │ 220.0 │ active │
└────┴──────────┴───────┴────────┘
```

### Partition Operations {#partition-operations}

For partitioned tables, partition operations are forwarded:

```sql
CREATE TABLE logs (
    date Date,
    level String,
    message String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date;

CREATE TABLE logs_alias ENGINE = Alias('logs');

INSERT INTO logs_alias VALUES 
    ('2024-01-15', 'INFO', 'message1'),
    ('2024-02-15', 'ERROR', 'message2'),
    ('2024-03-15', 'INFO', 'message3');

-- Detach partition through alias
ALTER TABLE logs_alias DETACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 2 (partition 202402 detached)

-- Attach partition back
ALTER TABLE logs_alias ATTACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 3
```

### Table Optimization {#table-optimization}

Optimize operations merge parts in the target table:

```sql
CREATE TABLE events (
    id UInt32,
    data String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE events_alias ENGINE = Alias('events');

-- Multiple inserts create multiple parts
INSERT INTO events_alias VALUES (1, 'data1');
INSERT INTO events_alias VALUES (2, 'data2');
INSERT INTO events_alias VALUES (3, 'data3');

-- Check parts count
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;

-- Optimize through alias
OPTIMIZE TABLE events_alias FINAL;

-- Parts are merged in target table
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;  -- Returns 1
```

### Alias Management {#alias-management}

Aliases can be renamed or dropped independently:

```sql
CREATE TABLE important_data (
    id UInt32,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO important_data VALUES (1, 'critical'), (2, 'important');

CREATE TABLE old_alias ENGINE = Alias('important_data');

-- Rename alias (target table unchanged)
RENAME TABLE old_alias TO new_alias;

-- Create another alias to same table
CREATE TABLE another_alias ENGINE = Alias('important_data');

-- Drop one alias (target table and other aliases unchanged)
DROP TABLE new_alias;

SELECT * FROM another_alias;  -- Still works
SELECT count() FROM important_data;  -- Data intact, returns 2
```
