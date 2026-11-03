---
description: 'Documentation for Functions for Working with Dictionaries'
sidebar_label: 'Dictionaries'
slug: /sql-reference/functions/ext-dict-functions
title: 'Functions for Working with Dictionaries'
doc_type: 'reference'
---

# Functions for Working with Dictionaries

:::note
For dictionaries created with [DDL queries](../statements/create/dictionary/overview.md), the `dict_name` parameter must be fully specified, like `<database>.<dict_name>`. Otherwise, the current database is used.
:::

For information on connecting and configuring dictionaries, see [Dictionaries](../statements/create/dictionary/overview.md).

## Example dictionaries {#example-dictionary}

The examples in this section make use of the following dictionaries. You can create them in ClickHouse 
to run the examples for the functions described below.

<details>
<summary>Example dictionary for dictGet\<T\> and dictGet\<T\>OrDefault functions</summary>

```sql
-- Create table with all the required data types
CREATE TABLE all_types_test (
    `id` UInt32,
    
    -- String type
    `String_value` String,
    
    -- Unsigned integer types
    `UInt8_value` UInt8,
    `UInt16_value` UInt16,
    `UInt32_value` UInt32,
    `UInt64_value` UInt64,
    
    -- Signed integer types
    `Int8_value` Int8,
    `Int16_value` Int16,
    `Int32_value` Int32,
    `Int64_value` Int64,
    
    -- Floating point types
    `Float32_value` Float32,
    `Float64_value` Float64,
    
    -- Date/time types
    `Date_value` Date,
    `DateTime_value` DateTime,
    
    -- Network types
    `IPv4_value` IPv4,
    `IPv6_value` IPv6,
    
    -- UUID type
    `UUID_value` UUID
) ENGINE = MergeTree() 
ORDER BY id;
```
```sql
-- Insert test data
INSERT INTO all_types_test VALUES
(
    1,                              -- id
    'ClickHouse',                   -- String
    100,                            -- UInt8
    5000,                           -- UInt16
    1000000,                        -- UInt32
    9223372036854775807,            -- UInt64
    -100,                           -- Int8
    -5000,                          -- Int16
    -1000000,                       -- Int32
    -9223372036854775808,           -- Int64
    123.45,                         -- Float32
    987654.123456,                  -- Float64
    '2024-01-15',                   -- Date
    '2024-01-15 10:30:00',          -- DateTime
    '192.168.1.1',                  -- IPv4
    '2001:db8::1',                  -- IPv6
    '550e8400-e29b-41d4-a716-446655440000' -- UUID
)
```

```sql
-- Create dictionary
CREATE DICTIONARY all_types_dict
(
    id UInt32,
    String_value String,
    UInt8_value UInt8,
    UInt16_value UInt16,
    UInt32_value UInt32,
    UInt64_value UInt64,
    Int8_value Int8,
    Int16_value Int16,
    Int32_value Int32,
    Int64_value Int64,
    Float32_value Float32,
    Float64_value Float64,
    Date_value Date,
    DateTime_value DateTime,
    IPv4_value IPv4,
    IPv6_value IPv6,
    UUID_value UUID
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'all_types_test' DB 'default'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);
```
</details>

<details>
<summary>Example dictionary for dictGetAll</summary>

Create a table to store the data for the regexp tree dictionary:

```sql
CREATE TABLE regexp_os(
    id UInt64,
    parent_id UInt64,
    regexp String,
    keys Array(String),
    values Array(String)
)
ENGINE = Memory;
```

Insert data into the table:

```sql
INSERT INTO regexp_os 
SELECT *
FROM s3(
    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/' ||
    'user_agent_regex/regexp_os.csv'
);
```

Create the regexp tree dictionary:

```sql
CREATE DICTIONARY regexp_tree
(
    regexp String,
    os_replacement String DEFAULT 'Other',
    os_v1_replacement String DEFAULT '0',
    os_v2_replacement String DEFAULT '0',
    os_v3_replacement String DEFAULT '0',
    os_v4_replacement String DEFAULT '0'
)
PRIMARY KEY regexp
SOURCE(CLICKHOUSE(TABLE 'regexp_os'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(REGEXP_TREE);
```
</details>

<details>
<summary>Example range key dictionary</summary>

Create the input table:

```sql
CREATE TABLE range_key_dictionary_source_table
(
    key UInt64,
    start_date Date,
    end_date Date,
    value String,
    value_nullable Nullable(String)
)
ENGINE = TinyLog();
```

Insert the data into the input table:

```sql
INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
```

Create the dictionary:

```sql
CREATE DICTIONARY range_key_dictionary
(
    key UInt64,
    start_date Date,
    end_date Date,
    value String,
    value_nullable Nullable(String)
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_key_dictionary_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_date);
```
</details>

<details>
<summary>Example complex key dictionary</summary>

Create the source table: 

```sql
CREATE TABLE dict_mult_source
(
id UInt32,
c1 UInt32,
c2 String
) ENGINE = Memory;
```

Insert the data into the source table:

```sql
INSERT INTO dict_mult_source VALUES
(1, 1, '1'),
(2, 2, '2'),
(3, 3, '3');
```

Create the dictionary:

```sql
CREATE DICTIONARY ext_dict_mult
(
    id UInt32,
    c1 UInt32,
    c2 String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dict_mult_source' DB 'default'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);
```
</details>

<details>
<summary>Example hierarchical dictionary</summary>

Create the source table:

```sql
CREATE TABLE hierarchy_source
(
  id UInt64,
  parent_id UInt64,
  name String
) ENGINE = Memory;
```

Insert the data into the source table:

```sql
INSERT INTO hierarchy_source VALUES
(0, 0, 'Root'),
(1, 0, 'Level 1 - Node 1'),
(2, 1, 'Level 2 - Node 2'),
(3, 1, 'Level 2 - Node 3'),
(4, 2, 'Level 3 - Node 4'),
(5, 2, 'Level 3 - Node 5'),
(6, 3, 'Level 3 - Node 6');

-- 0 (Root)
-- └── 1 (Level 1 - Node 1)
--     ├── 2 (Level 2 - Node 2)
--     │   ├── 4 (Level 3 - Node 4)
--     │   └── 5 (Level 3 - Node 5)
--     └── 3 (Level 2 - Node 3)
--         └── 6 (Level 3 - Node 6)
```

Create the dictionary:

```sql
CREATE DICTIONARY hierarchical_dictionary
(
    id UInt64,
    parent_id UInt64 HIERARCHICAL,
    name String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'hierarchy_source' DB 'default'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);
```
</details>

<!-- 
The inner content of the tags below are replaced at doc framework build time with 
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->
