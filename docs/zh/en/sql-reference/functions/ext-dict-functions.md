---
description: '字典函数文档'
sidebar_label: '字典'
slug: /sql-reference/functions/ext-dict-functions
title: '字典函数'
doc_type: 'reference'
---

:::note
对于使用 [DDL queries](../statements/create/dictionary/overview.md) 创建的字典，`dict_name` 参数必须写成完整形式，例如 `<database>.<dict_name>`。否则，将使用当前数据库。
:::

有关如何连接和配置字典，请参阅[字典](../statements/create/dictionary/overview.md)。

<div id="example-dictionary">
  ## 示例字典
</div>

本节中的示例会用到以下字典。你可以在 ClickHouse 中创建它们，
以运行下文所述函数的示例。

<details>
  <summary>`dictGet<T>` 和 `dictGet<T>OrDefault` 函数的示例字典</summary>

  ```sql
  -- 创建包含所有必需数据类型的表
  CREATE TABLE all_types_test (
      `id` UInt32,
      
      -- String 类型
      `String_value` String,
      
      -- 无符号整数类型
      `UInt8_value` UInt8,
      `UInt16_value` UInt16,
      `UInt32_value` UInt32,
      `UInt64_value` UInt64,
      
      -- 有符号整数类型
      `Int8_value` Int8,
      `Int16_value` Int16,
      `Int32_value` Int32,
      `Int64_value` Int64,
      
      -- 浮点类型
      `Float32_value` Float32,
      `Float64_value` Float64,
      
      -- 日期/时间类型
      `Date_value` Date,
      `DateTime_value` DateTime,
      
      -- 网络类型
      `IPv4_value` IPv4,
      `IPv6_value` IPv6,
      
      -- UUID 类型
      `UUID_value` UUID
  ) ENGINE = MergeTree() 
  ORDER BY id;
  ```

  ```sql
  -- 插入测试数据
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
  -- 创建字典
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
  <summary>`dictGetAll` 的示例字典</summary>

  创建一个表，用于存储 regexp tree 字典的数据：

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

  向表中插入数据：

  ```sql
  INSERT INTO regexp_os 
  SELECT *
  FROM s3(
      'https://datasets-documentation.s3.eu-west-3.amazonaws.com/' ||
      'user_agent_regex/regexp_os.csv'
  );
  ```

  创建 regexp tree 字典：

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
  <summary>示例范围键字典</summary>

  创建输入表：

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

  向输入表中插入数据：

  ```sql
  INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
  INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
  INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
  ```

  创建字典：

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
  <summary>示例复合键字典</summary>

  创建源表：

  ```sql
  CREATE TABLE dict_mult_source
  (
  id UInt32,
  c1 UInt32,
  c2 String
  ) ENGINE = Memory;
  ```

  向源表中插入数据：

  ```sql
  INSERT INTO dict_mult_source VALUES
  (1, 1, '1'),
  (2, 2, '2'),
  (3, 3, '3');
  ```

  创建字典：

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
  <summary>示例层级字典</summary>

  创建源表：

  ```sql
  CREATE TABLE hierarchy_source
  (
    id UInt64,
    parent_id UInt64,
    name String
  ) ENGINE = Memory;
  ```

  向源表中插入数据：

  ```sql
  INSERT INTO hierarchy_source VALUES
  (0, 0, 'Root'),
  (1, 0, 'Level 1 - Node 1'),
  (2, 1, 'Level 2 - Node 2'),
  (3, 1, 'Level 2 - Node 3'),
  (4, 2, 'Level 3 - Node 4'),
  (5, 2, 'Level 3 - Node 5'),
  (6, 3, 'Level 3 - Node 6');

  -- 0（根）
  -- └── 1（第 1 层 - 节点 1）
  --     ├── 2（第 2 层 - 节点 2）
  --     │   ├── 4（第 3 层 - 节点 4）
  --     │   └── 5（第 3 层 - 节点 5）
  --     └── 3（第 2 层 - 节点 3）
  --         └── 6（第 3 层 - 节点 6）
  ```

  创建字典：

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

<div id="passing-keys">
  ## 向字典函数传递键值
</div>

`dictGet`、`dictGetOrDefault`、`dictGetOrNull` 和 `dictHas` 等函数的键参数 (`id_expr`) 取决于字典所使用的键类型：

* 对于使用**简单键** (`UInt64`) 的字典，直接传入键值：

```sql
SELECT dictGet('simple_key_dictionary', 'attr_name', toUInt64(1));
```

* 对于使用多个属性组成的**复合 (复杂) 键**的字典，请将键值作为元组传递：

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', ('value_for_field1', 42));
```

* 当**复合键仅包含一个属性**时，可以直接传递键值，无需将其包装在 `tuple` 中。以下两种写法都有效且等价：

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', 'key');
SELECT dictGet('complex_key_dictionary', 'attr_name', tuple('key'));
```

这也适用于 `ip_trie` 字典，其键是单个属性。要查找的 IP 地址可以直接传入：

```sql
SELECT dictGet('ip_trie_dictionary', 'attr_name', toIPv4('202.79.32.10'));
```

{/* 
  下面这些标签中的内容会在文档框架构建时
  替换为根据 system.functions 生成的文档。请勿修改或删除这些标签。
  参见：https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }