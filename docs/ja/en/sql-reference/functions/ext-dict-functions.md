---
description: 'Dictionary を操作する関数のドキュメント'
sidebar_label: 'Dictionaries'
slug: /sql-reference/functions/ext-dict-functions
title: 'Dictionary を操作する関数'
doc_type: 'reference'
---

:::note
[DDL queries](../statements/create/dictionary/overview.md) で作成された Dictionary では、`dict_name` パラメータを `<database>.<dict_name>` のように完全修飾で指定する必要があります。そうしない場合は、現在のデータベースが使用されます。
:::

Dictionary の接続方法と設定については、[Dictionaries](../statements/create/dictionary/overview.md) を参照してください。

<div id="example-dictionary">
  ## Dictionary の例
</div>

このセクションの例では、以下の Dictionary を使用します。以下で説明する関数の例を実行できるように、
これらを ClickHouse で作成できます。

<details>
  <summary>dictGet&lt;T&gt; および dictGet&lt;T&gt;OrDefault 関数用の Dictionary の例</summary>

  ```sql
  -- 必要なすべてのデータ型を含むテーブルを作成
  CREATE TABLE all_types_test (
      `id` UInt32,
      
      -- String 型
      `String_value` String,
      
      -- 符号なし整数型
      `UInt8_value` UInt8,
      `UInt16_value` UInt16,
      `UInt32_value` UInt32,
      `UInt64_value` UInt64,
      
      -- 符号付き整数型
      `Int8_value` Int8,
      `Int16_value` Int16,
      `Int32_value` Int32,
      `Int64_value` Int64,
      
      -- 浮動小数点型
      `Float32_value` Float32,
      `Float64_value` Float64,
      
      -- 日付/時刻型
      `Date_value` Date,
      `DateTime_value` DateTime,
      
      -- ネットワーク型
      `IPv4_value` IPv4,
      `IPv6_value` IPv6,
      
      -- UUID 型
      `UUID_value` UUID
  ) ENGINE = MergeTree() 
  ORDER BY id;
  ```

  ```sql
  -- テストデータを挿入
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
  -- Dictionary を作成
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
  <summary>dictGetAll 用の Dictionary の例</summary>

  regexp tree dictionary のデータを格納するテーブルを作成します。

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

  テーブルにデータを挿入します。

  ```sql
  INSERT INTO regexp_os 
  SELECT *
  FROM s3(
      'https://datasets-documentation.s3.eu-west-3.amazonaws.com/' ||
      'user_agent_regex/regexp_os.csv'
  );
  ```

  regexp tree dictionary を作成します。

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
  <summary>範囲キーDictionaryの例</summary>

  入力テーブルを作成します。

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

  入力テーブルにデータを挿入します。

  ```sql
  INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
  INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
  INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
  ```

  Dictionaryを作成します。

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
  <summary>複合キーDictionaryの例</summary>

  ソーステーブルを作成します。

  ```sql
  CREATE TABLE dict_mult_source
  (
  id UInt32,
  c1 UInt32,
  c2 String
  ) ENGINE = Memory;
  ```

  ソーステーブルにデータを挿入します。

  ```sql
  INSERT INTO dict_mult_source VALUES
  (1, 1, '1'),
  (2, 2, '2'),
  (3, 3, '3');
  ```

  Dictionaryを作成します。

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
  <summary>階層Dictionaryの例</summary>

  ソーステーブルを作成します。

  ```sql
  CREATE TABLE hierarchy_source
  (
    id UInt64,
    parent_id UInt64,
    name String
  ) ENGINE = Memory;
  ```

  ソーステーブルにデータを挿入します。

  ```sql
  INSERT INTO hierarchy_source VALUES
  (0, 0, 'Root'),
  (1, 0, 'Level 1 - Node 1'),
  (2, 1, 'Level 2 - Node 2'),
  (3, 1, 'Level 2 - Node 3'),
  (4, 2, 'Level 3 - Node 4'),
  (5, 2, 'Level 3 - Node 5'),
  (6, 3, 'Level 3 - Node 6');

  -- 0 (ルート)
  -- └── 1 (レベル 1 - ノード 1)
  --     ├── 2 (レベル 2 - ノード 2)
  --     │   ├── 4 (レベル 3 - ノード 4)
  --     │   └── 5 (レベル 3 - ノード 5)
  --     └── 3 (レベル 2 - ノード 3)
  --         └── 6 (レベル 3 - ノード 6)
  ```

  Dictionaryを作成します。

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
  ## Dictionary関数へのキーの渡し方
</div>

`dictGet`、`dictGetOrDefault`、`dictGetOrNull`、`dictHas` などの関数におけるキー引数 (`id_expr`) は、Dictionaryのキーによって異なります。

* **単純キー** (`UInt64`) を持つDictionaryの場合は、キーの値を直接渡します。

```sql
SELECT dictGet('simple_key_dictionary', 'attr_name', toUInt64(1));
```

* 複数の属性からなる**複合 (complex) キー**を持つDictionaryでは、キーの値をタプルで渡します。

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', ('value_for_field1', 42));
```

* **複合キーが1つの属性だけで構成されている**場合、キー値は `tuple` で囲まずに直接渡せます。次のどちらも有効で、同等です：

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', 'key');
SELECT dictGet('complex_key_dictionary', 'attr_name', tuple('key'));
```

これは、キーが単一の属性である `ip_trie` Dictionaryにも適用されます。検索対象の IP アドレスは直接渡せます。

```sql
SELECT dictGet('ip_trie_dictionary', 'attr_name', toIPv4('202.79.32.10'));
```

{/* 
  以下のタグ内の内容は、ドキュメントフレームワークの build 時に
  system.functions から生成されたドキュメントで置き換えられます。タグは変更または削除しないでください。
  参照: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }