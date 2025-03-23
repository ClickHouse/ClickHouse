---
slug: /ja/sql-reference/statements/create/dictionary
sidebar_position: 38
sidebar_label: DICTIONARY
title: "CREATE DICTIONARY"
---

指定された[構造](../../../sql-reference/dictionaries/index.md#dictionary-key-and-fields)、[ソース](../../../sql-reference/dictionaries/index.md#dictionary-sources)、[レイアウト](../../../sql-reference/dictionaries/index.md#storig-dictionaries-in-memory)、および[有効期間](../../../sql-reference/dictionaries/index.md#dictionary-updates)を持つ新しい[Dictionary](../../../sql-reference/dictionaries/index.md)を作成します。

## 文法

``` sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2],
    attr1 type2 [DEFAULT|EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2 [DEFAULT|EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

Dictionaryの構造は属性で構成されます。Dictionaryの属性は、テーブルのカラムに類似して指定されます。唯一必要なのはそのタイプで、他のプロパティはデフォルト値を持つことができます。

`ON CLUSTER`句は、クラスター上でDictionaryを作成することを可能にします。詳細は[分散DDL](../../../sql-reference/distributed-ddl.md)を参照してください。

Dictionaryの[レイアウト](../../../sql-reference/dictionaries/index.md#storig-dictionaries-in-memory)に応じて、1つまたは複数の属性をDictionaryのキーとして指定できます。

## SOURCE

Dictionaryのソースは以下のものが可能です:
- 現在のClickHouseサービス内のテーブル
- リモートClickHouseサービス内のテーブル
- HTTP(S)でアクセス可能なファイル
- 他のデータベース

### 現在のClickHouseサービス内のテーブルからDictionaryを作成する

入力テーブル `source_table`:

``` text
┌─id─┬─value──┐
│  1 │ First  │
│  2 │ Second │
└────┴────────┘
```

Dictionaryの作成:

``` sql
CREATE DICTIONARY id_value_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
```

Dictionaryを出力する:

``` sql
SHOW CREATE DICTIONARY id_value_dictionary;
```

```response
CREATE DICTIONARY default.id_value_dictionary
(
    `id` UInt64,
    `value` String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_table'))
LIFETIME(MIN 0 MAX 1000)
LAYOUT(FLAT())
```

:::note
[ClickHouse Cloud](https://clickhouse.com)でSQLコンソールを使用する際には、Dictionaryを作成する際にユーザー（`default`または役割`default_role`を持つ他のユーザー）とパスワードを指定する必要があります。
:::note

```sql
CREATE USER IF NOT EXISTS clickhouse_admin
IDENTIFIED WITH sha256_password BY 'passworD43$x';

GRANT default_role TO clickhouse_admin;

CREATE DATABASE foo_db;

CREATE TABLE foo_db.source_table (
    id UInt64,
    value String
) ENGINE = MergeTree
PRIMARY KEY id;

CREATE DICTIONARY foo_db.id_value_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_table' USER 'clickhouse_admin' PASSWORD 'passworD43$x' DB 'foo_db' ))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);
```

### リモートClickHouseサービスのテーブルからDictionaryを作成する

入力テーブル（リモートClickHouseサービス内）`source_table`:

``` text
┌─id─┬─value──┐
│  1 │ First  │
│  2 │ Second │
└────┴────────┘
```

Dictionaryの作成:

``` sql
CREATE DICTIONARY id_value_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'HOSTNAME' PORT 9000 USER 'default' PASSWORD 'PASSWORD' TABLE 'source_table' DB 'default'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
```

### HTTP(S)でアクセス可能なファイルからDictionaryを作成する

```sql
CREATE DICTIONARY default.taxi_zone_dictionary
(
    `LocationID` UInt16 DEFAULT 0,
    `Borough` String,
    `Zone` String,
    `service_zone` String
)
PRIMARY KEY LocationID
SOURCE(HTTP(URL 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv' FORMAT 'CSVWithNames'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED())
```

### 他のデータベースからDictionaryを作成する

詳細は[Dictionary ソース](/docs/ja/sql-reference/dictionaries/index.md#dictionary-sources/#dbms)をご参照ください。

**関連情報**

- 詳細については、[Dictionaries](../../../sql-reference/dictionaries/index.md)のセクションをご覧ください。
- [system.dictionaries](../../../operations/system-tables/dictionaries.md) — このテーブルには[Dictionary](../../../sql-reference/dictionaries/index.md)に関する情報が含まれています。
