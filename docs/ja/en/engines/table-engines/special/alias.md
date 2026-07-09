---
description: 'Alias テーブルエンジンは、別のテーブルに対する透過的なプロキシを作成します。すべての操作はターゲットテーブルに転送され、エイリアス自体はデータを保存しません。'
sidebar_label: 'Alias'
sidebar_position: 5
slug: /engines/table-engines/special/alias
title: 'Alias テーブルエンジン'
doc_type: 'reference'
---

<div id="alias-table-engine">
  # Alias テーブルエンジン
</div>

`Alias` エンジンは、別のテーブルへのプロキシを作成します。すべての読み取りおよび書き込み操作はターゲットテーブルに転送され、エイリアス自体はデータを保持せず、ターゲットテーブルへの参照のみを持ちます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_table)
```

または、データベース名を明示する場合:

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_db, target_table)
```

:::note
`Alias` テーブルでは、明示的なカラム定義はサポートされていません。カラムはターゲットテーブルから自動的に継承されるため、エイリアスは常にターゲットテーブルのスキーマと一致します。
:::

<div id="engine-parameters">
  ## エンジンパラメータ
</div>

* **`target_db (optional)`** — ターゲットテーブルを含むデータベース名。
* **`target_table`** — ターゲットテーブル名。

:::note
`target_db` を省略し、`target_table` が完全修飾されていない場合 (例: `Alias('my_table')`) 、ターゲットはセッションの現在のデータベースではなく、エイリアス自体と同じデータベースとして解決されます。
:::

<div id="supported-operations">
  ## サポートされている操作
</div>

`Alias` テーブルエンジンは、主要な操作をすべてサポートしています。 

<div id="operations-on-target">
  ### ターゲットテーブルに対する操作
</div>

これらの操作はターゲットテーブルに対してプロキシされます。

| Operation                    | Support | Description                              |
| ---------------------------- | ------- | ---------------------------------------- |
| `SELECT`                     | ✅       | ターゲットテーブルからデータを読み取る                      |
| `INSERT`                     | ✅       | ターゲットテーブルにデータを書き込む                       |
| `INSERT SELECT`              | ✅       | ターゲットテーブルにデータを一括挿入する                     |
| `ALTER TABLE ADD COLUMN`     | ✅       | ターゲットテーブルにカラムを追加する                       |
| `ALTER TABLE MODIFY SETTING` | ✅       | ターゲットテーブルのテーブル設定を変更する                    |
| `ALTER TABLE PARTITION`      | ✅       | ターゲットに対するパーティション操作 (DETACH/ATTACH/DROP)  |
| `ALTER TABLE UPDATE`         | ✅       | ターゲットテーブル内の行を更新する (mutation)             |
| `ALTER TABLE DELETE`         | ✅       | ターゲットテーブルから行を削除する (mutation)             |
| `OPTIMIZE TABLE`             | ✅       | ターゲットテーブルを最適化する (パーツをマージ)                |
| `TRUNCATE TABLE`             | ✅       | ターゲットテーブルをTRUNCATEする                     |

<div id="operations-on-alias">
  ### エイリアス自体に対する操作
</div>

これらの操作はエイリアスにのみ影響し、ターゲットテーブルには**影響しません**。

| 操作             | サポート | 説明                               |
| -------------- | ---- | -------------------------------- |
| `DROP TABLE`   | ✅    | エイリアスのみを削除し、ターゲットテーブルは変更されません    |
| `RENAME TABLE` | ✅    | エイリアスの名前のみを変更し、ターゲットテーブルは変更されません |

<div id="usage-examples">
  ## 使用例
</div>

<div id="basic-alias-creation">
  ### 基本的なエイリアスの作成
</div>

同じデータベース内にシンプルなエイリアスを作成します。

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

<div id="cross-database-alias">
  ### データベースをまたぐエイリアス
</div>

別のデータベース内のテーブルを指すエイリアスを作成します:

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

<div id="write-operations">
  ### エイリアス経由の書き込み操作
</div>

すべての書き込み操作はターゲットテーブルに転送されます：

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

<div id="schema-modification">
  ### スキーマの変更
</div>

ALTER 操作では、ターゲットテーブルのスキーマが変更されます。

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

<div id="data-mutations">
  ### データミューテーション
</div>

UPDATE および DELETE 操作をサポートしています。

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

<div id="partition-operations">
  ### パーティション操作
</div>

パーティション化されたテーブルでは、パーティションに対する操作は転送されます。

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

<div id="table-optimization">
  ### テーブルの最適化
</div>

ターゲットテーブル内でパーツをマージする操作を最適化します。

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

<div id="alias-management">
  ### エイリアスの管理
</div>

エイリアスはそれぞれ個別にリネームまたは削除できます。

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