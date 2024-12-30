---
slug: /ja/sql-reference/statements/alter/projection
sidebar_position: 49
sidebar_label: PROJECTION
title: "プロジェクション"
---

プロジェクションはクエリ実行を最適化する形式でデータを保存します。この機能は次の用途に便利です：
- 主キーの一部でないカラムに対するクエリの実行
- カラムの事前集計により、計算とIOの両方を削減

テーブルに対して1つ以上のプロジェクションを定義することができ、クエリ解析時にユーザーの指定したクエリを変更せずに、スキャンすべきデータが最小のプロジェクションをClickHouseが選択します。

:::note ディスク使用量

プロジェクションは内部で新しい隠しテーブルを作成するため、より多くのIOとディスクスペースが必要になります。
例として、異なる主キーを定義したプロジェクションがある場合、元のテーブルからのすべてのデータが複製されます。
:::

プロジェクションの内部動作に関する技術的な詳細はこの[ページ](/docs/ja/guides/best-practices/sparse-primary-indexes.md/#option-3-projections)をご覧ください。

## 主キーを使用せずにフィルタリングする例

テーブルの作成：
```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```
`ALTER TABLE`を使って既存のテーブルにプロジェクションを追加することができます：
```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
SELECT
*
ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```
データの挿入：
```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

プロジェクションを使用することで、元のテーブルで`user_name`が`PRIMARY_KEY`として定義されていなくても、`user_name`でのフィルタリングを素早く行えます。クエリ時にClickHouseは、プロジェクションを使用することでデータの処理量が少なくなると判断します。なぜなら、データが`user_name`でソートされているからです。
```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

クエリがプロジェクションを使用しているか確認するには、`system.query_log`テーブルを確認します。`projections`フィールドには使用されたプロジェクションの名前が入り、未使用の場合は空になります：
```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

## 集計前のクエリの例

プロジェクションを使用してテーブルを作成：
```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```
データの挿入：
```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```
```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```
`user_agent`フィールドを使った`GROUP BY`クエリを最初に実行します。このクエリは事前集計が一致しないため、定義されたプロジェクションを使用しません。
```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

プロジェクションを使用するためには、事前集計または`GROUP BY`フィールドの一部またはすべてを選択するクエリを実行します。
```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```
```
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

前述のように、`system.query_log`テーブルを確認できます。`projections`フィールドには使用されたプロジェクションの名前が入り、未使用の場合は空になります：
```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

# プロジェクションの操作

次の操作が[プロジェクション](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#projections)で利用可能です：

## ADD PROJECTION

`ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY] )` - テーブルのメタデータにプロジェクションの説明を追加します。

## DROP PROJECTION

`ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name` - テーブルのメタデータからプロジェクションの説明を削除し、プロジェクションのファイルをディスクから削除します。[ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

## MATERIALIZE PROJECTION

`ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]` - 指定されたパーティション`partition_name`内のプロジェクション`name`を再構築します。[ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

## CLEAR PROJECTION

`ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]` - 説明を削除せずにプロジェクションのファイルをディスクから削除します。[ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

`ADD`、`DROP`、`CLEAR`コマンドはメタデータの変更やファイルの削除のみを行う軽量な操作です。

また、これらはClickHouse KeeperまたはZooKeeperを介してプロジェクションのメタデータを同期することによりレプリケーションされます。

:::note
プロジェクションの操作は[`*MergeTree`](/docs/ja/engines/table-engines/mergetree-family/mergetree.md)エンジン（[レプリケート](/docs/ja/engines/table-engines/mergetree-family/replication.md)されたバリアントを含む）のテーブルでのみサポートされています。
:::
