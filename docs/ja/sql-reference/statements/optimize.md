---
slug: /ja/sql-reference/statements/optimize
sidebar_position: 47
sidebar_label: OPTIMIZE
title: "OPTIMIZE ステートメント"
---

このクエリはテーブルのデータパーツのスケジュールされていないマージを初期化しようとします。`OPTIMIZE TABLE ... FINAL` の使用は管理を目的としたものであり、日常的な操作には推奨しないことに注意してください（詳細は[こちらのドキュメント](/docs/ja/optimize/avoidoptimizefinal)を参照）。

:::note
`OPTIMIZE` は `Too many parts` エラーを修正できません。
:::

**構文**

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE [BY expression]]
```

`OPTIMIZE` クエリは [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) ファミリー（[マテリアライズドビュー](../../sql-reference/statements/create/view.md#materialized-view)を含む）および [Buffer](../../engines/table-engines/special/buffer.md) エンジンでサポートされています。他のテーブルエンジンではサポートされていません。

`OPTIMIZE` が [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) ファミリーのテーブルエンジンで使用される場合、ClickHouse はマージのタスクを作成し、すべてのレプリカでの実行を待機します（[alter_sync](../../operations/settings/settings.md#alter-sync) 設定が `2` に設定されている場合）または現在のレプリカで（[alter_sync](../../operations/settings/settings.md#alter-sync) 設定が `1` に設定されている場合）。

- `OPTIMIZE` が何らかの理由でマージを行わない場合、クライアントには通知されません。通知を有効にするには、[optimize_throw_if_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) 設定を使用してください。
- `PARTITION` を指定すると、指定されたパーティションのみが最適化されます。[パーティション式の設定方法](alter/partition.md#how-to-set-partition-expression)。
- `FINAL` を指定すると、すべてのデータがすでに1つのパーツにある場合でも最適化が行われます。この動作は [optimize_skip_merged_partitions](../../operations/settings/settings.md#optimize-skip-merged-partitions) で制御できます。同時にマージが行われている場合でも、マージは強制されます。
- `DEDUPLICATE` を指定すると、完全に同一の行（by-clauseが指定されていない限り）が重複排除されます（すべてのカラムが比較されます）。これは MergeTree エンジンにのみ意味があります。

`OPTIMIZE` クエリの実行のために非アクティブなレプリカを待つ時間（秒）を指定するには、[replication_wait_for_inactive_replica_timeout](../../operations/settings/settings.md#replication-wait-for-inactive-replica-timeout) 設定を使用します。

:::note    
`alter_sync` が `2` に設定されていて、いくつかのレプリカが `replication_wait_for_inactive_replica_timeout` 設定で指定された時間を超えて非アクティブである場合、`UNFINISHED` 例外がスローされます。
:::

## BY 式

重複排除をすべてのカラムではなくカスタムセットのカラムで行いたい場合、カラムのリストを明示的に指定するか、[`*`](../../sql-reference/statements/select/index.md#asterisk)、[`COLUMNS`](../../sql-reference/statements/select/index.md#columns-expression)、[`EXCEPT`](../../sql-reference/statements/select/index.md#except-modifier) 式の任意の組み合わせを使用できます。明示的または暗黙に展開されたカラムリストには、行の順序付け式（主キーおよびソートキー）およびパーティショニング式（パーティションキー）で指定されたすべてのカラムを含める必要があります。

:::note    
`*` は `SELECT` と同じように動作します：[MATERIALIZED](../../sql-reference/statements/create/table.md#materialized) および [ALIAS](../../sql-reference/statements/create/table.md#alias) カラムは展開用に使用されません。

また、カラムの空のリストを指定したり、空のリストになる式を書いたり、`ALIAS` カラムで重複排除を行うことはエラーになります。
:::

**構文**

``` sql
OPTIMIZE TABLE table DEDUPLICATE; -- すべてのカラム
OPTIMIZE TABLE table DEDUPLICATE BY *; -- MATERIALIZED および ALIAS カラムを除外する
OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

**例**

以下のようなテーブルを考えます：

``` sql
CREATE TABLE example (
    primary_key Int32,
    secondary_key Int32,
    value UInt32,
    partition_key UInt32,
    materialized_value UInt32 MATERIALIZED 12345,
    aliased_value UInt32 ALIAS 2,
    PRIMARY KEY primary_key
) ENGINE=MergeTree
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);
```
``` sql
INSERT INTO example (primary_key, secondary_key, value, partition_key)
VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
```
``` sql
SELECT * FROM example;
```
結果:
```

┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```
次に挙げるすべての例は、この5行から成る状態に対して実行されます。

#### `DEDUPLICATE`
重複排除のためのカラムが指定されていない場合、すべてのカラムが考慮されます。すべてのカラムの値が前の行の対応する値と等しい場合に限り、行が削除されます：

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```
``` sql
SELECT * FROM example;
```
結果:
```
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```
#### `DEDUPLICATE BY *`
カラムが暗黙的に指定されると、テーブルは `ALIAS` または `MATERIALIZED` でないすべてのカラムで重複排除されます。上記のテーブルを考慮すると、これらは `primary_key`、`secondary_key`、`value`、`partition_key` カラムです：
```sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```
``` sql
SELECT * FROM example;
```
結果:
```
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```
#### `DEDUPLICATE BY * EXCEPT`
`ALIAS` または `MATERIALIZED` でないすべてのカラムで重複排除し、明示的に `value` 以外とする：`primary_key`、`secondary_key`、`partition_key` カラム。

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT value;
```
``` sql
SELECT * FROM example;
```
結果:
```
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```
#### `DEDUPLICATE BY <list of columns>`
`primary_key`、`secondary_key`、`partition_key` カラムで明示的に重複排除：
```sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```
``` sql
SELECT * FROM example;
```
結果:
```
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```
#### `DEDUPLICATE BY COLUMNS(<regex>)`
正規表現に一致するすべてのカラムで重複排除：`primary_key`、`secondary_key`、`partition_key` カラム：
```sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```
``` sql
SELECT * FROM example;
```
結果:
```
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```
