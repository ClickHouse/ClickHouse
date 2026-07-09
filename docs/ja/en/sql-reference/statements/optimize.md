---
description: 'OPTIMIZE のドキュメント'
sidebar_label: 'OPTIMIZE'
sidebar_position: 47
slug: /sql-reference/statements/optimize
title: 'OPTIMIZE ステートメント'
doc_type: 'reference'
---

このクエリは、テーブルのデータパーツに対して、スケジュールされていないマージの開始を試みます。なお、`OPTIMIZE TABLE ... FINAL` の使用は通常推奨されません ([こちらのドキュメント](/ja/optimize/avoidoptimizefinal)を参照してください) 。これは、日常的な運用ではなく管理作業での利用を想定したものだからです。

:::note
`OPTIMIZE` では `パーツが多すぎる` エラーを解消できません。
:::

**構文**

```sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL | FORCE] [DEDUPLICATE [BY expression]]
```

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

`OPTIMIZE` クエリは、[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) ファミリー ([materialized view](/ja/sql-reference/statements/create/view#materialized-view) を含む) および [Buffer](../../engines/table-engines/special/buffer.md) エンジンでサポートされています。その他のテーブルエンジンはサポートされていません。

`OPTIMIZE` を [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) ファミリーのテーブルエンジンで使用すると、ClickHouse はマージ用のタスクを作成し、すべてのレプリカでの実行を待機します ([alter&#95;sync](/ja/operations/settings/settings#alter_sync) 設定が `2` の場合) 。または、現在のレプリカでの実行を待機します ([alter&#95;sync](/ja/operations/settings/settings#alter_sync) 設定が `1` の場合) 。

* `OPTIMIZE` が何らかの理由でマージを実行しなかった場合でも、クライアントには通知されません。通知を有効にするには、[optimize&#95;throw&#95;if&#95;noop](/ja/operations/settings/settings#optimize_throw_if_noop) 設定を使用してください。
* `PARTITION` を指定すると、指定したパーティションだけが最適化されます。[パーティション式の設定方法](alter/partition.md#how-to-set-partition-expression)。
* `FINAL` または `FORCE` を指定すると、すべてのデータがすでに 1 つのパーツにある場合でも最適化が実行されます。この動作は [optimize&#95;skip&#95;merged&#95;partitions](/ja/operations/settings/settings#optimize_skip_merged_partitions) で制御できます。また、同時実行のマージが行われている場合でも、マージは強制的に実行されます。
* `DEDUPLICATE` を指定すると、完全に同一の行 (BY 句が指定されていない場合) は重複排除されます (すべてのカラムが比較されます) 。これは MergeTree エンジンでのみ意味があります。

非アクティブなレプリカが `OPTIMIZE` クエリを実行するのをどれくらいの時間 (秒単位) 待機するかは、[replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ja/operations/settings/settings#replication_wait_for_inactive_replica_timeout) 設定で指定できます。

:::note
`alter_sync` が `2` に設定されていて、一部のレプリカが `replication_wait_for_inactive_replica_timeout` 設定で指定された時間を超えて非アクティブな場合は、例外 `UNFINISHED` がスローされます。
:::

<div id="dry-run">
  ## DRY RUN
</div>

`DRY RUN` 句は、結果をコミットせずに指定したパーツのマージをシミュレートします。マージ後のパーツは一時的な場所に書き込まれ、検証された後に破棄されます。元のパーツおよびテーブルデータは変更されません。

これは次のような場合に役立ちます。

* ClickHouse のバージョン間でマージの正確性をテストする。
* マージ関連のバグを決定論的に再現する。
* マージのパフォーマンスをベンチマークする。

`DRY RUN` は [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) ファミリーのテーブルでのみサポートされています。パーツ名の一覧を伴う `PARTS` キーワードが必要です。指定するすべてのパーツは、存在していてアクティブであり、同じパーティションに属している必要があります。

`DRY RUN` は `FINAL` および `PARTITION` とは併用できません。`DEDUPLICATE` (任意でカラム指定可能) および `CLEANUP` (`ReplacingMergeTree` テーブル向け) とは組み合わせて使用できます。

**構文**

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

デフォルトでは、生成されたマージ後のパーツは [`CHECK TABLE`](/ja/sql-reference/statements/check-table) クエリと同様の方法で検証されます。この動作は [optimize&#95;dry&#95;run&#95;check&#95;part](/ja/operations/settings/settings#optimize_dry_run_check_part) 設定で制御されており、デフォルトで有効になっています。これを無効にすると検証がスキップされるため、マージ処理自体をベンチマークする場合に役立ちます。

**例**

```sql
CREATE TABLE dry_run_example (key UInt64, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO dry_run_example VALUES (1, 'a'), (2, 'b');
INSERT INTO dry_run_example VALUES (1, 'c'), (4, 'd');

-- Simulate merging using two parts
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0';

-- Simulate merging with deduplication
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0' DEDUPLICATE;

-- Parts and data remain unchanged after DRY RUN
SELECT name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 'dry_run_example' AND active
ORDER BY name;
```

```response
┌─name────────┬─rows─┐
│ all_1_1_0   │    2 │
│ all_2_2_0   │    2 │
└─────────────┴──────┘
```

<div id="by-expression">
  ## BY 式
</div>

すべてのカラムではなく、特定のカラムの組み合わせに対して重複排除を行う場合は、カラムのリストを明示的に指定するか、[`*`](../../sql-reference/statements/select/index.md#asterisk)、[`COLUMNS`](/ja/sql-reference/statements/select#select-clause)、[`EXCEPT`](/ja/sql-reference/statements/select/except-modifier) 式を任意に組み合わせて指定できます。明示的に記述した、または暗黙的に展開されたカラムのリストには、行の並び順を決定する式 (プライマリキーとソートキーの両方) およびパーティション化式 (パーティションキー) で指定されているすべてのカラムを含める必要があります。

:::note
`*` は `SELECT` と同様に動作することに注意してください。[MATERIALIZED](/ja/sql-reference/statements/create/view#materialized-view) カラムおよび [ALIAS](../../sql-reference/statements/create/table.md#alias) カラムは、展開対象に含まれません。

また、空のカラムリストを指定したり、結果として空のカラムリストになる式を記述したり、`ALIAS` カラムで重複排除を行ったりすると、エラーになります。
:::

**構文**

```sql
OPTIMIZE TABLE table DEDUPLICATE; -- all columns
OPTIMIZE TABLE table DEDUPLICATE BY *; -- excludes MATERIALIZED and ALIAS columns
OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

**例**

次のテーブルについて考えてみましょう：

```sql title="Query"
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

```sql title="Query"
INSERT INTO example (primary_key, secondary_key, value, partition_key)
VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
```

```sql title="Query"
SELECT * FROM example;
```

```sql title="Response"

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

以下の例はすべて、5行あるこの状態に対して実行されます。

<div id="deduplicate">
  #### `DEDUPLICATE`
</div>

重複排除の対象となるカラムが指定されていない場合は、すべてのカラムが対象になります。行が削除されるのは、すべてのカラムの値が前の行の対応する値とすべて一致する場合に限られます。

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
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

<div id="deduplicate-by-">
  #### `DEDUPLICATE BY *`
</div>

カラムが暗黙的に指定されている場合、テーブルは `ALIAS` または `MATERIALIZED` ではないすべてのカラムを基準に重複排除されます。上記のテーブルでは、該当するのは `primary_key`、`secondary_key`、`value`、`partition_key` の各カラムです。

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
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

<div id="deduplicate-by--except">
  #### `DEDUPLICATE BY * EXCEPT`
</div>

`ALIAS` または `MATERIALIZED` ではなく、さらに明示的に `value` でもない、つまり `primary_key`、`secondary_key`、`partition_key` の各カラムを対象に重複排除します。

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT value;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
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

<div id="deduplicate-by-list-of-columns">
  #### `DEDUPLICATE BY <list of columns>`
</div>

重複を排除する対象として、`primary_key`、`secondary_key`、`partition_key` の各カラムを明示的に指定します:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
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

<div id="deduplicate-by-columnsregex">
  #### `DEDUPLICATE BY COLUMNS(<regex>)`
</div>

正規表現に一致するすべてのカラム (`primary_key`、`secondary_key`、`partition_key`) で重複を排除します:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
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