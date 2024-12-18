---
slug: /ja/operations/system-tables/mutations
---
# mutations

このテーブルには、[MergeTree](/docs/ja/engines/table-engines/mergetree-family/mergetree.md)テーブルの[ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#mutations)とその進捗状況に関する情報が含まれています。各ミューテーションコマンドは単一の行で表されます。

## カラム:

- `database` ([String](/docs/ja/sql-reference/data-types/string.md)) — ミューテーションが適用されたデータベースの名前。

- `table` ([String](/docs/ja/sql-reference/data-types/string.md)) — ミューテーションが適用されたテーブルの名前。

- `mutation_id` ([String](/docs/ja/sql-reference/data-types/string.md)) — ミューテーションのID。レプリケートされたテーブルの場合、これらのIDはClickHouse Keeperの`<table_path_in_clickhouse_keeper>/mutations/`ディレクトリ内のznode名に対応します。非レプリケートされたテーブルの場合、IDはテーブルのデータディレクトリ内のファイル名に対応します。

- `command` ([String](/docs/ja/sql-reference/data-types/string.md)) — ミューテーションコマンド文字列（`ALTER TABLE [db.]table`の後のクエリ部分）。

- `create_time` ([DateTime](/docs/ja/sql-reference/data-types/datetime.md)) — ミューテーションコマンドが実行のために送信された日時。

- `block_numbers.partition_id` ([Array](/docs/ja/sql-reference/data-types/array.md)([String](/docs/ja/sql-reference/data-types/string.md))) — レプリケートされたテーブルのミューテーションの場合、配列はパーティションのIDを含みます（各パーティションに1つのレコード）。非レプリケートされたテーブルのミューテーションの場合、配列は空です。

- `block_numbers.number` ([Array](/docs/ja/sql-reference/data-types/array.md)([Int64](/docs/ja/sql-reference/data-types/int-uint.md))) — レプリケートされたテーブルのミューテーションの場合、配列は各パーティションに1つのレコードを含み、そのミューテーションによって取得されたブロック番号を示しています。この番号より小さい番号を持つブロックを含むパーツのみがパーティションでミューテーションされます。

    非レプリケートされたテーブルでは、すべてのパーティションでブロック番号が単一のシーケンスを形成します。したがって、非レプリケートされたテーブルのミューテーションの場合、カラムはミューテーションによって取得された単一ブロック番号を持つ1つのレコードを含みます。

- `parts_to_do_names` ([Array](/docs/ja/sql-reference/data-types/array.md)([String](/docs/ja/sql-reference/data-types/string.md))) — ミューテーションを完了するためにミューテートする必要のあるデータパーツの名前の配列。

- `parts_to_do` ([Int64](/docs/ja/sql-reference/data-types/int-uint.md)) — ミューテーションを完了するためにミューテートが必要なデータパーツの数。

- `is_done` ([UInt8](/docs/ja/sql-reference/data-types/int-uint.md)) — ミューテーションが完了しているかどうかのフラグ。可能な値：
    - ミューテーションが完了している場合は`1`、
    - ミューテーションがまだ進行中の場合は`0`。

:::note
`parts_to_do = 0`であっても、レプリケートされたテーブルのミューテーションがまだ完了していない可能性があります。それは、新しいデータパーツを作成し、ミューテートが必要な長時間実行されている`INSERT`クエリのためです。
:::

いくつかのデータパーツのミューテートに問題があった場合、次のカラムに追加情報が含まれます：

- `latest_failed_part` ([String](/docs/ja/sql-reference/data-types/string.md)) — ミューテートできなかった最新のパーツの名前。

- `latest_fail_time` ([DateTime](/docs/ja/sql-reference/data-types/datetime.md)) — 最新のパーツミューテーションの失敗日時。

- `latest_fail_reason` ([String](/docs/ja/sql-reference/data-types/string.md)) — 最新のパーツミューテーション失敗を引き起こした例外メッセージ。

## ミューテーションの監視

`system.mutations`テーブルで進捗を追跡するには、次のようなクエリを使用します - これには`system.*`テーブルの読み取り権限が必要です：

``` sql
SELECT * FROM clusterAllReplicas('cluster_name', 'db', system.mutations)
WHERE is_done=0 AND table='tmp';
```

:::tip
`table='tmp'`の`tmp`を、ミューテーションを確認しているテーブルの名前に置き換えてください。
:::

**関連情報**

- [ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#mutations)
- [MergeTree](/docs/ja/engines/table-engines/mergetree-family/mergetree.md)テーブルエンジン
- [ReplicatedMergeTree](/docs/ja/engines/table-engines/mergetree-family/replication.md)ファミリー
