---
description: '論理更新を使うと、パッチパートを用いたデータベース内のデータ更新を簡素化できます。'
keywords: ['update']
sidebar_label: 'UPDATE'
sidebar_position: 39
slug: /sql-reference/statements/update
title: '論理更新の UPDATE ステートメント'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

:::note
論理更新は現在ベータ版です。
問題が発生した場合は、[ClickHouseリポジトリ](https://github.com/clickhouse/clickhouse/issues)で issue を登録してください。
:::

論理更新 `UPDATE` ステートメントは、式 `filter_expr` に一致するテーブル `[db.]table` の行を更新します。
これは、データパーツ内のカラム全体を書き換える高コストな処理である [`ALTER TABLE ... UPDATE`](/ja/sql-reference/statements/alter/update) クエリと区別するため、&quot;論理更新&quot; と呼ばれます。
これは [`MergeTree`](/ja/engines/table-engines/mergetree-family/mergetree) テーブルエンジンファミリーでのみ使用できます。

```sql
UPDATE [db.]table [ON CLUSTER cluster] SET column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr;
```

`filter_expr` は `UInt8` 型である必要があります。このクエリは、`filter_expr` がゼロ以外の値を取る行について、指定したカラムの値を、それぞれ対応する式の値に更新します。
値は `CAST` 演算子を使用してカラムの型にキャストされます。プライマリキーまたはパーティションキーの計算に使用されるカラムの更新はサポートされていません。

<div id="examples">
  ## 例
</div>

```sql
UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today();

UPDATE wikistat SET hits = hits + 1, time = now() WHERE path = 'ClickHouse';
```

<div id="lightweight-update-does-not-update-data-immediately">
  ## 論理更新ではデータはすぐには更新されません
</div>

論理更新 `UPDATE` は、更新されたカラムと行のみを含む特別な種類のデータパートである **パッチパート** を使って実装されています。
論理更新 `UPDATE` では パッチパート が作成されますが、ストレージ内の元のデータが直ちに物理的に変更されるわけではありません。
更新の処理は `INSERT ... SELECT ...` クエリに似ていますが、`UPDATE` クエリは パッチパート の作成が完了するまで待ってから結果を返します。

更新後の値は次のようになります。

* パッチの適用により、`SELECT` クエリから**即座に参照可能**になります
* **物理的に実体化**されるのは、その後のマージや mutation の実行時のみです
* すべての active parts でパッチが実体化されると、**自動的にクリーンアップ**されます

<div id="lightweight-update-requirements">
  ## 論理更新の要件
</div>

論理更新は、[`MergeTree`](/ja/engines/table-engines/mergetree-family/mergetree)、[`ReplacingMergeTree`](/ja/engines/table-engines/mergetree-family/replacingmergetree)、[`CollapsingMergeTree`](/ja/engines/table-engines/mergetree-family/collapsingmergetree)、[`VersionedCollapsingMergeTree`](https://clickhouse.com/docs/engines/table-engines/mergetree-family/versionedcollapsingmergetree) エンジン、およびそれらの [`Replicated`](/ja/engines/table-engines/mergetree-family/replication.md) 版と [`Shared`](/ja/cloud/reference/shared-merge-tree) 版でサポートされています。

論理更新を使用するには、テーブル設定 [`enable_block_number_column`](/ja/operations/settings/merge-tree-settings#enable_block_number_column) と [`enable_block_offset_column`](/ja/operations/settings/merge-tree-settings#enable_block_offset_column) により、`_block_number` および `_block_offset` カラムのマテリアライズを有効にする必要があります。

<div id="lightweight-delete">
  ## 論理削除
</div>

[論理削除 `DELETE`](/ja/sql-reference/statements/delete) クエリは、`ALTER UPDATE` ミューテーションの代わりに、論理更新 `UPDATE` として実行できます。論理削除 `DELETE` の実装は、設定 [`lightweight_delete_mode`](/ja/operations/settings/settings#lightweight_delete_mode) によって制御されます。

<div id="performance-considerations">
  ## パフォーマンスに関する考慮事項
</div>

**論理更新の利点:**

* 更新のレイテンシは、`INSERT ... SELECT ...` クエリのレイテンシと同程度です
* 書き込まれるのは更新されたカラムと値のみで、データパーツ内のカラム全体は書き込まれません
* 現在実行中のマージやミューテーションの完了を待つ必要がないため、更新のレイテンシは予測しやすくなります
* 論理更新は並列実行が可能です

**想定されるパフォーマンスへの影響:**

* パッチの適用が必要な `SELECT` クエリにはオーバーヘッドが追加されます
* パッチの適用が必要なデータパーツ内のカラムでは、[スキップ索引](/ja/engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes) は使用されません。テーブルにパッチパートがある場合は、パッチの適用が不要なデータパーツも含めて、[プロジェクション](/ja/engines/table-engines/mergetree-family/mergetree.md/#projections) は使用されません。
* 頻繁すぎる小規模な更新は、&quot;パーツが多すぎる&quot; エラーの原因になる可能性があります。複数の更新を 1 つのクエリにまとめることを推奨します。たとえば、更新対象の ID を `WHERE` 句内の 1 つの `IN` 句にまとめます
* 論理更新は、少量の行 (テーブルの約 10% まで) を更新するよう設計されています。より多くの行を更新する必要がある場合は、[`ALTER TABLE ... UPDATE`](/ja/sql-reference/statements/alter/update) ミューテーションの使用を推奨します

<div id="concurrent-operations">
  ## 同時実行操作
</div>

論理更新は、通常のミューテーションとは異なり、現在実行中のマージやミューテーションが完了するのを待ちません。
同時に実行される論理更新の整合性は、設定 [`update_sequential_consistency`](/ja/operations/settings/settings#update_sequential_consistency) と [`update_parallel_mode`](/ja/operations/settings/settings#update_parallel_mode) によって制御されます。

<div id="update-permissions">
  ## UPDATE の権限
</div>

`UPDATE` を実行するには、`ALTER UPDATE` 権限が必要です。特定のユーザーに特定のテーブルで `UPDATE` ステートメントを実行する権限を付与するには、次を実行します。

```sql
GRANT ALTER UPDATE ON db.table TO username;
```

<div id="details-of-the-implementation">
  ## 実装の詳細
</div>

パッチパートは通常のパーツと同じですが、更新されたカラムと、いくつかのシステムカラムだけを含みます。

* `_part` - 元のパーツの名前
* `_part_offset` - 元のパーツ内の行番号
* `_block_number` - 元のパーツ内でのその行のブロック番号
* `_block_offset` - 元のパーツ内でのその行のブロックオフセット
* `_data_version` - 更新データのデータバージョン (`UPDATE` クエリに割り当てられるブロック番号)

平均すると、パッチパートでは更新された各行ごとに約 40 バイトのオーバーヘッド (非圧縮データ) が発生します。
システムカラムは、更新対象となる元のパーツ内の行を特定するのに役立ちます。
システムカラムは、パッチパートを適用する必要がある場合に読み取り用として追加される、元のパーツの[仮想カラム](/ja/engines/table-engines/mergetree-family/mergetree.md/#virtual-columns)に対応しています。
パッチパートは `_part` と `_part_offset` でソートされます。

パッチパートは、元のパーツとは異なるパーティションに属します。
パッチパートのパーティション ID は `patch-<hash of column names in patch part>-<original_partition_id>` です。
そのため、含まれるカラムが異なるパッチパートは、別々のパーティションに格納されます。
たとえば、3 つの更新 `SET x = 1 WHERE <cond>`、`SET y = 1 WHERE <cond>`、`SET x = 1, y = 1 WHERE <cond>` を行うと、3 つの異なるパーティションに 3 つのパッチパートが作成されます。

パッチパート同士はマージできるため、`SELECT` クエリで適用されるパッチの数を減らし、オーバーヘッドを抑えられます。パッチパートのマージでは、`_data_version` をバージョンカラムとして [replacing](/ja/engines/table-engines/mergetree-family/replacingmergetree) マージアルゴリズムを使用します。
そのため、パッチパートには常に、そのパーツ内の更新済み各行について最新バージョンが格納されます。

論理更新は、現在実行中のマージやミューテーションの完了を待たず、常にデータパーツの current snapshot を使って更新を実行し、パッチパートを生成します。
このため、パッチパートの適用には 2 つのケースがあります。

たとえば、パーツ `A` を読み取る際に、パッチパート `X` を適用する必要があるとします。

* `X` にパーツ `A` 自体が含まれている場合。これは、`UPDATE` 実行時に `A` がマージに参加していなかった場合に発生します。
* `X` に、パーツ `A` に取り込まれた `B` と `C` が含まれている場合。これは、`UPDATE` 実行時にマージ (`B`, `C`) -&gt; `A` が進行中だった場合に発生します。

この 2 つのケースに対して、パッチパートの適用方法もそれぞれ 2 通りあります。

* ソート済みカラム `_part`、`_part_offset` を使ったマージ
* `_block_number`、`_block_offset` カラムを使った join

join モードは merge モードより低速で、より多くのメモリを必要としますが、使われる頻度はそれほど高くありません。

<div id="related-content">
  ## 関連情報
</div>

* [`ALTER UPDATE`](/ja/sql-reference/statements/alter/update) - 高負荷な `UPDATE` 操作
* [論理削除 `DELETE`](/ja/sql-reference/statements/delete) - 論理削除 `DELETE` 操作
* [`APPLY PATCHES`](/ja/sql-reference/statements/alter/apply-patches) - データパーツに対するパッチの物理的な実体化を強制します (mutation 操作)