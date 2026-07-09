---
description: '論理削除により、データベースからデータを削除する処理が簡単になります。'
keywords: ['delete']
sidebar_label: 'DELETE'
sidebar_position: 36
slug: /sql-reference/statements/delete
title: '論理削除 `DELETE` ステートメント'
doc_type: 'reference'
---

論理削除 `DELETE` ステートメントは、式 `expr` に一致するテーブル `[db.]table` の行を削除します。使用できるのは *MergeTree テーブルエンジンファミリーのみです。

```sql
DELETE FROM [db.]table [ON CLUSTER cluster] [IN PARTITION partition_expr] WHERE expr;
```

これは、重量級の処理である [ALTER TABLE ... DELETE](/ja/sql-reference/statements/alter/delete) コマンドと区別するため、&quot;論理削除 `DELETE`&quot;と呼ばれます。

<div id="examples">
  ## 例
</div>

```sql
-- Deletes all rows from the `hits` table where the `Title` column contains the text `hello`
DELETE FROM hits WHERE Title LIKE '%hello%';
```

<div id="lightweight-delete-does-not-delete-data-immediately">
  ## 論理削除ではデータはすぐには削除されません
</div>

論理削除は、行を削除済みとしてマークしますが、直ちに物理削除は行わない [ミューテーション](/ja/sql-reference/statements/alter#mutations) として実装されています。

デフォルトでは、`DELETE`ステートメントは行への削除済みマークが完了するまで待機してから終了します。データ量が多い場合、この処理には時間がかかることがあります。代わりに、設定 [`lightweight_deletes_sync`](/ja/operations/settings/settings#lightweight_deletes_sync) を使用して、バックグラウンドで非同期に実行することもできます。これを無効にすると、`DELETE`ステートメントはすぐに終了しますが、バックグラウンドのミューテーションが完了するまでは、データがクエリから見える可能性があります。

このミューテーションは、削除済みとしてマークされた行を物理的には削除しません。実際に削除されるのは、次回のマージ時のみです。そのため、一定期間、データは実際にはストレージから削除されず、削除済みとしてマークされるだけの状態になる可能性があります。

データが予測可能な時間内にストレージから削除されることを保証する必要がある場合は、テーブル設定 [`min_age_to_force_merge_seconds`](/ja/operations/settings/merge-tree-settings#min_age_to_force_merge_seconds) の使用を検討してください。あるいは、[ALTER TABLE ... DELETE](/ja/sql-reference/statements/alter/delete) コマンドを使用することもできます。`ALTER TABLE ... DELETE` を使用したデータ削除では、影響を受けるすべてのパーツを再作成するため、大量のリソースを消費する可能性がある点に注意してください。

<div id="deleting-large-amounts-of-data">
  ## 大量のデータの削除
</div>

大量の削除は ClickHouse のパフォーマンスに悪影響を及ぼす可能性があります。テーブル内のすべての行を削除したい場合は、[`TRUNCATE TABLE`](/ja/sql-reference/statements/truncate) コマンドの使用を検討してください。

削除が頻繁に発生することが見込まれる場合は、[カスタムのパーティションキー](/ja/engines/table-engines/mergetree-family/custom-partitioning-key) の使用を検討してください。そうすることで、[`ALTER TABLE ... DROP PARTITION`](/ja/sql-reference/statements/alter/partition#drop-partitionpart) コマンドを使って、そのパーティションに属するすべての行をすばやく削除できます。

<div id="limitations-of-lightweight-delete">
  ## 論理削除の制限事項
</div>

<div id="lightweight-deletes-with-projections">
  ### プロジェクションがある場合の論理削除 `DELETE`
</div>

デフォルトでは、プロジェクションを持つテーブルでは `DELETE` は使用できません。これは、プロジェクション内の行も `DELETE` 操作の影響を受ける可能性があるためです。ただし、この動作は [MergeTree 設定](/ja/operations/settings/merge-tree-settings) `lightweight_mutation_projection_mode` で変更できます。

<div id="performance-considerations-when-using-lightweight-delete">
  ## 論理削除 `DELETE` 使用時のパフォーマンスに関する考慮事項
</div>

**論理削除 `DELETE` ステートメントで大量のデータを削除すると、`SELECT` クエリのパフォーマンスに悪影響を及ぼす可能性があります。**

次の要因も、論理削除 `DELETE` のパフォーマンスに悪影響を与える可能性があります。

* `DELETE` クエリ内の `WHERE` 条件が複雑で負荷が高い。
* ミューテーション キューが多数の他の ミューテーション で埋まっている場合、テーブル上のすべての ミューテーション は順次実行されるため、パフォーマンス上の問題につながる可能性があります。
* 対象のテーブルに非常に多くのデータパーツがある。
* compact パーツに大量のデータがある。Compact パーツでは、すべてのカラムが 1 つのファイルに格納されます。

<div id="delete-permissions">
  ## DELETE 権限
</div>

`DELETE` の実行には `ALTER DELETE` 権限が必要です。特定のユーザーに特定のテーブルで `DELETE` ステートメントを許可するには、次のコマンドを実行します。

```sql
GRANT ALTER DELETE ON db.table to username;
```

<div id="how-lightweight-deletes-work-internally-in-clickhouse">
  ## ClickHouse における論理削除の内部的な仕組み
</div>

1. **影響を受ける行に「マスク」が適用される**

   `DELETE FROM table ...` クエリが実行されると、ClickHouse は各行を「存在する」または「削除済み」としてマークするマスクを保存します。これらの「削除済み」行は、以降のクエリ結果から除外されます。ただし、行が実際に削除されるのは、その後のマージ時です。このマスクの書き込みは、`ALTER TABLE ... DELETE` クエリで行われる処理よりもはるかに軽量です。

   このマスクは、表示対象のすべての行に `True` を、削除された行に `False` を格納する隠しシステムカラム `_row_exists` として実装されています。このカラムは、パーツ内の一部の行が削除された場合にのみそのパーツに存在します。パーツ内のすべての値が `True` の場合、このカラムは存在しません。

2. **`SELECT` クエリはマスクを含む形に変換される**

   マスク対象のカラムがクエリで使用される場合、`SELECT ... FROM table WHERE condition` クエリには内部的に `_row_exists` に対する predicate が追加され、次のように変換されます:

   ```sql
   SELECT ... FROM table PREWHERE _row_exists WHERE condition
   ```

   実行時には、返してはいけない行を判定するために `_row_exists` カラムが読み取られます。削除済みの行が多い場合、ClickHouse は残りのカラムを読み取る際に、どの granules を完全にスキップできるかを判定できます。

3. **`DELETE` クエリは `ALTER TABLE ... UPDATE` クエリに変換される**

   `DELETE FROM table WHERE condition` は、`ALTER TABLE table UPDATE _row_exists = 0 WHERE condition` ミューテーション に変換されます。

   内部的には、この ミューテーション は 2 つの手順で実行されます:

   1. 各パーツが影響を受けるかどうかを判定するために、パーツごとに `SELECT count() FROM table WHERE condition` コマンドが実行されます。

   2. 上記のコマンドに基づいて、影響を受けるパーツには ミューテーション が適用され、影響を受けないパーツにはハードリンクが作成されます。wide パーツの場合は、各行の `_row_exists` カラムが更新され、その他のすべてのカラムファイルにはハードリンクが作成されます。compact パーツの場合は、すべてのカラムが 1 つのファイルにまとめて格納されているため、すべてのカラムが再書き込みされます。

   上記の手順から、マスキング手法を用いた論理削除は、影響を受けるパーツについてすべてのカラムファイルを書き換える必要がないため、従来の `ALTER TABLE ... DELETE` よりも高い性能を実現できることがわかります。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouse での更新および削除の処理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)