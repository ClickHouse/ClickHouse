---
slug: /ja/sql-reference/statements/delete
sidebar_position: 36
sidebar_label: DELETE
description: 論理削除は、データベースからデータを削除するプロセスを簡素化します。
keywords: [delete]
title: 論理 DELETE 文
---

論理削除は、*MergeTree テーブルエンジンファミリ*にのみ利用可能で、式 `expr` に一致する `[db.]table` から行を削除します。

``` sql
DELETE FROM [db.]table [ON CLUSTER cluster] [IN PARTITION partition_expr] WHERE expr;
```

これは、[ALTER TABLE ... DELETE](/ja/sql-reference/statements/alter/delete) コマンドとの対比で「論理 `削除`」と呼ばれていますが、ALTER コマンドは重いプロセスです。

## 例

```sql
-- `Title` カラムが `hello` を含む `hits` テーブルからすべての行を削除
DELETE FROM hits WHERE Title LIKE '%hello%';
```

## 論理削除はデータを即座に削除しない

論理 `DELETE` は、行を削除済みとしてマークする[ミューテーション](/ja/sql-reference/statements/alter#mutations)として実装されており、物理的に即座に削除されません。

デフォルトでは、`DELETE` 文は、行が削除済みとしてマークされるまで待機してから戻ります。データ量が大きい場合、これには長時間かかることがあります。代わりに、設定 [`lightweight_deletes_sync`](/ja/operations/settings/settings#lightweight_deletes_sync) を使用してバックグラウンドで非同期に実行できます。無効にすると、`DELETE` 文は即座に戻りますが、バックグラウンドミューテーションが終了するまでクエリに対してデータが表示され続ける可能性があります。

このミューテーションは、削除済みとマークされた行を物理的に削除しません。これは次のマージの際にのみ発生します。その結果、データがストレージから実際に削除されておらず、削除済みとマークされているだけの不特定の期間が存在する可能性があります。

予測可能な期間内にストレージからデータが削除されることを保証する必要がある場合、テーブル設定 [`min_age_to_force_merge_seconds`](https://clickhouse.com/docs/ja/operations/settings/merge-tree-settings#min_age_to_force_merge_seconds) の使用を検討してください。または、[ALTER TABLE ... DELETE](/ja/sql-reference/statements/alter/delete) コマンドを使用することもできます。`ALTER TABLE ... DELETE` を使用したデータの削除は、すべての影響を受けるパーツを再作成するため、多くのリソースを消費する可能性があることに注意してください。

## 大量のデータを削除する

大量の削除は ClickHouse のパフォーマンスに悪影響を与える可能性があります。テーブルのすべての行を削除しようとしている場合は、[`TRUNCATE TABLE`](/ja/sql-reference/statements/truncate) コマンドの使用を検討してください。

頻繁な削除が予想される場合は、[カスタムパーティションキー](/ja/engines/table-engines/mergetree-family/custom-partitioning-key) の使用を検討してください。これによって、[`ALTER TABLE ... DROP PARTITION`](/ja/sql-reference/statements/alter/partition#drop-partitionpart) コマンドを使用して、そのパーティションに関連するすべての行をすばやく削除できます。

## 論理削除の制限

### プロジェクションを持つ論理 `DELETE`

デフォルトでは、`DELETE` はプロジェクションを持つテーブルには対応していません。これは、プロジェクション内の行が `DELETE` 操作によって影響される可能性があるためです。しかし、この動作を変更する [MergeTree 設定](https://clickhouse.com/docs/ja/operations/settings/merge-tree-settings) `lightweight_mutation_projection_mode` があります。

## 論理削除を使用する際のパフォーマンス考慮事項

**論理`DELETE`文を使用して大量のデータを削除すると、SELECT クエリのパフォーマンスに悪影響を与える可能性があります。**

次の点も論理 `DELETE` のパフォーマンスにマイナスの影響を与える可能性があります：

- `DELETE` クエリで重い `WHERE` 条件がある。
- ミューテーションキューが他の多くのミューテーションで満たされている場合、テーブルのすべてのミューテーションは順に実行されるため、パフォーマンスの問題につながる可能性があります。
- 影響を受けたテーブルに非常に多くのデータパーツがある。
- コンパクトパーツに大量のデータがある場合。コンパクトパーツでは、すべてのカラムが1つのファイルに保存されます。

## 削除の権限

`DELETE` には `ALTER DELETE` 権限が必要です。指定したユーザーに対して特定のテーブルで `DELETE` 文を有効にするには、次のコマンドを実行します：

```sql
GRANT ALTER DELETE ON db.table to username;
```

## ClickHouse 内部での論理削除の動作

1. **影響を受けた行に「マスク」が適用される**

   `DELETE FROM table ...` クエリが実行されると、ClickHouse は各行に “existing” と “deleted” のいずれかでマークされたマスクを保存します。これらの “deleted” 行は、後続のクエリでは省略されます。しかし、行は実際には後続のマージによってのみ削除されます。このマスクの書き込みは、`ALTER TABLE ... DELETE` クエリが行うものよりも軽量です。

   このマスクは、隠された `_row_exists` システムカラムとして実装され、すべての表示行に対して `True` を、削除された行に対しては `False` を格納します。このカラムは、パーツ内で一部の行が削除された場合にのみ存在します。このカラムは、すべての値が `True` である場合には存在しません。

2. **`SELECT` クエリはマスクを組み込むように変換される**

   マスクされたカラムがクエリで使用されると、`SELECT ... FROM table WHERE condition` クエリは内部的に `_row_exists` に対する述語で拡張され、次のように変換されます：
   ```sql
   SELECT ... FROM table PREWHERE _row_exists WHERE condition
   ```
   実行時に、カラム `_row_exists` が読み取られ、どの行を返さないかが決定されます。大量の削除済行がある場合、ClickHouse は他のカラムを読み取る際にスキップできるグラニュールを特定できます。

3. **`DELETE` クエリは `ALTER TABLE ... UPDATE` クエリに変換される**

   `DELETE FROM table WHERE condition` は、`ALTER TABLE table UPDATE _row_exists = 0 WHERE condition` ミューテーションに変換されます。

   内部的に、このミューテーションは2段階で実行されます：

   1. 各個別パーツに対して、パーツが影響を受けているかどうかを判断するために、`SELECT count() FROM table WHERE condition` コマンドが実行されます。

   2. 上記のコマンドに基づいて、影響を受けたパーツはミューテートされ、影響を受けていないパーツにはハードリンクが作成されます。ワイドパーツの場合、各行の `_row_exists` カラムが更新され、他のすべてのカラムのファイルはハードリンクされます。コンパクトパーツの場合、すべてのカラムが一つのファイルに一緒に保存されているため、すべてのカラムが再書き込みされます。

   上記の手順から、マスキング技術を使用した論理 `DELETE` は、影響を受けたパーツのすべてのカラムファイルを再書き込みしないため、従来の `ALTER TABLE ... DELETE` に比べてパフォーマンスを改善していることがわかります。

## 関連コンテンツ

- ブログ: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)

