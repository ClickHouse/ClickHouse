---
description: 'CHECK TABLE のドキュメント'
sidebar_label: 'CHECK TABLE'
sidebar_position: 41
slug: /sql-reference/statements/check-table
title: 'CHECK TABLE ステートメント'
doc_type: 'reference'
---

ClickHouse の `CHECK TABLE` クエリは、特定のテーブルまたはそのパーティションに対して整合性チェックを実行するために使用されます。checksums やその他の内部データ構造を検証することで、データの整合性を確認します。

特に、実際のファイルサイズを、server に保存されている想定値と比較します。ファイルサイズが保存済みの値と一致しない場合、データが破損していることを示します。これは、たとえばクエリ実行中のシステムクラッシュによって発生することがあります。

:::warning
`CHECK TABLE` クエリは、テーブル内のすべてのデータを読み取り、一部のリソースを占有する可能性があるため、リソース消費が大きくなる場合があります。
このクエリを実行する前に、パフォーマンスとリソース使用状況への影響を十分に考慮してください。
このクエリを実行してもシステムのパフォーマンスは向上しないため、内容を十分に理解していない場合は実行しないでください。
:::

<div id="syntax">
  ## 構文
</div>

クエリの基本構文は次のとおりです。

```sql
CHECK TABLE table_name [PARTITION partition_expression | PART part_name] [FORMAT format] [SETTINGS check_query_single_value_result = (0|1) [, other_settings]]
```

* `table_name`: チェックするテーブルの名前を指定します。
* `partition_expression`:  (省略可) テーブルの特定のパーティションをチェックする場合は、この式でパーティションを指定できます。
* `part_name`:  (省略可) テーブル内の特定のパートをチェックする場合は、パート名を指定する文字列リテラルを追加できます。
* `FORMAT format`:  (省略可) 結果の出力フォーマットを指定できます。
* `SETTINGS`:  (省略可) 追加の設定を指定できます。
  *  (省略可) : [check&#95;query&#95;single&#95;value&#95;result](../../operations/settings/settings#check_query_single_value_result): この設定は、出力を詳細 (`0`) にするか要約 (`1`) にするかを制御します。
  * 他の設定も適用できます。結果の順序が決定論的である必要がない場合は、`max_threads` を 1 より大きい値に設定してクエリを高速化できます。

クエリの応答は、`check_query_single_value_result` 設定の値によって異なります。
`check_query_single_value_result = 1` の場合は、1 行のみの `result` カラムだけが返されます。この行の値は、整合性チェックに合格した場合は `1`、データが破損している場合は `0` になります。

`check_query_single_value_result = 0` の場合、クエリは次のカラムを返します。

* `part_path`: データパートへのパス、またはファイル名を示します。
  * `is_passed`: このパートのチェックが成功した場合は 1、失敗した場合は 0 を返します。
  * `message`: エラーや成功メッセージなど、チェックに関連する追加メッセージです。

`CHECK TABLE` クエリは、次のテーブルエンジンをサポートしています。

* [Log](../../engines/table-engines/log-family/log.md)
* [TinyLog](../../engines/table-engines/log-family/tinylog.md)
* [StripeLog](../../engines/table-engines/log-family/stripelog.md)
* [MergeTree family](../../engines/table-engines/mergetree-family/mergetree.md)

それ以外のテーブルエンジンを使用するテーブルに対して実行すると、`NOT_IMPLEMENTED` 例外が発生します。

`*Log` family のエンジンは、障害発生時の自動データ復旧を提供しません。`CHECK TABLE` クエリを使用して、データ損失を速やかに検知してください。

<div id="examples">
  ## 例
</div>

デフォルトでは、`CHECK TABLE` クエリにより、テーブルチェックの全体的なステータスが表示されます。

```sql title="Query"
CHECK TABLE test_table;
```

```text title="Response"
┌─result─┐
│      1 │
└────────┘
```

各データパートごとのチェック結果を確認したい場合は、`check_query_single_value_result` 設定を使用できます。

また、テーブルの特定のパーティションを確認するには、`PARTITION` キーワードを使用できます。

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
│ 201003_3_3_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

同様に、`PART` キーワードを使用すると、テーブルの特定のパートを確認できます。

```sql title="Query"
CHECK TABLE t0 PART '201003_7_7_0'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

パートが存在しない場合、クエリはエラーを返すことに注意してください。

```sql title="Query"
CHECK TABLE t0 PART '201003_111_222_0'
```

```text title="Response"
DB::Exception: No such data part '201003_111_222_0' to check in table 'default.t0'. (NO_SUCH_DATA_PART)
```

<div id="receiving-a-corrupted-result">
  ### &#39;Corrupted&#39; という結果が返される場合
</div>

:::warning
免責事項: ここで説明する手順には、データディレクトリ内のファイルを手動で直接操作または削除する作業が含まれます。これは実験環境または開発環境でのみ実施してください。データ損失やその他の予期しない影響を招くおそれがあるため、本番サーバーでは**絶対に**実行しないでください。
:::

既存のチェックサムファイルを削除します:

```bash
rm /var/lib/clickhouse-server/data/default/t0/201003_3_3_0/checksums.txt
```

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message──────────────────────────────────┐
│ 201003_7_7_0 │         1 │                                          │
│ 201003_3_3_0 │         1 │ Checksums recounted and written to disk. │
└──────────────┴───────────┴──────────────────────────────────────────┘
```

`checksums.txt` ファイルがない場合は、復元できます。特定のパーティションに対して `CHECK TABLE` コマンドを実行すると、その際に再計算されて書き直され、ステータスは引き続き &#39;is&#95;passed = 1&#39; と報告されます。

`CHECK ALL TABLES` クエリを使用すると、既存のすべての `(Replicated)MergeTree` テーブルをまとめてチェックできます。

```sql
CHECK ALL TABLES
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text
┌─database─┬─table────┬─part_path───┬─is_passed─┬─message─┐
│ default  │ t2       │ all_1_95_3  │         1 │         │
│ db1      │ table_01 │ all_39_39_0 │         1 │         │
│ default  │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ table_01 │ all_1_6_1   │         1 │         │
│ default  │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ table_01 │ all_7_38_2  │         1 │         │
│ db1      │ t1       │ all_7_38_2  │         1 │         │
│ default  │ t1       │ all_7_38_2  │         1 │         │
└──────────┴──────────┴─────────────┴───────────┴─────────┘
```

<div id="if-the-data-is-corrupted">
  ## データが破損している場合
</div>

テーブルが破損している場合は、破損していないデータを別のテーブルにコピーできます。手順は次のとおりです。

1. 破損したテーブルと同じ構造の新しいテーブルを作成します。これには、クエリ `CREATE TABLE <new_table_name> AS <damaged_table_name>` を実行します。
2. 次のクエリを単一スレッドで処理するため、`max_threads` の値を 1 に設定します。これには、クエリ `SET max_threads = 1` を実行します。
3. クエリ `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>` を実行します。このリクエストにより、破損したテーブルから別のテーブルへ、破損していないデータがコピーされます。コピーされるのは、破損した箇所より前のデータのみです。
4. `max_threads` の値をリセットするには、`clickhouse-client` を再起動します。