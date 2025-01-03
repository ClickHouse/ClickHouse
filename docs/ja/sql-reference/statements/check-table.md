---
slug: /ja/sql-reference/statements/check-table
sidebar_position: 41
sidebar_label: CHECK TABLE
title: "CHECK TABLE ステートメント"
---

ClickHouse の `CHECK TABLE` クエリは、特定のテーブルまたはそのパーティションに対して検証を行うために使用されます。これにより、チェックサムやその他の内部データ構造を確認してデータの整合性を保証します。

特に、実際のファイルサイズとサーバーに保存されている期待値を比較します。ファイルサイズが保存されている値と一致しない場合、それはデータが破損していることを意味します。これは、クエリ実行中にシステムがクラッシュするなどの原因で発生することがあります。

:::note
`CHECK TABLE` クエリはテーブル内のすべてのデータを読み込む可能性があり、リソースを占有するため、非常にリソースを消費することがあります。
このクエリを実行する前にパフォーマンスおよびリソース使用率への影響を考慮してください。
:::

## 構文

クエリの基本的な構文は以下の通りです：

```sql
CHECK TABLE table_name [PARTITION partition_expression | PART part_name] [FORMAT format] [SETTINGS check_query_single_value_result = (0|1) [, other_settings]]
```

- `table_name`: チェックするテーブルの名前を指定します。
- `partition_expression`: (オプション) テーブルの特定のパーティションをチェックしたい場合、この式を使用してパーティションを指定します。
- `part_name`: (オプション) テーブル内の特定のパートをチェックしたい場合、文字列リテラルを追加してパート名を指定します。
- `FORMAT format`: (オプション) 結果の出力フォーマットを指定できます。
- `SETTINGS`: (オプション) 追加の設定を許可します。
  - **`check_query_single_value_result`**: (オプション) 詳細な結果 (`0`) または要約された結果 (`1`) を切り替えるための設定。
  - 他の設定も適用できます。結果の順序を予測可能にする必要がない場合、`max_threads` を1以上に設定してクエリを高速化できます。

クエリの応答は、`check_query_single_value_result` 設定の値に依存します。
`check_query_single_value_result = 1` の場合、`result` カラムに1行のみが返されます。この行の値は、整合性チェックに合格した場合は `1` であり、データが破損している場合は `0` です。

`check_query_single_value_result = 0` の場合、クエリは以下のカラムを返します：
- `part_path`: データパートまたはファイル名へのパスを示します。
- `is_passed`: このパートのチェックが成功した場合は1、そうでない場合は0を返します。
- `message`: エラーや成功メッセージなど、チェックに関連する追加メッセージ。

`CHECK TABLE` クエリは以下のテーブルエンジンをサポートします：

- [Log](../../engines/table-engines/log-family/log.md)
- [TinyLog](../../engines/table-engines/log-family/tinylog.md)
- [StripeLog](../../engines/table-engines/log-family/stripelog.md)
- [MergeTree ファミリー](../../engines/table-engines/mergetree-family/mergetree.md)

別のテーブルエンジンを持つテーブルで実行すると、`NOT_IMPLEMENTED` 例外が発生します。

`*Log` ファミリーのエンジンは、障害時の自動データ回復を提供しません。`CHECK TABLE` クエリを使用してデータ損失をタイムリーに追跡してください。

## 例

デフォルトでは、`CHECK TABLE` クエリはテーブルの一般的なチェック状況を表示します：

```sql
CHECK TABLE test_table;
```

```text
┌─result─┐
│      1 │
└────────┘
```

個々のデータパートごとのチェック状況を確認したい場合、`check_query_single_value_result` 設定を使用できます。

また、テーブルの特定のパーティションをチェックするには、`PARTITION` キーワードを使用できます。

```sql
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

出力：

```text
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
│ 201003_3_3_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

同様に、特定のパートをチェックするには、`PART` キーワードを使用します。

```sql
CHECK TABLE t0 PART '201003_7_7_0'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

出力：

```text
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

パートが存在しない場合、クエリはエラーを返します：

```sql
CHECK TABLE t0 PART '201003_111_222_0'
```

```text
DB::Exception: No such data part '201003_111_222_0' to check in table 'default.t0'. (NO_SUCH_DATA_PART)
```

### 'Corrupted' 結果の受信

:::warning
免責事項: ここで説明する手順、特にデータディレクトリからの直接的なファイルの操作や削除は、実験または開発環境のみを対象としています。生産サーバーでこれを行うと、データ損失やその他の予期せぬ結果を招く可能性があるため、絶対に行わないでください。
:::

既存のチェックサムファイルを削除：

```bash
rm /var/lib/clickhouse-server/data/default/t0/201003_3_3_0/checksums.txt
```

```sql
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

出力：

```text
┌─part_path────┬─is_passed─┬─message──────────────────────────────────┐
│ 201003_7_7_0 │         1 │                                          │
│ 201003_3_3_0 │         1 │ Checksums recounted and written to disk. │
└──────────────┴───────────┴──────────────────────────────────────────┘
```

checksums.txt ファイルが存在しない場合、それを復元できます。これは、特定のパーティションに対する `CHECK TABLE` コマンドの実行中に再計算され、再書き込みされ、ステータスは 'is_passed = 1' として報告され続けます。

`(Replicated)MergeTree` テーブルをすべて一度にチェックするには、`CHECK ALL TABLES` クエリを使用します。

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

## データが破損している場合

テーブルが破損している場合、破損していないデータを別のテーブルにコピーできます。これを行うには：

1.  損傷したテーブルと同じ構造の新しいテーブルを作成します。このために、クエリ `CREATE TABLE <new_table_name> AS <damaged_table_name>` を実行します。
2.  次のクエリを単一スレッドで処理するために `max_threads` の値を1に設定します。これを行うために、クエリ `SET max_threads = 1` を実行します。
3.  クエリ `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>` を実行します。このリクエストは損傷したテーブルから別のテーブルに破損していないデータをコピーします。破損部分の前のデータのみがコピーされます。
4.  `clickhouse-client` を再起動して `max_threads` の値をリセットします。
