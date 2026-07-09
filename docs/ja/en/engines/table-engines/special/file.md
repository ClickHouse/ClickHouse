---
description: 'File テーブルエンジンは、サポートされているファイルフォーマット（`TabSeparated`、`Native` など）のいずれかで、データをファイルに保存します。'
sidebar_label: 'File'
sidebar_position: 40
slug: /engines/table-engines/special/file
title: 'File テーブルエンジン'
doc_type: 'reference'
---

File テーブルエンジンは、サポートされている[ファイルフォーマット](/ja/interfaces/formats#formats-overview) (`TabSeparated`、`Native` など) のいずれかで、データをファイルに保存します。

使用シナリオ:

* ClickHouse からファイルへデータをエクスポートする。
* データをあるフォーマットから別のフォーマットに変換する。
* ディスク上のファイルを編集して ClickHouse のデータを更新する。

:::note
このエンジンは現在 ClickHouse Cloud では利用できません。代わりに、[S3 table function を使用してください](/ja/sql-reference/table-functions/s3.md)。
:::

<div id="usage-in-clickhouse-server">
  ## ClickHouse Server での利用
</div>

```sql
File(Format)
```

`Format` パラメータでは、使用可能なファイルフォーマットのいずれかを指定します。
`SELECT` クエリを実行するには、そのフォーマットが入力に対応している必要があり、`INSERT` クエリを実行するには、出力に対応している必要があります。使用可能なフォーマットは [フォーマット](/ja/interfaces/formats#formats-overview) セクションに一覧表示されています。

ClickHouse では、`File` に対して filesystem の path を指定することはできません。代わりに、server configuration の [path](../../../operations/server-configuration-parameters/settings.md) 設定で定義されたフォルダーが使用されます。

`File(Format)` を使用してテーブルを作成すると、そのフォルダー内に空の subdirectory が作成されます。そのテーブルにデータが書き込まれると、その subdirectory 内の `data.Format` ファイルに保存されます。

この subdirectory とファイルは server filesystem 上に手動で作成することもできます。その後、対応する名前のテーブル情報に [ATTACH](../../../sql-reference/statements/attach.md) すれば、そのファイル内のデータをクエリできます。

:::note
この機能を使用する際は注意してください。ClickHouse は、このようなファイルに対する外部からの変更を追跡しません。ClickHouse 経由の書き込みと ClickHouse 外部からの書き込みが同時に行われた場合の結果は undefined です。
:::

<div id="example">
  ## 例
</div>

**1.** `file_engine_table` テーブルを作成します:

```sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

デフォルトでは、ClickHouse がフォルダ `/var/lib/clickhouse/data/default/file_engine_table` を作成します。

**2.** `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` を手動で作成し、次の内容を記述します。

```bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** データにクエリを実行します：

```sql
SELECT * FROM file_engine_table
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="usage-in-clickhouse-local">
  ## ClickHouse-localでの使用
</div>

[clickhouse-local](../../../operations/utilities/clickhouse-local.md) では、Fileエンジンは `Format` に加えて file path も受け付けます。デフォルトの入力/出力ストリームは、`0` または `stdin`、`1` または `stdout` のように、数値名または可読名で指定できます。追加のエンジンパラメータまたはファイル拡張子 (`gz`、`br`、`xz`) に基づいて、圧縮ファイルの読み書きも可能です。

**例:**

```bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

<div id="details-of-implementation">
  ## 実装の詳細
</div>

* 複数の `SELECT` クエリは同時実行できますが、`INSERT` クエリは互いの完了を待ちます。
* `INSERT` クエリで新しいファイルを作成できます。
* ファイルが存在する場合、`INSERT` はその末尾に新しい値を追記します。
* 以下はサポートされていません:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * インデックス
  * レプリケーション

<div id="partition-by">
  ## PARTITION BY
</div>

`PARTITION BY` — 任意です。パーティションキーでデータをパーティション化すると、別々のファイルを作成できます。ほとんどの場合、パーティションキーは必要ありません。必要な場合でも、通常は月単位より細かいパーティションキーは不要です。パーティション化でクエリが高速化されることはありません (`ORDER BY` 式とは対照的です) 。細かすぎるパーティション化は絶対に避けてください。クライアント識別子や名前でデータをパーティション化しないでください (代わりに、クライアント識別子または名前を `ORDER BY` 式の先頭のカラムにしてください) 。

月単位でパーティション化するには、`toYYYYMM(date_column)` 式を使用します。ここで、`date_column` は [Date](/ja/sql-reference/data-types/date.md) 型の日付を持つカラムです。ここでのパーティション名は `"YYYYMM"` フォーマットになります。

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — ファイルのパス。型: `LowCardinality(String)`.
* `_file` — ファイル名。型: `LowCardinality(String)`.
* `_size` — ファイルサイズ (バイト単位) 。型: `Nullable(UInt64)`。サイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`。時刻が不明な場合、値は `NULL` です。

<div id="settings">
  ## 設定
</div>

* [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/ja/operations/settings/settings#engine_file_empty_if_not_exists) - 存在しないファイルから空のデータを選択できるようにします。デフォルトでは無効です。
* [engine&#95;file&#95;truncate&#95;on&#95;insert](/ja/operations/settings/settings#engine_file_truncate_on_insert) - insert の前にファイルを切り詰められるようにします。デフォルトでは無効です。
* [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/ja/operations/settings/settings.md#engine_file_allow_create_multiple_files) - フォーマットに接尾辞がある場合、insert のたびに新しいファイルを作成できるようにします。デフォルトでは無効です。
* [engine&#95;file&#95;skip&#95;empty&#95;files](/ja/operations/settings/settings.md#engine_file_skip_empty_files) - 読み取り時に空のファイルをスキップできるようにします。デフォルトでは無効です。
* [storage&#95;file&#95;read&#95;method](/ja/operations/settings/settings#engine_file_empty_if_not_exists) - ストレージファイルからデータを読み取る方法です。次のいずれかです: `read`, `pread`, `mmap`。`mmap` は clickhouse-server には適用されません (clickhouse-local 向けです) 。デフォルト値: clickhouse-server では `pread`、clickhouse-local では `mmap`。