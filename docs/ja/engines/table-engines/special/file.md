---
slug: /ja/engines/table-engines/special/file
sidebar_position: 40
sidebar_label: File
---

# Fileテーブルエンジン

Fileテーブルエンジンは、サポートされている[ファイルフォーマット](../../../interfaces/formats.md#formats)（`TabSeparated`、`Native`など）の一つでファイルにデータを保持します。

使用シナリオ:

- ClickHouseからファイルへのデータエクスポート。
- データをあるフォーマットから別のフォーマットに変換。
- ディスク上のファイルを編集してClickHouse内のデータを更新。

:::note
このエンジンは現在ClickHouse Cloudでは利用できませんので、[代わりにS3テーブル関数を使用してください](/docs/ja/sql-reference/table-functions/s3.md)。
:::

## ClickHouseサーバーでの使用 {#usage-in-clickhouse-server}

``` sql
File(Format)
```

`Format`パラメータは利用可能なファイルフォーマットの一つを指定します。`SELECT`クエリを実行するには入力用、`INSERT`クエリを実行するには出力用のフォーマットがサポートされている必要があります。利用可能なフォーマットは[Formats](../../../interfaces/formats.md#formats)セクションに一覧で載っています。

ClickHouseは、`File`のファイルシステムパスの指定を許可していません。これはサーバー設定で[パス](../../../operations/server-configuration-parameters/settings.md)として定義されたフォルダを使用します。

`File(Format)`を使用してテーブルを作成すると、そのフォルダ内に空のサブディレクトリが作成されます。テーブルにデータが書き込まれると、そのサブディレクトリ内の`data.Format`ファイルにデータが保存されます。

サーバーファイルシステム内にこのサブフォルダとファイルを手動で作成し、マッチングする名前で[ATTACH](../../../sql-reference/statements/attach.md)することで、そのファイルからデータをクエリすることができます。

:::note
この機能を使用する際は注意してください。ClickHouseはそのようなファイルへの外部変更を追跡しません。ClickHouseとClickHouse外で同時に書き込みが行われた場合、結果は未定義です。
:::

## 例 {#example}

**1.** `file_engine_table`テーブルを設定する:

``` sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

デフォルトでは、ClickHouseは`/var/lib/clickhouse/data/default/file_engine_table`フォルダを作成します。

**2.** `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated`を手動で作成し、以下の内容を含める：

``` bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** データをクエリする：

``` sql
SELECT * FROM file_engine_table
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## ClickHouse-localでの使用 {#usage-in-clickhouse-local}

[clickhouse-local](../../../operations/utilities/clickhouse-local.md)では、Fileエンジンは`Format`に加えてファイルパスを受け入れます。デフォルトの入力/出力ストリームは、数値または人間に読みやすい名前（`0`や`stdin`、`1`や`stdout`など）で指定できます。圧縮ファイルの読み書きが、追加のエンジンパラメータまたはファイル拡張子（`gz`、`br`、または`xz`）に基づいて可能です。

**例:**

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## 実装の詳細 {#details-of-implementation}

- 複数の`SELECT`クエリを同時に実行できますが、`INSERT`クエリは互いに待ちます。
- `INSERT`クエリによる新しいファイルの作成がサポートされています。
- ファイルが存在する場合、`INSERT`は新しい値を追加します。
- サポートされていない機能：
  - `ALTER`
  - `SELECT ... SAMPLE`
  - インデックス
  - レプリケーション

## PARTITION BY {#partition-by}

`PARTITION BY` — オプションです。データをパーティションキーでパーティショニングすることで、別々のファイルを作成できます。ほとんどの場合、パーティションキーは必要ありませんし、必要であっても月ごとよりも詳細にする必要は通常ありません。パーティショニングはクエリを高速化しません（ORDER BY式とは対照的に）。過度に詳細なパーティショニングを使用しないでください。クライアント識別子や名前などでデータをパーティショニングしないで、クライアント識別子や名前をORDER BY式の最初のカラムにしてください。

月ごとにパーティショニングするには、`date_column`が[Date](/docs/ja/sql-reference/data-types/date.md)型の日付を持つカラムである場合、`toYYYYMM(date_column)`式を使用します。ここでのパーティション名は`"YYYYMM"`フォーマットになります。

## 仮想カラム {#virtual-columns}

- `_path` — ファイルへのパス。型: `LowCardinalty(String)`。
- `_file` — ファイルの名前。型: `LowCardinalty(String)`。
- `_size` — ファイルサイズ（バイト）。型: `Nullable(UInt64)`。サイズが不明な場合は値は`NULL`。
- `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`。時刻が不明な場合は値は`NULL`。

## 設定 {#settings}

- [engine_file_empty_if_not_exists](/docs/ja/operations/settings/settings.md#engine-file-empty_if-not-exists) - 存在しないファイルから空のデータを選択することを許可します。デフォルトで無効です。
- [engine_file_truncate_on_insert](/docs/ja/operations/settings/settings.md#engine-file-truncate-on-insert) - 挿入前にファイルを切り詰めることを許可します。デフォルトで無効です。
- [engine_file_allow_create_multiple_files](/docs/ja/operations/settings/settings.md#engine_file_allow_create_multiple_files) - フォーマットにサフィックスがある場合は、各挿入で新しいファイルを作成することを許可します。デフォルトで無効です。
- [engine_file_skip_empty_files](/docs/ja/operations/settings/settings.md#engine-file-skip-empty-files) - 読み込み中に空のファイルをスキップすることを許可します。デフォルトで無効です。
- [storage_file_read_method](/docs/ja/operations/settings/settings.md#engine-file-empty_if-not-exists) - ストレージファイルからデータを読み取る方法、`read`、`pread`、`mmap`のいずれか。mmapメソッドはclickhouse-serverには適用されません（clickhouse-local用）。デフォルト値: clickhouse-serverでは`pread`、clickhouse-localでは`mmap`。
