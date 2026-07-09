---
description: 'HDFS 内のファイルからテーブルを作成します。このテーブル関数は
  url および file テーブル関数に似ています。'
sidebar_label: 'hdfs'
sidebar_position: 80
slug: /sql-reference/table-functions/hdfs
title: 'hdfs'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-function">
  # hdfs テーブル関数
</div>

HDFS 内のファイルからテーブルを作成するテーブル関数です。このテーブル関数は、[url](../../sql-reference/table-functions/url.md) および [file](../../sql-reference/table-functions/file.md) のテーブル関数に似ています。

<div id="syntax">
  ## 構文
</div>

```sql
hdfs(URI, format, structure)
```

<div id="arguments">
  ## 引数
</div>

| Argument    | Description                                                                                                               |
| ----------- | ------------------------------------------------------------------------------------------------------------------------- |
| `URI`       | HDFS 内のファイルへの相対 URI。ファイルパスでは、読み取り専用モードで `*`、`?`、`{abc,def}`、`{N..M}` の各グロブを使用できます。ここで、`N`、`M` は数値、`'abc'`、`'def'` は文字列です。 |
| `format`    | ファイルの [フォーマット](/ja/sql-reference/formats)。                                                                                   |
| `structure` | テーブルの構造。形式は `'column1_name column1_type, column2_name column2_type, ...'` です。                                             |

<div id="returned_value">
  ## 戻り値
</div>

指定したファイル内のデータを読み書きするための、指定した構造を持つテーブルを返します。

**例**

`hdfs://hdfs1:9000/test` のテーブルと、そこから最初の 2 行を選択する例:

```sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="globs_in_path">
  ## パス内のグロブ
</div>

パスではグロブを使用できます。ファイルは、接尾辞やプレフィックスだけでなく、パスパターン全体に一致している必要があります。

* `*` — 空文字列を含み、`/` を除く任意の長さの文字列を表します。
* `**` — フォルダ内のすべてのファイルを再帰的に表します。
* `?` — 任意の 1 文字を表します。
* `{some_string,another_string,yet_another_one}` — `'some_string'`、`'another_string'`、`'yet_another_one'` のいずれかの文字列に置き換えます。これらの文字列には `/` 記号を含めることができます。
* `{N..M}` — `>= N` かつ `<= M` の任意の数を表します。

`{}` を使う構文は、[remote](remote.md) および [file](file.md) テーブル関数と似ています。

**例**

1. HDFS 上に、次の URI を持つ複数のファイルがあるとします。

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. これらのファイル内の行数をクエリします。

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. これら2つのディレクトリ内のすべてのファイルの行数を取得します:

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
ファイル一覧に先頭が 0 の数値範囲が含まれる場合は、各桁ごとに中括弧を使った構文を使用するか、`?` を使用してください。
:::

**例**

`file000`、`file001`、...、`file999` という名前のファイルからデータを取得します:

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — ファイルのパス。型: `LowCardinality(String)`.
* `_file` — ファイル名。型: `LowCardinality(String)`.
* `_size` — ファイルサイズ (バイト単位) 。型: `Nullable(UInt64)`. サイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`. 時刻が不明な場合、値は `NULL` です。

<div id="hive-style-partitioning">
  ## use_hive_partitioning 設定
</div>

`use_hive_partitioning` 設定を 1 にすると、ClickHouse はパス (`/name=value/`) 内の Hive スタイルのパーティションを検出し、クエリでパーティションカラムを仮想カラムとして使用できるようになります。これらの仮想カラムには、パーティション化されたパス内と同じ名前が付きます。

**例**

Hive スタイルのパーティションで作成された仮想カラムを使用する

```sql
SELECT * FROM HDFS('hdfs://hdfs1:9000/data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="storage-settings">
  ## ストレージ設定
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/ja/operations/settings/settings.md#hdfs_truncate_on_insert) - ファイルに insert する前に、そのファイルを切り詰めることを許可します。デフォルトでは無効です。
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/ja/operations/settings/settings.md#hdfs_create_new_file_on_insert) - フォーマットに接尾辞がある場合、insert のたびに新しいファイルを作成することを許可します。デフォルトでは無効です。
* [hdfs&#95;skip&#95;empty&#95;files](/ja/operations/settings/settings.md#hdfs_skip_empty_files) - 読み取り時に空のファイルをスキップすることを許可します。デフォルトでは無効です。

<div id="related">
  ## 関連
</div>

* [仮想カラム](../../engines/table-engines/index.md#table_engines-virtual_columns)