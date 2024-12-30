---
slug: /ja/sql-reference/table-functions/hdfs
sidebar_position: 80
sidebar_label: hdfs
---

# hdfs

HDFS内のファイルからテーブルを作成します。このテーブル関数は、[url](../../sql-reference/table-functions/url.md)や[file](../../sql-reference/table-functions/file.md)の関数と似ています。

``` sql
hdfs(URI, format, structure)
```

**入力パラメータ**

- `URI` — HDFS内のファイルへの相対URIです。ファイルパスは読み取り専用モードで以下のグロブをサポートしています: `*`, `?`, `{abc,def}` および `{N..M}` ただし `N`, `M` は数字で、`'abc', 'def'` は文字列です。
- `format` — ファイルの[フォーマット](../../interfaces/formats.md#formats)です。
- `structure` — テーブルの構造です。フォーマットは `'column1_name column1_type, column2_name column2_type, ...'` です。

**返される値**

指定されたファイル内のデータを読み書きできるようにした、指定された構造を持つテーブルです。

**例**

`hdfs://hdfs1:9000/test` からテーブルを取得し、そのテーブルから最初の二行を選択します:

``` sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

## パスのグロブ {#globs_in_path}

パスはグロブ化を使用することができます。ファイルはパスパターン全体と一致する必要があり、接尾辞や接頭辞だけに一致するものではありません。

- `*` — `/` を除く任意の文字列を表しますが、空の文字列も含みます。
- `**` — フォルダ内のすべてのファイルを再帰的に表します。
- `?` — 任意の1文字を表します。
- `{some_string,another_string,yet_another_one}` — 文字列 `'some_string', 'another_string', 'yet_another_one'` のいずれかを代用します。文字列には `/` 記号を含めることができます。
- `{N..M}` — 任意の数値 `>= N` かつ `<= M` を表します。

`{}` を使用した構造は、[remote](remote.md) および [file](file.md) テーブル関数に似ています。

**例**

1. 以下のURIを持つ複数のファイルがHDFSに存在するとします:

- ‘hdfs://hdfs1:9000/some_dir/some_file_1’
- ‘hdfs://hdfs1:9000/some_dir/some_file_2’
- ‘hdfs://hdfs1:9000/some_dir/some_file_3’
- ‘hdfs://hdfs1:9000/another_dir/some_file_1’
- ‘hdfs://hdfs1:9000/another_dir/some_file_2’
- ‘hdfs://hdfs1:9000/another_dir/some_file_3’

2. これらのファイル内の行数をクエリします:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. これら二つのディレクトリのすべてのファイル内の行数をクエリします:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
ファイルのリストに先頭ゼロ付きの数値範囲が含まれる場合、各桁ごとに波括弧を使用するか、`?` を使用してください。
:::

**例**

`file000`, `file001`, ... , `file999` と名付けられたファイルからデータをクエリします:

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## バーチャルカラム

- `_path` — ファイルのパス。タイプ: `LowCardinalty(String)`。
- `_file` — ファイル名。タイプ: `LowCardinalty(String)`。
- `_size` — ファイルのサイズ（バイト単位）。タイプ: `Nullable(UInt64)`。サイズが不明な場合、値は `NULL` です。
- `_time` — ファイルの最終更新日時。タイプ: `Nullable(DateTime)`。日時が不明な場合、値は `NULL` です。

## Hiveスタイルのパーティショニング {#hive-style-partitioning}

`use_hive_partitioning` を1に設定すると、ClickHouseはパス内の Hiveスタイルのパーティショニング (`/name=value/`) を検出し、クエリ内でパーティションカラムをバーチャルカラムとして使用できるようになります。これらのバーチャルカラムは、パーティション化されたパス内のカラムと同じ名前ですが、`_`で始まります。

**例**

Hiveスタイルのパーティショニングで作成されたバーチャルカラムを使用します

``` sql
SET use_hive_partitioning = 1;
SELECT * from HDFS('hdfs://hdfs1:9000/data/path/date=*/country=*/code=*/*.parquet') where _date > '2020-01-01' and _country = 'Netherlands' and _code = 42;
```

## ストレージ設定 {#storage-settings}

- [hdfs_truncate_on_insert](/docs/ja/operations/settings/settings.md#hdfs_truncate_on_insert) - 挿入前にファイルを切り詰めることを許可します。デフォルトでは無効です。
- [hdfs_create_new_file_on_insert](/docs/ja/operations/settings/settings.md#hdfs_create_new_file_on_insert) - フォーマットに接尾辞がある場合、挿入ごとに新しいファイルを作成します。デフォルトでは無効です。
- [hdfs_skip_empty_files](/docs/ja/operations/settings/settings.md#hdfs_skip_empty_files) - 読み取り中に空のファイルをスキップすることを許可します。デフォルトでは無効です。
- [ignore_access_denied_multidirectory_globs](/docs/ja/operations/settings/settings.md#ignore_access_denied_multidirectory_globs) - マルチディレクトリグロブに対して許可拒否エラーを無視することを許可します。

**関連項目**

- [バーチャルカラム](../../engines/table-engines/index.md#table_engines-virtual_columns)
