---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: hdfs
---

# hdfs {#hdfs}

HDFS内のファイルからテーブルを作成します。 この表関数は次のようになります [url](url.md) と [ファイル](file.md) ワンズ

``` sql
hdfs(URI, format, structure)
```

**入力パラメータ**

-   `URI` — The relative URI to the file in HDFS. Path to file support following globs in readonly mode: `*`, `?`, `{abc,def}` と `{N..M}` どこに `N`, `M` — numbers, \``'abc', 'def'` — strings.
-   `format` — The [形式](../../interfaces/formats.md#formats) ファイルの。
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**戻り値**

テーブルの指定された構造を読み取りまたは書き込みデータを、指定されたファイルです。

**例**

テーブルから `hdfs://hdfs1:9000/test` そして、それから最初の二つの行の選択:

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

**パス内のグロブ**

複数のパスコンポーネ のための処理中のファイルが存在するマッチのパスのパターンのみならず接尾辞または接頭).

-   `*` — Substitutes any number of any characters except `/` 空の文字列を含む。
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

構造との `{}` に類似しています [遠隔テーブル機能](../../sql-reference/table-functions/remote.md)).

**例**

1.  HDFSに次のUriを持つ複数のファイルがあるとします:

-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_3’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_3’

1.  これらのファイル内の行の量を照会します:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

1.  クエリの量の行のすべてのファイルのディレクトリ:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "警告"
    ファイ `?`.

**例**

クエリからのデータファイル名 `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## 仮想列 {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**も参照。**

-   [仮想列](https://clickhouse.tech/docs/en/operations/table_engines/#table_engines-virtual_columns)

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/hdfs/) <!--hide-->
