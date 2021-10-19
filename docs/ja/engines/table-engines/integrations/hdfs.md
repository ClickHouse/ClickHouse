---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: HDFS
---

# HDFS {#table_engines-hdfs}

このエンジンは、 [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) 生態系および管理データ [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)クリックハウス経由。 このエンジンは同様です
に [ファイル](../special/file.md#table_engines-file) と [URL](../special/url.md#table_engines-url) エンジンが、Hadoop固有の機能を提供します。

## 使用法 {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

その `URI` パラメータは、HDFS内のファイルURI全体です。
その `format` パラメータを指定するか、ファイルのファイルフォーマット 実行するには
`SELECT` この形式は、入力と実行のためにサポートされている必要があります
`INSERT` queries – for output. The available formats are listed in the
[形式](../../../interfaces/formats.md#formats) セクション
のパス部分 `URI` globsを含むことができます。 この場合、テーブルはreadonlyになります。

**例:**

**1.** セットアップ `hdfs_engine_table` テーブル:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Fillファイル:

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** データの照会:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## 実装の詳細 {#implementation-details}

-   読み書きできる並列
-   対応していません:
    -   `ALTER` と `SELECT...SAMPLE` 作戦だ
    -   インデックス。
    -   複製。

**パス内のグロブ**

複数のパスコンポーネ のための処理中のファイルが存在するマッチのパスのパターンです。 ファイルのリストは `SELECT` （ではない `CREATE` 瞬間）。

-   `*` — Substitutes any number of any characters except `/` 空の文字列を含む。
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

構造との `{}` に類似しています [リモート](../../../sql-reference/table-functions/remote.md) テーブル関数。

**例**

1.  HDFS上に次のUriを持つTSV形式のファイルがいくつかあるとします:

-   ‘hdfs://hdfs1:9000/some_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_3’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_3’

1.  あはいくつかの方法が考えられているテーブルの構成は、すべてのファイル:

<!-- -->

``` sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

別の方法:

``` sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

テーブルはすべてのファイルの両方のディレクトリ(すべてのファイルが満たすべき書式は、スキーマに記載のクエリ):

``` sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

!!! warning "警告"
    ファイ `?`.

**例**

このように作成されたテーブルとファイル名 `file000`, `file001`, … , `file999`:

``` sql
CREARE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

## 仮想列 {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**も参照。**

-   [仮想列](../index.md#table_engines-virtual_columns)

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/hdfs/) <!--hide-->
