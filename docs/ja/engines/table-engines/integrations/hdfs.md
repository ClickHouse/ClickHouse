---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 36
toc_title: HDFS
---

# HDFS {#table_engines-hdfs}

このエンジンは、 [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) データの管理を可能にすることによって生態系 [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)クリックハウス経由。 このエンジンは同様です
に [ファイル](../special/file.md#table_engines-file) と [URL](../special/url.md#table_engines-url) エンジンが、Hadoop固有の機能を提供します。

## 使い方 {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

その `URI` parameterは、HDFSのファイルURI全体です。
その `format` パラメータを指定するか、ファイルのファイルフォーマット 実行するには
`SELECT` クエリは、形式は、入力のためにサポートされ、実行する必要があります
`INSERT` queries – for output. The available formats are listed in the
[形式](../../../interfaces/formats.md#formats) セクション。
のパス部分 `URI` グロブを含む可能性があります。 この場合、テーブルは読み取り専用になります。

**例えば:**

**1.** セットアップ `hdfs_engine_table` テーブル:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Fillファイル:

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** データのクエリ:

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
-   サポートなし:
    -   `ALTER` と `SELECT...SAMPLE` オペレーション
    -   インデックス。
    -   複製だ

**パス内のグロブ**

複数のパスコンポーネン のための処理中のファイルが存在するマッチのパスのパターンです。 ファイルのリストは、 `SELECT` （ないで `CREATE` 瞬間）。

-   `*` — Substitutes any number of any characters except `/` 空の文字列を含む。
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

構造との `{}` に類似していて下さい [リモート](../../../sql-reference/table-functions/remote.md) テーブル機能。

**例えば**

1.  HDFSに次のUriを持つTSV形式のファイルがいくつかあるとします:

-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_3’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_3’

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
    ファイルのリストに先行するゼロが付いた数値範囲が含まれている場合は、各桁ごとに中かっこで囲みます。 `?`.

**例えば**

このように作成されたテーブルとファイル名 `file000`, `file001`, … , `file999`:

``` sql
CREARE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

## 仮想列 {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**また見なさい**

-   [仮想列](../index.md#table_engines-virtual_columns)

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/hdfs/) <!--hide-->
