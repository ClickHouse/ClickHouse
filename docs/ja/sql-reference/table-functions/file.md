---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 37
toc_title: "\u30D5\u30A1\u30A4\u30EB"
---

# ファイル {#file}

ファイルからテーブルを作成します。 この表関数は次のようになります [url](url.md) と [hdfs](hdfs.md) もの。

``` sql
file(path, format, structure)
```

**入力パラメータ**

-   `path` — The relative path to the file from [user\_files\_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). 読み取り専用モードのglobsに続くファイルサポートのパス: `*`, `?`, `{abc,def}` と `{N..M}` どこに `N`, `M` — numbers, \``'abc', 'def'` — strings.
-   `format` — The [書式](../../interfaces/formats.md#formats) ファイルの
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**戻り値**

指定したファイルにデータを読み書きするための、指定した構造体を持つテーブル。

**例えば**

設定 `user_files_path` そして、ファイルの内容 `test.csv`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

テーブルから`test.csv` そしてそれからの最初の二つの行の選択:

``` sql
SELECT *
FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

``` sql
-- getting the first 10 lines of a table that contains 3 columns of UInt32 type from a CSV file
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```

**パス内のグロブ**

複数のパスコンポーネン 処理されるためには、ファイルが存在し、パスパターン全体（接尾辞や接頭辞だけでなく）に一致する必要があります。

-   `*` — Substitutes any number of any characters except `/` 空の文字列を含む。
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

構造との `{}` に類似していて下さい [遠隔テーブル機能](../../sql-reference/table-functions/remote.md)).

**例えば**

1.  次の相対パスを持つ複数のファイルがあるとします:

-   ‘some\_dir/some\_file\_1’
-   ‘some\_dir/some\_file\_2’
-   ‘some\_dir/some\_file\_3’
-   ‘another\_dir/some\_file\_1’
-   ‘another\_dir/some\_file\_2’
-   ‘another\_dir/some\_file\_3’

1.  これらのファイルの行数を照会します:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

1.  クエリの量の行のすべてのファイルのディレクトリ:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "警告"
    ファイルのリストに先行するゼロを持つ数値範囲が含まれている場合は、各桁のために中かっこで囲みます。 `?`.

**例えば**

クエリからのデータファイル名 `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*)
FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## 仮想列 {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**また見なさい**

-   [仮想列](https://clickhouse.tech/docs/en/operations/table_engines/#table_engines-virtual_columns)

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/file/) <!--hide-->
