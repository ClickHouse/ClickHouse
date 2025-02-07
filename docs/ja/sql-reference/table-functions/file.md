---
slug: /ja/sql-reference/table-functions/file
sidebar_position: 60
sidebar_label: file
---

# file

`file`は、ファイルに対してSELECT文とINSERT文を実行するためのテーブルエンジンであり、[s3](/docs/ja/sql-reference/table-functions/url.md)テーブル関数に似たテーブルライクなインターフェースを提供します。ローカルファイルを扱う際には`file()`を使用し、S3、GCS、MinIOなどのオブジェクトストレージのバケットを扱う際には`s3()`を使用します。

`file`関数は、ファイルからデータを読み込んだり、ファイルにデータを書き込むために`SELECT`および`INSERT`クエリで使用できます。

**構文**

```sql
file([path_to_archive ::] path [,format] [,structure] [,compression])
```

**パラメータ**

- `path` — [user_files_path](/docs/ja/operations/server-configuration-parameters/settings.md#user_files_path)からのファイルへの相対パスです。読み取り専用モードで以下の[グロブ](#globs-in-path)をサポートしています。`*`、`?`、`{abc,def}`（`'abc'`と`'def'`は文字列）、および`{N..M}`（`N`と`M`は数値）。
- `path_to_archive` - zip/tar/7zアーカイブへの相対パス。同じグロブを`path`と共にサポート。
- `format` — ファイルの[フォーマット](/docs/ja/interfaces/formats.md#formats)。
- `structure` — テーブルの構造。形式： `'column1_name column1_type, column2_name column2_type, ...'`。
- `compression` — `SELECT`クエリで使用する既存の圧縮タイプ、または`INSERT`クエリで使用する希望の圧縮タイプ。サポートされる圧縮タイプは`gz`、`br`、`xz`、`zst`、`lz4`、および`bz2`です。

**返される値**

ファイル内のデータの読み書きに使用されるテーブル。

## ファイルへの書き込み例

### TSVファイルに書き込む

```sql
INSERT INTO TABLE FUNCTION
file('test.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

結果として、データは`test.tsv`ファイルに書き込まれます：

```bash
# cat /var/lib/clickhouse/user_files/test.tsv
1	2	3
3	2	1
1	3	2
```

### 複数のTSVファイルへのパーティション化された書き込み

`file()`型のテーブル関数にデータを挿入する際に`PARTITION BY`式を指定すると、各パーティションごとに個別のファイルが作成されます。データを別々のファイルに分割することで読み取り操作のパフォーマンスを向上させることができます。

```sql
INSERT INTO TABLE FUNCTION
file('test_{_partition_id}.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

結果として、データは3つのファイル、`test_1.tsv`、`test_2.tsv`、`test_3.tsv`に書き込まれます。

```bash
# cat /var/lib/clickhouse/user_files/test_1.tsv
3	2	1

# cat /var/lib/clickhouse/user_files/test_2.tsv
1	3	2

# cat /var/lib/clickhouse/user_files/test_3.tsv
1	2	3
```

## ファイルからの読み取り例

### CSVファイルからのSELECT

まず、サーバー設定で`user_files_path`を設定し、`test.csv`ファイルを準備します：

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

次に、`test.csv`からデータをテーブルに読み込み、最初の2行を選択します：

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

### ファイルからテーブルへのデータの挿入

```sql
INSERT INTO FUNCTION
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1);
```
```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

`archive1.zip`または`archive2.zip`にある`table.csv`からのデータの読み込み：

```sql
SELECT * FROM file('user_files/archives/archive{1..2}.zip :: table.csv');
```

## パスのグロブ

パスにはグロビングを使用できます。ファイルはパスパターン全体にマッチする必要があり、接頭辞または接尾辞のみではありません。パスが既存のディレクトリを指し、グロブを使用しない場合、そのパスには暗黙的に`*`が追加され、ディレクトリ内のすべてのファイルが選択されるという一例外があります。

- `*` — 空文字列を含みますが`/`を除く任意の文字。
- `?` — 任意の単一文字。
- `{some_string,another_string,yet_another_one}` — 文字列 `'some_string'`, `'another_string'`, `'yet_another_one'` のいずれかを置換できます。文字列には`/`を含めることができます。
- `{N..M}` — `N >=` および `<= M` の任意の数。
- `**` - フォルダ内のすべてのファイルを再帰的に表します。

`{}`を使用した構造は[remote](remote.md)や[hdfs](hdfs.md)テーブル関数に似ています。

**例**

以下の相対パスを持つファイルがあるとします：

- `some_dir/some_file_1`
- `some_dir/some_file_2`
- `some_dir/some_file_3`
- `another_dir/some_file_1`
- `another_dir/some_file_2`
- `another_dir/some_file_3`

すべてのファイルの行数をクエリします：

```sql
SELECT count(*) FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32');
```

同じ結果を達成する別のパス表現：

```sql
SELECT count(*) FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32');
```

暗黙的な`*`を使用して`some_dir`内のすべての行数をクエリします：

```sql
SELECT count(*) FROM file('some_dir', 'TSV', 'name String, value UInt32');
```

:::note
ファイルリストに先行ゼロを持つ数範囲が含まれている場合、各桁に対して中括弧を使用する構造を使用するか、`?`を使用します。
:::

**例**

`file000`, `file001`, ..., `file999`というファイルに含まれる合計行数をクエリします：

```sql
SELECT count(*) FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32');
```

**例**

ディレクトリ`big_dir/`内のすべてのファイルから再帰的に行数をクエリします：

```sql
SELECT count(*) FROM file('big_dir/**', 'CSV', 'name String, value UInt32');
```

**例**

ディレクトリ`big_dir/`内の任意のフォルダ内にあるファイル`file002`から再帰的にすべての行数をクエリします：

```sql
SELECT count(*) FROM file('big_dir/**/file002', 'CSV', 'name String, value UInt32');
```

## 仮想カラム {#virtual-columns}

- `_path` — ファイルへのパス。型：`LowCardinality(String)`。
- `_file` — ファイル名。型：`LowCardinality(String)`。
- `_size` — バイト単位のファイルサイズ。型：`Nullable(UInt64)`。ファイルサイズが不明な場合、値は `NULL` になります。
- `_time` — ファイルの最終更新時刻。型：`Nullable(DateTime)`。時間が不明な場合、値は`NULL`になります。

## Hiveスタイルのパーティション化 {#hive-style-partitioning}

`use_hive_partitioning`設定を1に設定すると、ClickHouseはパス内のHiveスタイルのパーティション化(`/name=value/`)を検出し、パーティションカラムをクエリ内で仮想カラムとして使用できるようになります。これらの仮想カラムはパーティション化されたパスのカラム名と同じですが、先頭に`_`が付きます。

**例**

Hiveスタイルのパーティション化で作成された仮想カラムを使用

```sql
SET use_hive_partitioning = 1;
SELECT * from file('data/path/date=*/country=*/code=*/*.parquet') where _date > '2020-01-01' and _country = 'Netherlands' and _code = 42;
```

## 設定 {#settings}

- [engine_file_empty_if_not_exists](/docs/ja/operations/settings/settings.md#engine-file-empty_if-not-exists) - 存在しないファイルから空のデータを選択することを可能にします。デフォルトでは無効です。
- [engine_file_truncate_on_insert](/docs/ja/operations/settings/settings.md#engine-file-truncate-on-insert) - 挿入前にファイルを切り詰めることを許可します。デフォルトでは無効です。
- [engine_file_allow_create_multiple_files](/docs/ja/operations/settings/settings.md#engine_file_allow_create_multiple_files) - フォーマットにサフィックスがある場合、各挿入で新しいファイルを作成することを許可します。デフォルトでは無効です。
- [engine_file_skip_empty_files](/docs/ja/operations/settings/settings.md#engine_file_skip_empty_files) - 読み取り中に空のファイルをスキップすることを許可します。デフォルトでは無効です。
- [storage_file_read_method](/docs/ja/operations/settings/settings.md#engine-file-empty_if-not-exists) - ストレージファイルからデータを読み取る方法の1つ。read, pread, mmap（clickhouse-localのみ）。デフォルト値：`clickhouse-server`では`pread`、`clickhouse-local`では`mmap`。

**関連項目**

- [Virtual columns](/docs/ja/engines/table-engines/index.md#table_engines-virtual_columns)
- [処理後にファイルの名前を変更する](/docs/ja/operations/settings/settings.md#rename_files_after_processing)
