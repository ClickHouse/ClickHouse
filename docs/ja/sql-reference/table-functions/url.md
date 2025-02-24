---
slug: /ja/sql-reference/table-functions/url
sidebar_position: 200
sidebar_label: url
---

# url

`url` 関数は指定された `URL` から `format` と `structure` に従ったテーブルを作成します。

`url` 関数は、[URL](../../engines/table-engines/special/url.md) テーブルのデータに対する `SELECT` および `INSERT` クエリで使用できます。

**構文**

``` sql
url(URL [,format] [,structure] [,headers])
```

**パラメータ**

- `URL` — `GET` または `POST` リクエストを受け入れることができる HTTP または HTTPS サーバーのアドレス（それぞれ `SELECT` または `INSERT` クエリ時）。型: [String](../../sql-reference/data-types/string.md)。
- `format` — データの[フォーマット](../../interfaces/formats.md#formats)。型: [String](../../sql-reference/data-types/string.md)。
- `structure` — `'UserID UInt64, Name String'` 形式によるテーブル構造。カラム名と型を決定します。型: [String](../../sql-reference/data-types/string.md)。
- `headers` - `'headers('key1'='value1', 'key2'='value2')'` の形式で指定するヘッダー。HTTP 呼び出し時にヘッダーの設定が可能です。

**返される値**

指定された形式と構造を持ち、定義された `URL` からデータを取得したテーブル。

**例**

`String` 型と [UInt32](../../sql-reference/data-types/int-uint.md) 型のカラムを持つテーブルの最初の 3 行を、[CSV](../../interfaces/formats.md#csv) フォーマットで回答する HTTP サーバーから取得します。

``` sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

`URL` からデータをテーブルに挿入します:

``` sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

## URLのグロブ

中括弧 `{ }` で囲まれたパターンは、シャードのセットを生成するか、フェイルオーバーアドレスを指定するために使用します。サポートされているパターンの種類と例については、[remote](remote.md#globs-in-addresses) 関数の説明を参照してください。
パターン内の `|` 文字は、フェイルオーバーアドレスを指定するために使用されます。パターンにリストされた順に反復されます。生成されるアドレスの数は [glob_expansion_max_elements](../../operations/settings/settings.md#glob_expansion_max_elements) 設定によって制限されます。

## 仮想カラム

- `_path` — `URL` へのパス。型: `LowCardinalty(String)`。
- `_file` — `URL` のリソース名。型: `LowCardinalty(String)`。
- `_size` — リソースのサイズ（バイト単位）。型: `Nullable(UInt64)`。サイズが不明な場合、この値は `NULL` です。
- `_time` — ファイルの最終変更時間。型: `Nullable(DateTime)`。時間が不明な場合、この値は `NULL` です。
- `_headers` - HTTP レスポンスヘッダー。型: `Map(LowCardinality(String), LowCardinality(String))`。

## Hiveスタイルのパーティショニング {#hive-style-partitioning}

`use_hive_partitioning` 設定を1に設定すると、ClickHouseはパス内のHiveスタイルのパーティショニング（`/name=value/`）を検出し、クエリ内で仮想カラムとしてパーティションカラムを使用できるようになります。これらの仮想カラムはパーティションパス内と同じ名前を持ちますが、先頭に`_`が付きます。

**例**

Hiveスタイルのパーティショニングで作成された仮想カラムを使用

``` sql
SET use_hive_partitioning = 1;
SELECT * from url('http://data/path/date=*/country=*/code=*/*.parquet') where _date > '2020-01-01' and _country = 'Netherlands' and _code = 42;
```

## ストレージ設定 {#storage-settings}

- [engine_url_skip_empty_files](/docs/ja/operations/settings/settings.md#engine_url_skip_empty_files) - 読み取り時に空のファイルをスキップすることができます。デフォルトでは無効です。
- [enable_url_encoding](/docs/ja/operations/settings/settings.md#enable_url_encoding) - URIのパスのデコード/エンコードを有効/無効にすることができます。デフォルトで有効です。

**関連項目**

- [仮想カラム](/docs/ja/engines/table-engines/index.md#table_engines-virtual_columns)
