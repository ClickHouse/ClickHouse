---
slug: /ja/interfaces/schema-inference
sidebar_position: 21
sidebar_label: スキーマの推測
title: 入力データからの自動スキーマの推測
---

ClickHouseは、ほぼすべての対応する[入力フォーマット](formats.md)で、入力データの構造を自動的に決定できます。このドキュメントでは、スキーマの推測が使用されるとき、その動作、およびそれを制御することができる設定について説明します。

## 使用法 {#usage}

スキーマの推測は、ClickHouseが特定のデータフォーマットでデータを読み取る必要があり、その構造が不明なときに使用されます。

## テーブル関数 [file](../sql-reference/table-functions/file.md), [s3](../sql-reference/table-functions/s3.md), [url](../sql-reference/table-functions/url.md), [hdfs](../sql-reference/table-functions/hdfs.md), [azureBlobStorage](../sql-reference/table-functions/azureBlobStorage.md).

これらのテーブル関数は、入力データの構造を持つオプションの引数 `structure` を持っています。この引数が指定されていないか `auto` に設定されている場合、データから構造が推測されます。

**例:**

ディレクトリ `user_files` に JSONEachRow 形式のファイル `hobbies.jsonl` があり、内容は次の通りです:
```json
{"id" :  1, "age" :  25, "name" :  "Josh", "hobbies" :  ["football", "cooking", "music"]}
{"id" :  2, "age" :  19, "name" :  "Alan", "hobbies" :  ["tennis", "art"]}
{"id" :  3, "age" :  32, "name" :  "Lana", "hobbies" :  ["fitness", "reading", "shopping"]}
{"id" :  4, "age" :  47, "name" :  "Brayan", "hobbies" :  ["movies", "skydiving"]}
```

ClickHouseは構造を指定せずにこのデータを読み取ることができます：
```sql
SELECT * FROM file('hobbies.jsonl')
```
```response
┌─id─┬─age─┬─name───┬─hobbies──────────────────────────┐
│  1 │  25 │ Josh   │ ['football','cooking','music']   │
│  2 │  19 │ Alan   │ ['tennis','art']                 │
│  3 │  32 │ Lana   │ ['fitness','reading','shopping'] │
│  4 │  47 │ Brayan │ ['movies','skydiving']           │
└────┴─────┴────────┴──────────────────────────────────┘
```

注: フォーマット `JSONEachRow` は自動的にファイル拡張子 `.jsonl` から決定されました。

`DESCRIBE` クエリを使用して自動的に決定された構造を確認できます：
```sql
DESCRIBE file('hobbies.jsonl')
```
```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

## テーブルエンジン [File](../engines/table-engines/special/file.md), [S3](../engines/table-engines/integrations/s3.md), [URL](../engines/table-engines/special/url.md), [HDFS](../engines/table-engines/integrations/hdfs.md), [azureBlobStorage](../engines/table-engines/integrations/azureBlobStorage.md)

`CREATE TABLE` クエリでカラムのリストが指定されていない場合、テーブルの構造はデータから自動的に推測されます。

**例:**

ファイル `hobbies.jsonl` を使用します。このファイルからデータを持つエンジン `File` のテーブルを作成できます：
```sql
CREATE TABLE hobbies ENGINE=File(JSONEachRow, 'hobbies.jsonl')
```
```response
Ok.
```
```sql
SELECT * FROM hobbies
```
```response
┌─id─┬─age─┬─name───┬─hobbies──────────────────────────┐
│  1 │  25 │ Josh   │ ['football','cooking','music']   │
│  2 │  19 │ Alan   │ ['tennis','art']                 │
│  3 │  32 │ Lana   │ ['fitness','reading','shopping'] │
│  4 │  47 │ Brayan │ ['movies','skydiving']           │
└────┴─────┴────────┴──────────────────────────────────┘
```
```sql
DESCRIBE TABLE hobbies
```
```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

## clickhouse-local

`clickhouse-local` は入力データの構造を持つオプションのパラメータ `-S/--structure` を持っています。このパラメータが指定されていないか `auto` に設定されている場合、データから構造が推測されます。

**例:**

ファイル `hobbies.jsonl` を使用します。このファイルから `clickhouse-local` を使用してデータをクエリすることができます：
```shell
clickhouse-local --file='hobbies.jsonl' --table='hobbies' --query='DESCRIBE TABLE hobbies'
```
```response
id	Nullable(Int64)
age	Nullable(Int64)
name	Nullable(String)
hobbies	Array(Nullable(String))
```
```shell
clickhouse-local --file='hobbies.jsonl' --table='hobbies' --query='SELECT * FROM hobbies'
```
```response
1	25	Josh	['football','cooking','music']
2	19	Alan	['tennis','art']
3	32	Lana	['fitness','reading','shopping']
4	47	Brayan	['movies','skydiving']
```

## 挿入テーブルからの構造の使用 {#using-structure-from-insertion-table}

テーブル関数 `file/s3/url/hdfs` を使用してテーブルにデータを挿入するとき、データから抽出する代わりに挿入テーブルの構造を使用するオプションがあります。スキーマの推測には時間がかかることがあるため、挿入のパフォーマンスが向上します。また、テーブルが最適化されたスキーマを持っている場合には、型の変換が行われないので役立ちます。

この動作を制御する特別な設定 [use_structure_from_insertion_table_in_table_functions](/docs/ja/operations/settings/settings.md/#use_structure_from_insertion_table_in_table_functions) があります。これは3つの値を持ちます：
- 0 - テーブル関数はデータから構造を抽出します。
- 1 - テーブル関数は挿入テーブルの構造を使用します。
- 2 - ClickHouseは挿入テーブルの構造を使用できるかスキーマの推測を使用するか自動的に判断します。デフォルト値。

**例 1:**

次の構造でテーブル `hobbies1` を作成しましょう：
```sql
CREATE TABLE hobbies1
(
    `id` UInt64,
    `age` LowCardinality(UInt8),
    `name` String,
    `hobbies` Array(String)
)
ENGINE = MergeTree
ORDER BY id;
```

ファイル `hobbies.jsonl` からデータを挿入します：

```sql
INSERT INTO hobbies1 SELECT * FROM file(hobbies.jsonl)
```

この場合、ファイルからのすべてのカラムが変更なしでテーブルに挿入されるため、ClickHouseはスキーマの推測を使用せず、挿入テーブルの構造を使用します。

**例 2:**

次の構造でテーブル `hobbies2` を作成しましょう：
```sql
CREATE TABLE hobbies2
(
  `id` UInt64,
  `age` LowCardinality(UInt8),
  `hobbies` Array(String)
)
  ENGINE = MergeTree
ORDER BY id;
```

ファイル `hobbies.jsonl` からデータを挿入します：

```sql
INSERT INTO hobbies2 SELECT id, age, hobbies FROM file(hobbies.jsonl)
```

この場合、`SELECT` クエリで使用されるすべてのカラムがテーブルに存在するため、ClickHouseは挿入テーブルの構造を使用します。ただし、これは JSONEachRow、TSKV、Parquet などの一部のカラムの読み取りをサポートする入力形式でのみ動作します（例えばTSV形式では動作しません）。

**例 3:**

次の構造でテーブル `hobbies3` を作成しましょう：

```sql
CREATE TABLE hobbies3
(
  `identifier` UInt64,
  `age` LowCardinality(UInt8),
  `hobbies` Array(String)
)
  ENGINE = MergeTree
ORDER BY identifier;
```

ファイル `hobbies.jsonl` からデータを挿入します：

```sql
INSERT INTO hobbies3 SELECT id, age, hobbies FROM file(hobbies.jsonl)
```

この場合、`SELECT` クエリでカラム `id` が使用されていますが、テーブルにはこのカラムが存在しません（`identifier` という名前のカラムがあります）、したがって ClickHouse は挿入テーブルの構造を使用できず、スキーマの推測が使用されます。

**例 4:**

次の構造でテーブル `hobbies4` を作成しましょう：

```sql
CREATE TABLE hobbies4
(
  `id` UInt64,
  `any_hobby` Nullable(String)
)
  ENGINE = MergeTree
ORDER BY id;
```

ファイル `hobbies.jsonl` からデータを挿入します：

```sql
INSERT INTO hobbies4 SELECT id, empty(hobbies) ? NULL : hobbies[1] FROM file(hobbies.jsonl)
```

この場合、カラム `hobbies` に対して `SELECT` クエリ内でテーブルに挿入するためのいくつかの操作が行われているため、ClickHouse は挿入テーブルの構造を使用できず、スキーマの推測が使用されます。

## スキーマ推測キャッシュ {#schema-inference-cache}

ほとんどの入力形式のスキーマ推測では、その構造を決定するためにデータをいくつか読み込み、このプロセスには時間がかかることがあります。
ClickHouseが同じファイルからデータを読み取るたびに同じスキーマの推測を防ぐために、推測されたスキーマはキャッシュされ、再度同じファイルにアクセスする場合、ClickHouseはキャッシュからスキーマを使用します。

このキャッシュを制御する特別な設定があります：
- `schema_inference_cache_max_elements_for_{file/s3/hdfs/url/azure}` - 対応するテーブル関数のキャッシュされたスキーマの最大数。デフォルト値は `4096` です。これらの設定はサーバー構成で設定する必要があります。
- `schema_inference_use_cache_for_{file,s3,hdfs,url,azure}` - スキーマ推測のためのキャッシュをオン/オフすることができます。これらの設定はクエリ内で使用できます。

ファイルの形式設定を変更することによりファイルのスキーマを変更できます。
この理由から、スキーマ推測キャッシュはファイルソース、フォーマット名、使用されたフォーマット設定、ファイルの最終変更時刻によってスキーマを識別します。

注: `url` テーブル関数でアクセスした一部のファイルには最終修正時間に関する情報が含まれていない場合があります。このケースに特別な設定 `schema_inference_cache_require_modification_time_for_url` があります。この設定を無効にすることで、最終変更時間なしにキャッシュからのスキーマを使用することができます。

現在のキャッシュ内のすべてのスキーマとキャッシュをクリーンにするシステムクエリ `SYSTEM DROP SCHEMA CACHE [FOR File/S3/URL/HDFS]` を提供するシステムテーブル [schema_inference_cache](../operations/system-tables/schema_inference_cache.md) もあります。

**例:**

s3 のサンプルデータセット `github-2022.ndjson.gz` の構造を推測して、スキーマ推測キャッシュがどのように機能するかを見てみましょう：

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
SETTINGS allow_experimental_object_type = 1
```
```response
┌─name───────┬─type─────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ type       │ Nullable(String)         │              │                    │         │                  │                │
│ actor      │ Object(Nullable('json')) │              │                    │         │                  │                │
│ repo       │ Object(Nullable('json')) │              │                    │         │                  │                │
│ created_at │ Nullable(String)         │              │                    │         │                  │                │
│ payload    │ Object(Nullable('json')) │              │                    │         │                  │                │
└────────────┴──────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

5 rows in set. Elapsed: 0.601 sec.
```
```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
SETTINGS allow_experimental_object_type = 1
```
```response
┌─name───────┬─type─────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ type       │ Nullable(String)         │              │                    │         │                  │                │
│ actor      │ Object(Nullable('json')) │              │                    │         │                  │                │
│ repo       │ Object(Nullable('json')) │              │                    │         │                  │                │
│ created_at │ Nullable(String)         │              │                    │         │                  │                │
│ payload    │ Object(Nullable('json')) │              │                    │         │                  │                │
└────────────┴──────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

5 rows in set. Elapsed: 0.059 sec.
```

ご覧のとおり、2回目のクエリはほぼ即座に成功しました。

スキーマに影響を与える可能性のある設定を変更してみましょう：

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
SETTINGS input_format_json_read_objects_as_strings = 1

┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ type       │ Nullable(String) │              │                    │         │                  │                │
│ actor      │ Nullable(String) │              │                    │         │                  │                │
│ repo       │ Nullable(String) │              │                    │         │                  │                │
│ created_at │ Nullable(String) │              │                    │         │                  │                │
│ payload    │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

5 rows in set. Elapsed: 0.611 sec
```

キャッシュされたスキーマは、推測されたスキーマに影響を与える可能性がある設定が変更されたため、同じファイルには使用されませんでした。

`system.schema_inference_cache` テーブルの内容を確認してみましょう：

```sql
SELECT schema, format, source FROM system.schema_inference_cache WHERE storage='S3'
```
```response
┌─schema──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─format─┬─source───────────────────────────────────────────────────────────────────────────────────────────────────┐
│ type Nullable(String), actor Object(Nullable('json')), repo Object(Nullable('json')), created_at Nullable(String), payload Object(Nullable('json')) │ NDJSON │ datasets-documentation.s3.eu-west-3.amazonaws.com443/datasets-documentation/github/github-2022.ndjson.gz │
│ type Nullable(String), actor Nullable(String), repo Nullable(String), created_at Nullable(String), payload Nullable(String)                         │ NDJSON │ datasets-documentation.s3.eu-west-3.amazonaws.com443/datasets-documentation/github/github-2022.ndjson.gz │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

同じファイルに対して2つの異なるスキーマがあることがわかります。

システムクエリを使用してスキーマキャッシュをクリアできます：
```sql
SYSTEM DROP SCHEMA CACHE FOR S3
```
```response
Ok.
```
```sql
SELECT count() FROM system.schema_inference_cache WHERE storage='S3'
```
```response
┌─count()─┐
│       0 │
└─────────┘
```

## テキストフォーマット {#text-formats}

テキストフォーマットの場合、ClickHouseはデータを行ごとに読み取り、フォーマットに従ってカラム値を抽出し、その後、一部の再帰的なパーサーとヒューリスティックを使用して各値のタイプを決定します。スキーマ推測でデータから読み取る最大行数およびバイト数は、`input_format_max_rows_to_read_for_schema_inference`（デフォルトで 25000）および `input_format_max_bytes_to_read_for_schema_inference`（デフォルトで 32Mb）で制御されます。
デフォルトでは、すべての推測されたタイプは[Nullable](../sql-reference/data-types/nullable.md)ですが、`schema_inference_make_columns_nullable` を設定することでこれを変更できます（例は[テキストフォーマット用の設定](#settings-for-text-formats)セクションにあります）。

### JSONフォーマット {#json-formats}

JSONフォーマットでは、ClickHouse は JSON 仕様に従って値を解析し、その後、それらに最適なデータ型を見つけようとします。

どのように機能するか、どの型を推測できるのか、および JSONフォーマットで使用できる特定の設定を見てみましょう。

**例**

ここでは、[format](../sql-reference/table-functions/format.md) テーブル関数が例で使用されます。

整数、浮動小数点数、ブール値、文字列:
```sql
DESC format(JSONEachRow, '{"int" : 42, "float" : 42.42, "string" : "Hello, World!"}');
```
```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ int    │ Nullable(Int64)   │              │                    │         │                  │                │
│ float  │ Nullable(Float64) │              │                    │         │                  │                │
│ bool   │ Nullable(Bool)    │              │                    │         │                  │                │
│ string │ Nullable(String)  │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

日付、日付時刻:

```sql
DESC format(JSONEachRow, '{"date" : "2022-01-01", "datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}')
```
```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date       │ Nullable(Date)          │              │                    │         │                  │                │
│ datetime   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

配列:
```sql
DESC format(JSONEachRow, '{"arr" : [1, 2, 3], "nested_arrays" : [[1, 2, 3], [4, 5, 6], []]}')
```
```response
┌─name──────────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr           │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ nested_arrays │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

配列に `null` が含まれている場合、ClickHouseは他の配列要素からの型を使用します:
```sql
DESC format(JSONEachRow, '{"arr" : [null, 42, null]}')
```
```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

名前付きタプル:

設定 `input_format_json_try_infer_named_tuples_from_objects` が有効な場合、スキーマ推測の際に ClickHouseはJSONオブジェクトから名前付きタプルを推測しようとします。
できた名前付きタプルにはサンプルデータのすべての対応するJSONオブジェクトのすべての要素が含まれます。

```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

```response
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

名前なしタプル:

JSONフォーマットでは、異なるタイプの要素を持つ配列を名前なしタプルとして扱います。
```sql
DESC format(JSONEachRow, '{"tuple" : [1, "Hello, World!", [1, 2, 3]]}')
```
```response
┌─name──┬─type─────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ tuple │ Tuple(Nullable(Int64), Nullable(String), Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

一部の値が `null` や空の場合、他の行の対応する値の型を使用します:
```sql
DESC format(JSONEachRow, $$
                              {"tuple" : [1, null, null]}
                              {"tuple" : [null, "Hello, World!", []]}
                              {"tuple" : [null, null, [1, 2, 3]]}
                         $$)
```
```response
┌─name──┬─type─────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ tuple │ Tuple(Nullable(Int64), Nullable(String), Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

マップ:

JSONでは、同じ型の値を持つオブジェクトをMap型として読み取ることができます。
注意: これは設定 `input_format_json_read_objects_as_strings` および `input_format_json_try_infer_named_tuples_from_objects` が無効の場合にのみ機能します。

```sql
SET input_format_json_read_objects_as_strings = 0, input_format_json_try_infer_named_tuples_from_objects = 0;
DESC format(JSONEachRow, '{"map" : {"key1" : 42, "key2" : 24, "key3" : 4}}')
```
```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ map  │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

JSONオブジェクト型（設定 `allow_experimental_object_type` が有効な場合）:

```sql
SET allow_experimental_object_type = 1
DESC format(JSONEachRow, $$
                            {"obj" : {"key1" : 42}}
                            {"obj" : {"key2" : "Hello, World!"}}
                            {"obj" : {"key1" : 24, "key3" : {"a" : 42, "b" : null}}}
                         $$)
```
```response
┌─name─┬─type─────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Object(Nullable('json')) │              │                    │         │                  │                │
└──────┴──────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ネストした複雑な型:
```sql
DESC format(JSONEachRow, '{"value" : [[[42, 24], []], {"key1" : 42, "key2" : 24}]}')
```
```response
┌─name──┬─type─────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Tuple(Array(Array(Nullable(String))), Tuple(key1 Nullable(Int64), key2 Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ClickHouseがあるキーの型を決定できない場合、データがnull/空のオブジェクト/空の配列だけを含む場合、`input_format_json_infer_incomplete_types_as_strings` 設定が有効な場合は型 `String` が使用され、そうでない場合は例外がスローされます：
```sql
DESC format(JSONEachRow, '{"arr" : [null, null]}') SETTINGS input_format_json_infer_incomplete_types_as_strings = 1;
```
```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Nullable(String)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
DESC format(JSONEachRow, '{"arr" : [null, null]}') SETTINGS input_format_json_infer_incomplete_types_as_strings = 0;
```
```response
Code: 652. DB::Exception: Received from localhost:9000. DB::Exception:
Cannot determine type for column 'arr' by first 1 rows of data,
most likely this column contains only Nulls or empty Arrays/Maps.
...
```

#### JSON設定 {#json-settings}

##### input_format_json_try_infer_numbers_from_strings

この設定を有効にすると、文字列値から数値を推測できます。

この設定はデフォルトで無効です。

**例:**

```sql
SET input_format_json_try_infer_numbers_from_strings = 1;
DESC format(JSONEachRow, $$
                              {"value" : "42"}
                              {"value" : "424242424242"}
                         $$)
```
```response
┌─name──┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(Int64) │              │                    │         │                  │                │
└───────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

##### input_format_json_try_infer_named_tuples_from_objects

この設定を有効にすると、JSONオブジェクトから名前付きタプルを推測できます。結果の名前付きタプルには、サンプルデータの対応するJSONオブジェクトのすべての要素が含まれます。
JSONデータがスパースでない場合には、この設定が有効になります。

この設定はデフォルトで有効です。

**例**

```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

結果:

```
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"array" : [{"a" : 42, "b" : "Hello"}, {}, {"c" : [1,2,3]}, {"d" : "2020-01-01"}]}')
```

結果:

```
┌─name──┬─type────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ array │ Array(Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Nullable(Date))) │              │                    │         │                  │                │
└───────┴─────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

##### input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects

この設定を有効にすると、名前付きタプルの推測中に曖昧なパスに対して例外の代わりにString型を使用することができます（`input_format_json_try_infer_named_tuples_from_objects`が有効な場合）。
これにより、曖昧なパスがあってもJSONオブジェクトを名前付きタプルとして読み取ることができます。

デフォルトでは無効です。

**例**

設定無効時:
```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 0;
DESC format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : "Hello"}}}');
```
結果:

```text
Code: 636. DB::Exception: The table structure cannot be extracted from a JSONEachRow format file. Error:
Code: 117. DB::Exception: JSON objects have ambiguous data: in some objects path 'a' has type 'Int64' and in some - 'Tuple(b String)'. You can enable setting input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects to use String type for path 'a'. (INCORRECT_DATA) (version 24.3.1.1).
You can specify the structure manually. (CANNOT_EXTRACT_TABLE_STRUCTURE)
```

設定有効時:
```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : "a" : 42}, {"obj" : {"a" : {"b" : "Hello"}}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : "Hello"}}}');
```

結果:
```text
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(String))     │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
┌─obj─────────────────┐
│ ('42')              │
│ ('{"b" : "Hello"}') │
└─────────────────────┘
```

##### input_format_json_read_objects_as_strings

この設定を有効にすると、入れ子になったJSONオブジェクトを文字列として読み取ることができます。
この設定は、JSONオブジェクト型を使用せずに入れ子になったJSONオブジェクトを読み取るために使用できます。

この設定はデフォルトで有効です。

注意: この設定を有効にする場合は、`input_format_json_try_infer_named_tuples_from_objects`も無効にする必要があります。

```sql
SET input_format_json_read_objects_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 0;
DESC format(JSONEachRow, $$
                             {"obj" : {"key1" : 42, "key2" : [1,2,3,4]}}
                             {"obj" : {"key3" : {"nested_key" : 1}}}
                         $$)
```
```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```


##### input_format_json_read_numbers_as_strings

この設定を有効にすると、数値値を文字列として読み取ることができます。

この設定はデフォルトで有効です。

**例**

```sql
SET input_format_json_read_numbers_as_strings = 1;
DESC format(JSONEachRow, $$
                                {"value" : 1055}
                                {"value" : "unknown"}
                         $$)
```
```response
┌─name──┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(String) │              │                    │         │                  │                │
└───────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

##### input_format_json_read_bools_as_numbers

この設定を有効にすると、Bool値を数値として読み取ることができます。

この設定はデフォルトで有効です。

**例:**

```sql
SET input_format_json_read_bools_as_numbers = 1;
DESC format(JSONEachRow, $$
                                {"value" : true}
                                {"value" : 42}
                         $$)
```
```response
┌─name──┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(Int64) │              │                    │         │                  │                │
└───────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

##### input_format_json_read_bools_as_strings

この設定を有効にすると、Bool値を文字列として読み取ることができます。

この設定はデフォルトで有効です。

**例:**

```sql
SET input_format_json_read_bools_as_strings = 1;
DESC format(JSONEachRow, $$
                                {"value" : true}
                                {"value" : "Hello, World"}
                         $$)
```
```response
┌─name──┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(String) │              │                    │         │                  │                │
└───────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
##### input_format_json_read_arrays_as_strings

この設定を有効にすると、JSON配列値を文字列として読み取ることができます。

この設定はデフォルトで有効です。

**例**

```sql
SET input_format_json_read_arrays_as_strings = 1;
SELECT arr, toTypeName(arr), JSONExtractArrayRaw(arr)[3] from format(JSONEachRow, 'arr String', '{"arr" : [1, "Hello", [1,2,3]]}');
```
```response
┌─arr───────────────────┬─toTypeName(arr)─┬─arrayElement(JSONExtractArrayRaw(arr), 3)─┐
│ [1, "Hello", [1,2,3]] │ String          │ [1,2,3]                                   │
└───────────────────────┴─────────────────┴───────────────────────────────────────────┘
```

##### input_format_json_infer_incomplete_types_as_strings

この設定を有効にすると、スキーマ推測時にサンプルデータに `Null`/`{}`/`[]` のみを含むJSONキーに対してString型を使用できます。
JSONフォーマットでは、すべての対応する設定が有効であれば、任意の値を文字列として読み取ることができ、スキーマ推測中に`Cannot determine type for column 'column_name' by first 25000 rows of data, most likely this column contains only Nulls or empty Arrays/Maps` のようなエラーを回避するためにキーの型が不明な場合に文字列型を使用できます。

例:

```sql
SET input_format_json_infer_incomplete_types_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 1;
DESCRIBE format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
```

結果:
```
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Array(Nullable(Int64)), b Nullable(String), c Nullable(String), d Nullable(String), e Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

┌─obj────────────────────────────┐
│ ([1,2,3],'hello',NULL,'{}',[]) │
└────────────────────────────────┘
```

### CSV {#csv}

CSVフォーマットでは、ClickHouseはデリミタに従って行からカラム値を抽出します。ClickHouseは、数値や文字列以外の全ての型をダブルクオートで囲むことを期待しています。値がダブルクオートで囲まれている場合、ClickHouseは中のデータを再帰パーサーを使用して解析し、その後、最も適切なデータ型を見つけようとします。値がダブルクオートで囲まれていない場合、ClickHouseはそれを数値として解析し、数値でない場合、ClickHouseはそれを文字列として扱います。

ClickHouseがパーサーとヒューリスティックを使って複雑な型を決定しないようにしたい場合は、設定 `input_format_csv_use_best_effort_in_schema_inference` を無効にすることができ、ClickHouseはすべてのカラムを文字列として扱います。

設定 `input_format_csv_detect_header` が有効な場合、ClickHouseはスキーマ推論中にカラム名（おそらく型も）を含むヘッダを検出しようとします。この設定はデフォルトで有効です。

**例:**

整数、浮動小数点数、ブール値、文字列:
```sql
DESC format(CSV, '42,42.42,true,"Hello,World!"')
```
```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

引用符なしの文字列:
```sql
DESC format(CSV, 'Hello world!,World hello!')
```
```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

日付、日付時刻:

```sql
DESC format(CSV, '"2020-01-01","2020-01-01 00:00:00","2022-01-01 00:00:00.000"')
```
```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

配列:
```sql
DESC format(CSV, '"[1,2,3]","[[1, 2], [], [3, 4]]"')
```
```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
DESC format(CSV, $$"['Hello', 'world']","[['Abc', 'Def'], []]"$$)
```
```response
┌─name─┬─type───────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(String))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

配列にnullが含まれている場合、ClickHouseは他の配列要素からの型を使用します:
```sql
DESC format(CSV, '"[NULL, 42, NULL]"')
```
```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

マップ:
```sql
DESC format(CSV, $$"{'key1' : 42, 'key2' : 24}"$$)
```
```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ネストした配列とマップ:
```sql
DESC format(CSV, $$"[{'key1' : [[42, 42], []], 'key2' : [[null], [42]]}]"$$)
```
```response
┌─name─┬─type──────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Array(Nullable(Int64))))) │              │                    │         │                  │                │
└──────┴───────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ClickHouseが引用符内の型を決定できない場合、データにnullしか含まれていない場合、ClickHouseはそれをStringとして扱います:
```sql
DESC format(CSV, '"[NULL, NULL]"')
```
```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

設定 `input_format_csv_use_best_effort_in_schema_inference` を無効にした例:
```sql
SET input_format_csv_use_best_effort_in_schema_inference = 0
DESC format(CSV, '"[1,2,3]",42.42,Hello World!')
```
```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ヘッダー自動検出の例（`input_format_csv_detect_header` が有効な場合）：

名前のみ:
```sql
SELECT * FROM format(CSV,
$$"number","string","array"
42,"Hello","[1, 2, 3]"
43,"World","[4, 5, 6]"
$$)
```

```response
┌─number─┬─string─┬─array───┐
│     42 │ Hello  │ [1,2,3] │
│     43 │ World  │ [4,5,6] │
└────────┴────────┴─────────┘
```

名前と型:

```sql
DESC format(CSV,
$$"number","string","array"
"UInt32","String","Array(UInt16)"
42,"Hello","[1, 2, 3]"
43,"World","[4, 5, 6]"
$$)
```

```response
┌─name───┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ UInt32        │              │                    │         │                  │                │
│ string │ String        │              │                    │         │                  │                │
│ array  │ Array(UInt16) │              │                    │         │                  │                │
└────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

注意: ヘッダーは、少なくとも1つのカラムが非文字列型である場合のみ検出されます。すべてのカラムが文字列型である場合、ヘッダーは検出されません:

```sql
SELECT * FROM format(CSV,
$$"first_column","second_column"
"Hello","World"
"World","Hello"
$$)
```

```response
┌─c1───────────┬─c2────────────┐
│ first_column │ second_column │
│ Hello        │ World         │
│ World        │ Hello         │
└──────────────┴───────────────┘
```

#### CSV設定 {#csv-settings}

##### input_format_csv_try_infer_numbers_from_strings

この設定を有効にすると、文字列値から数値を推測できます。

この設定はデフォルトで無効です。

**例:**

```sql
SET input_format_json_try_infer_numbers_from_strings = 1;
DESC format(CSV, '42,42.42');
```
```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

### TSV/TSKV {#tsv-tskv}

TSV/TSKVフォーマットでは、ClickHouseはタブ区切り文字に従って行からカラム値を抽出し、その後、再帰パーサーを使用して抽出された値を解析し、最も適切な型を決定します。型が決定できない場合、ClickHouseはこの値を文字列として扱います。

ClickHouseがパーサーとヒューリスティックを使って複雑な型を決定しないようにしたい場合は、設定 `input_format_tsv_use_best_effort_in_schema_inference` を無効にすることができ、ClickHouseはすべてのカラムを文字列として扱います。

設定 `input_format_tsv_detect_header` が有効な場合、ClickHouseはスキーマ推論中にカラム名（おそらく型も）を含むヘッダを検出しようとします。この設定はデフォルトで有効です。

**例:**

整数、浮動小数点数、ブール値、文字列:
```sql
DESC format(TSV, '42\t42.42\ttrue\tHello,World!')
```
```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
DESC format(TSKV, 'int=42\tfloat=42.42\tbool=true\tstring=Hello,World!\n')
```
```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ int    │ Nullable(Int64)   │              │                    │         │                  │                │
│ float  │ Nullable(Float64) │              │                    │         │                  │                │
│ bool   │ Nullable(Bool)    │              │                    │         │                  │                │
│ string │ Nullable(String)  │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

日付、日付時刻:

```sql
DESC format(TSV, '2020-01-01\t2020-01-01 00:00:00\t2022-01-01 00:00:00.000')
```
```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

配列:
```sql
DESC format(TSV, '[1,2,3]\t[[1, 2], [], [3, 4]]')
```
```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
DESC format(TSV, '[''Hello'', ''world'']\t[[''Abc'', ''Def''], []]')
```
```response
┌─name─┬─type───────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(String))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

配列にnullが含まれている場合、ClickHouseは他の配列要素からの型を使用します:
```sql
DESC format(TSV, '[NULL, 42, NULL]')
```
```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```


Tuples:
```sql
DESC format(TSV, $$(42, 'Hello, world!')$$)
```
```response
┌─name─┬─type─────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Tuple(Nullable(Int64), Nullable(String)) │              │                    │         │                  │                │
└──────┴──────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Maps:
```sql
DESC format(TSV, $${'key1' : 42, 'key2' : 24}$$)
```
```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ネストされた配列、タプル、マップ:
```sql
DESC format(TSV, $$[{'key1' : [(42, 'Hello'), (24, NULL)], 'key2' : [(NULL, ','), (42, 'world!')]}]$$)
```
```response
┌─name─┬─type────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Tuple(Nullable(Int64), Nullable(String))))) │              │                    │         │                  │                │
└──────┴─────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ClickHouseが型を判別できない場合、データがnullのみを含むため、ClickHouseはそれをStringとして扱います:
```sql
DESC format(TSV, '[NULL, NULL]')
```
```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

設定 `input_format_tsv_use_best_effort_in_schema_inference` が無効になっている場合の例:
```sql
SET input_format_tsv_use_best_effort_in_schema_inference = 0
DESC format(TSV, '[1,2,3]	42.42	Hello World!')
```
```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ヘッダーの自動検出の例（`input_format_tsv_detect_header` が有効な場合）:

名前のみ:
```sql
SELECT * FROM format(TSV,
$$number	string	array
42	Hello	[1, 2, 3]
43	World	[4, 5, 6]
$$);
```

```response
┌─number─┬─string─┬─array───┐
│     42 │ Hello  │ [1,2,3] │
│     43 │ World  │ [4,5,6] │
└────────┴────────┴─────────┘
```

名前と型:

```sql
DESC format(TSV,
$$number	string	array
UInt32	String	Array(UInt16)
42	Hello	[1, 2, 3]
43	World	[4, 5, 6]
$$)
```

```response
┌─name───┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ UInt32        │              │                    │         │                  │                │
│ string │ String        │              │                    │         │                  │                │
│ array  │ Array(UInt16) │              │                    │         │                  │                │
└────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

すべてのカラムがString型の場合、ヘッダーは検出されないことに注意してください:

```sql
SELECT * FROM format(TSV,
$$first_column	second_column
Hello	World
World	Hello
$$)
```

```response
┌─c1───────────┬─c2────────────┐
│ first_column │ second_column │
│ Hello        │ World         │
│ World        │ Hello         │
└──────────────┴───────────────┘
```

### Values {#values}

Values形式では、ClickHouseは行からカラム値を抽出し、その後リテラルを解析するのに似た再帰的なパーサーを使用して解析します。

**例:**

整数、浮動小数点、ブール、文字列:
```sql
DESC format(Values, $$(42, 42.42, true, 'Hello,World!')$$)
```
```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

日付、日時:

```sql
 DESC format(Values, $$('2020-01-01', '2020-01-01 00:00:00', '2022-01-01 00:00:00.000')$$)
 ```
```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

配列:
```sql
DESC format(Values, '([1,2,3], [[1, 2], [], [3, 4]])')
```
```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

配列にnullが含まれる場合、ClickHouseは他の配列要素からタイプを使用します:
```sql
DESC format(Values, '([NULL, 42, NULL])')
```
```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

タプル:
```sql
DESC format(Values, $$((42, 'Hello, world!'))$$)
```
```response
┌─name─┬─type─────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Tuple(Nullable(Int64), Nullable(String)) │              │                    │         │                  │                │
└──────┴──────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

マップ:
```sql
DESC format(Values, $$({'key1' : 42, 'key2' : 24})$$)
```
```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ネストされた配列、タプル、およびマップ:
```sql
DESC format(Values, $$([{'key1' : [(42, 'Hello'), (24, NULL)], 'key2' : [(NULL, ','), (42, 'world!')]}])$$)
```
```response
┌─name─┬─type────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Tuple(Nullable(Int64), Nullable(String))))) │              │                    │         │                  │                │
└──────┴─────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ClickHouseが型を判別できない場合、データがnullのみを含むため、例外がスローされます:
```sql
DESC format(Values, '([NULL, NULL])')
```
```response
Code: 652. DB::Exception: Received from localhost:9000. DB::Exception:
Cannot determine type for column 'c1' by first 1 rows of data,
most likely this column contains only Nulls or empty Arrays/Maps.
...
```

設定 `input_format_tsv_use_best_effort_in_schema_inference` が無効になっている場合の例:
```sql
SET input_format_tsv_use_best_effort_in_schema_inference = 0
DESC format(TSV, '[1,2,3]	42.42	Hello World!')
```
```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

### CustomSeparated {#custom-separated}

CustomSeparated形式では、ClickHouseはまず行から指定された区切り文字に従ってカラム値をすべて抽出し、次にエスケープルールに従ってそれぞれの値のデータ型を推測しようとします。

設定 `input_format_custom_detect_header` が有効であれば、ClickHouseはスキーマ推測中にカラム名（および場合によっては型）を含むヘッダーを検出しようとします。この設定はデフォルトで有効になっています。

**例**

```sql
SET format_custom_row_before_delimiter = '<row_before_delimiter>',
       format_custom_row_after_delimiter = '<row_after_delimiter>\n',
       format_custom_row_between_delimiter = '<row_between_delimiter>\n',
       format_custom_result_before_delimiter = '<result_before_delimiter>\n',
       format_custom_result_after_delimiter = '<result_after_delimiter>\n',
       format_custom_field_delimiter = '<field_delimiter>',
       format_custom_escaping_rule = 'Quoted'

DESC format(CustomSeparated, $$<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter>'Some string 1'<field_delimiter>[1, NULL, 3]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>NULL<field_delimiter>'Some string 3'<field_delimiter>[1, 2, NULL]<row_after_delimiter>
<result_after_delimiter>
$$)
```
```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Float64)      │              │                    │         │                  │                │
│ c2   │ Nullable(String)       │              │                    │         │                  │                │
│ c3   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ヘッダー自動検出の例（`input_format_custom_detect_header` が有効な場合）

```sql
SET format_custom_row_before_delimiter = '<row_before_delimiter>',
       format_custom_row_after_delimiter = '<row_after_delimiter>\n',
       format_custom_row_between_delimiter = '<row_between_delimiter>\n',
       format_custom_result_before_delimiter = '<result_before_delimiter>\n',
       format_custom_result_after_delimiter = '<result_after_delimiter>\n',
       format_custom_field_delimiter = '<field_delimiter>',
       format_custom_escaping_rule = 'Quoted'

DESC format(CustomSeparated, $$<result_before_delimiter>
<row_before_delimiter>'number'<field_delimiter>'string'<field_delimiter>'array'<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42.42<field_delimiter>'Some string 1'<field_delimiter>[1, NULL, 3]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>NULL<field_delimiter>'Some string 3'<field_delimiter>[1, 2, NULL]<row_after_delimiter>
<result_after_delimiter>
$$)
```

```response
┌─number─┬─string────────┬─array──────┐
│  42.42 │ Some string 1 │ [1,NULL,3] │
│   ᴺᵁᴸᴸ │ Some string 3 │ [1,2,NULL] │
└────────┴───────────────┴────────────┘
```

### Template {#template}

Template形式では、ClickHouseはまず指定されたテンプレートに従って行からすべてのカラム値を抽出し、その後、そのエスケープルールに従ってそれぞれの値のデータ型を推測します。

**例**

次の内容のファイル `resultset` があるとします:
```
<result_before_delimiter>
${data}<result_after_delimiter>
```

そして次の内容のファイル `row_format` があるとします:
```
<row_before_delimiter>${column_1:CSV}<field_delimiter_1>${column_2:Quoted}<field_delimiter_2>${column_3:JSON}<row_after_delimiter>
```

次のクエリを実行できます:

```sql
SET format_template_rows_between_delimiter = '<row_between_delimiter>\n',
       format_template_row = 'row_format',
       format_template_resultset = 'resultset_format'

DESC format(Template, $$<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter_1>'Some string 1'<field_delimiter_2>[1, null, 2]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>\N<field_delimiter_1>'Some string 3'<field_delimiter_2>[1, 2, null]<row_after_delimiter>
<result_after_delimiter>
$$)
```
```response
┌─name─────┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ column_1 │ Nullable(Float64)      │              │                    │         │                  │                │
│ column_2 │ Nullable(String)       │              │                    │         │                  │                │
│ column_3 │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

### Regexp {#regexp}

Templateと同様に、Regexp形式ではClickHouseが正規表現に従って行からすべてのカラム値を抽出し、その後、指定されたエスケープルールに従ってそれぞれの値のデータ型を推測します。

**例**

```sql
SET format_regexp = '^Line: value_1=(.+?), value_2=(.+?), value_3=(.+?)',
       format_regexp_escaping_rule = 'CSV'
       
DESC format(Regexp, $$Line: value_1=42, value_2="Some string 1", value_3="[1, NULL, 3]"
Line: value_1=2, value_2="Some string 2", value_3="[4, 5, NULL]"$$)
```
```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)        │              │                    │         │                  │                │
│ c2   │ Nullable(String)       │              │                    │         │                  │                │
│ c3   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

### テキストフォーマット用の設定 {#settings-for-text-formats}

#### input_format_max_rows_to_read_for_schema_inference/input_format_max_bytes_to_read_for_schema_inference

これらの設定は、スキーマ推測中に読み込むデータの量を制御します。
より多くの行/バイトを読み込むほど、スキーマ推測に費やされる時間が増えますが、型を正しく判別できる可能性が高まります（特にデータに多くのnullが含まれている場合）。

デフォルト値:
-   `input_format_max_rows_to_read_for_schema_inference` は `25000`。
-   `input_format_max_bytes_to_read_for_schema_inference` は `33554432` (32 Mb)。

#### column_names_for_schema_inference

明示的なカラム名がない形式でスキーマ推測に使用するカラム名のリスト。指定された名前はデフォルトの `c1,c2,c3,...` の代わりに使用されます。形式: `column1,column2,column3,...`。

**例**

```sql
DESC format(TSV, 'Hello, World!	42	[1, 2, 3]') settings column_names_for_schema_inference = 'str,int,arr'
```
```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ str  │ Nullable(String)       │              │                    │         │                  │                │
│ int  │ Nullable(Int64)        │              │                    │         │                  │                │
│ arr  │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

#### schema_inference_hints

自動的に判別された型の代わりにスキーマ推測で使用するカラム名と型のリスト。形式: 'column_name1 column_type1, column_name2 column_type2, ...'。
この設定は、自動的に判別できなかったカラムの型を指定するか、スキーマ最適化のために使用できます。

**例**

```sql
DESC format(JSONEachRow, '{"id" : 1, "age" : 25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}') SETTINGS schema_inference_hints = 'age LowCardinality(UInt8), status Nullable(String)', allow_suspicious_low_cardinality_types=1
```
```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ LowCardinality(UInt8)   │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ status  │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

#### schema_inference_make_columns_nullable

null可能性に関する情報がない形式のスキーマ推測で、推測された型を `Nullable` にするかを制御します。
この設定が有効な場合、すべての推測された型は `Nullable` になります。無効な場合、推測された型は決して `Nullable` にはなりません。`auto` に設定されている場合、スキーマ推測中に解析されたサンプルでカラムに `NULL` が含まれている場合、またはファイルメタデータにカラムのnull可能性に関する情報が含まれている場合のみ、推測された型は `Nullable` になります。

デフォルトで有効です。

**例**

```sql
SET schema_inference_make_columns_nullable = 1
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```
```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ status  │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
SET schema_inference_make_columns_nullable = 'auto';
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```
```response
┌─name────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Int64            │              │                    │         │                  │                │
│ age     │ Int64            │              │                    │         │                  │                │
│ name    │ String           │              │                    │         │                  │                │
│ status  │ Nullable(String) │              │                    │         │                  │                │
│ hobbies │ Array(String)    │              │                    │         │                  │                │
└─────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET schema_inference_make_columns_nullable = 0;
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```
```response

┌─name────┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Int64         │              │                    │         │                  │                │
│ age     │ Int64         │              │                    │         │                  │                │
│ name    │ String        │              │                    │         │                  │                │
│ status  │ String        │              │                    │         │                  │                │
│ hobbies │ Array(String) │              │                    │         │                  │                │
└─────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

#### input_format_try_infer_integers

有効にすると、ClickHouseはテキスト形式のスキーマ推測で整数を浮動小数点の代わりに推測しようとします。
サンプルデータのカラム内のすべての数値が整数であれば、結果の型は `Int64` になります。少なくとも1つの数値が浮動小数点である場合、結果の型は `Float64` になります。
サンプルデータが整数のみを含み、少なくとも1つの整数が正で `Int64` をオーバーフローする場合、ClickHouseは `UInt64` を推測します。

デフォルトで有効です。

**例**

```sql
SET input_format_try_infer_integers = 0
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2}
                         $$)
```
```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Float64) │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
SET input_format_try_infer_integers = 1
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2}
                         $$)
```
```response
┌─name───┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Int64) │              │                    │         │                  │                │
└────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 18446744073709551615}
                         $$)
```
```response
┌─name───┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(UInt64) │              │                    │         │                  │                │
└────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2.2}
                         $$)
```
```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Float64) │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

#### input_format_try_infer_datetimes

有効にすると、ClickHouseはテキスト形式のスキーマ推測で `DateTime` または `DateTime64` 型を文字列フィールドから推測しようとします。
サンプルデータ内のカラムからすべてのフィールドが日時として正常に解析された場合、結果の型は `DateTime` または（任意の日時が少数部を持っている場合）`DateTime64(9)` になります。
少なくとも1つのフィールドが日時として解析されなかった場合、結果の型は `String` になります。

デフォルトで有効です。

**例**

```sql
SET input_format_try_infer_datetimes = 0;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```
```response
┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(String) │              │                    │         │                  │                │
│ datetime64 │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
SET input_format_try_infer_datetimes = 1;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```
```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "unknown", "datetime64" : "unknown"}
                         $$)
```
```response
┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(String) │              │                    │         │                  │                │
│ datetime64 │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

#### input_format_try_infer_datetimes_only_datetime64

有効にすると、`input_format_try_infer_datetimes` が有効な場合、日時の値が小数部を含まない場合でもClickHouseは常に `DateTime64(9)` を推測します。

デフォルトでは無効です。

**例**

```sql
SET input_format_try_infer_datetimes = 1;
SET input_format_try_infer_datetimes_only_datetime64 = 1;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```

```text
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

注意: スキーマ推測中の日時の解析は、設定 [date_time_input_format](/docs/ja/operations/settings/settings-formats.md#date_time_input_format) に影響されます

#### input_format_try_infer_dates

有効にすると、ClickHouseはテキスト形式のスキーマ推測で `Date` 型を文字列フィールドから推測しようとします。
サンプルデータ内のカラムからすべてのフィールドが日付として正常に解析された場合、結果の型は `Date` になります。
少なくとも1つのフィールドが日付として解析されなかった場合、結果の型は `String` になります。

デフォルトで有効です。

**例**

```sql
SET input_format_try_infer_datetimes = 0, input_format_try_infer_dates = 0
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "2022-01-01"}
                         $$)
```
```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
SET input_format_try_infer_dates = 1
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "2022-01-01"}
                         $$)
```
```response
┌─name─┬─type───────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(Date) │              │                    │         │                  │                │
└──────┴────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
```sql
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "unknown"}
                         $$)
```
```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

#### input_format_try_infer_exponent_floats

有効にすると、ClickHouseはテキスト形式（JSONを除く）で指数形式の浮動小数点を推測しようとします（指数形式の数値は常に推測されます）。

デフォルトでは無効です。

**例**

```sql
SET input_format_try_infer_exponent_floats = 1;
DESC format(CSV,
$$1.1E10
2.3e-12
42E00
$$)
```
```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Float64) │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

## 自己記述型フォーマット {#self-describing-formats}

自己記述型フォーマットには、データそのものにデータの構造に関する情報が含まれています。
たとえば、説明を含むヘッダー、バイナリ型ツリー、またはテーブルのようなものがあります。
ClickHouseはこうした形式のファイルから自動的にスキーマを推測するため、タイプに関する情報を含む部分を読み込み、
ClickHouseテーブルのスキーマにそれを変換します。

### -WithNamesAndTypes サフィックスがあるフォーマット {#formats-with-names-and-types}

ClickHouseは、一部のテキストフォーマットにサフィックス -WithNamesAndTypes をサポートしています。このサフィックスは、データが実際のデータの前にカラム名と型を含む2つの追加行を持っていることを意味します。
このような形式のスキーマ推測中に、ClickHouseは最初の2行を読み、カラム名と型を抽出します。

**例**

```sql
DESC format(TSVWithNamesAndTypes,
$$num	str	arr
UInt8	String	Array(UInt8)
42	Hello, World!	[1,2,3]
$$)
```
```response
┌─name─┬─type─────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ num  │ UInt8        │              │                    │         │                  │                │
│ str  │ String       │              │                    │         │                  │                │
│ arr  │ Array(UInt8) │              │                    │         │                  │                │
└──────┴──────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

### メタデータを持つJSONフォーマット {#json-with-metadata}

いくつかのJSON入力形式 ([JSON](formats.md#json), [JSONCompact](formats.md#json-compact), [JSONColumnsWithMetadata](formats.md#jsoncolumnswithmetadata)) は、カラム名と型のメタデータを含んでいます。
このようなフォーマットのスキーマ推測では、ClickHouseはこのメタデータを読み込みます。

**例**
```sql
DESC format(JSON, $$
{
	"meta":
	[
		{
			"name": "num",
			"type": "UInt8"
		},
		{
			"name": "str",
			"type": "String"
		},
		{
			"name": "arr",
			"type": "Array(UInt8)"
		}
	],

	"data":
	[
		{
			"num": 42,
			"str": "Hello, World",
			"arr": [1,2,3]
		}
	],

	"rows": 1,

	"statistics":
	{
		"elapsed": 0.005723915,
		"rows_read": 1,
		"bytes_read": 1
	}
}
$$)
```
```response
┌─name─┬─type─────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ num  │ UInt8        │              │                    │         │                  │                │
│ str  │ String       │              │                    │         │                  │                │
│ arr  │ Array(UInt8) │              │                    │         │                  │                │
└──────┴──────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

### Avro {#avro}

Avro形式では、ClickHouseがデータからそのスキーマを読み込み、次の型のマッチングを使用してClickHouseスキーマに変換します。

| Avroデータ型                       | ClickHouseデータ型                                                           |
|------------------------------------|--------------------------------------------------------------------------------|
| `boolean`                          | [Bool](../sql-reference/data-types/boolean.md)                                 |
| `int`                              | [Int32](../sql-reference/data-types/int-uint.md)                               |
| `int (date)` \*                    | [Date32](../sql-reference/data-types/date32.md)                                |
| `long`                             | [Int64](../sql-reference/data-types/int-uint.md)                               |
| `float`                            | [Float32](../sql-reference/data-types/float.md)                                |
| `double`                           | [Float64](../sql-reference/data-types/float.md)                                |
| `bytes`, `string`                  | [String](../sql-reference/data-types/string.md)                                |
| `fixed`                            | [FixedString(N)](../sql-reference/data-types/fixedstring.md)                   |
| `enum`                             | [Enum](../sql-reference/data-types/enum.md)                                    |
| `array(T)`                         | [Array(T)](../sql-reference/data-types/array.md)                               |
| `union(null, T)`, `union(T, null)` | [Nullable(T)](../sql-reference/data-types/date.md)                             |
| `null`                             | [Nullable(Nothing)](../sql-reference/data-types/special-data-types/nothing.md) |
| `string (uuid)` \*                 | [UUID](../sql-reference/data-types/uuid.md)                                    |
| `binary (decimal)` \*              | [Decimal(P, S)](../sql-reference/data-types/decimal.md)                         |

\* [Avro論理型](https://avro.apache.org/docs/current/spec.html#Logical+Types)

その他のAvro型はサポートされていません。

### Parquet {#parquet}

Parquet形式では、ClickHouseがそのスキーマをデータから読み込み、次の型のマッチングを使用してClickHouseスキーマに変換します。

| Parquetデータ型            | ClickHouseデータ型                                    |
|------------------------------|---------------------------------------------------------|
| `BOOL`                       | [Bool](../sql-reference/data-types/boolean.md)          |
| `UINT8`                      | [UInt8](../sql-reference/data-types/int-uint.md)        |
| `INT8`                       | [Int8](../sql-reference/data-types/int-uint.md)         |
| `UINT16`                     | [UInt16](../sql-reference/data-types/int-uint.md)       |
| `INT16`                      | [Int16](../sql-reference/data-types/int-uint.md)        |
| `UINT32`                     | [UInt32](../sql-reference/data-types/int-uint.md)       |
| `INT32`                      | [Int32](../sql-reference/data-types/int-uint.md)        |
| `UINT64`                     | [UInt64](../sql-reference/data-types/int-uint.md)       |
| `INT64`                      | [Int64](../sql-reference/data-types/int-uint.md)        |
| `FLOAT`                      | [Float32](../sql-reference/data-types/float.md)         |
| `DOUBLE`                     | [Float64](../sql-reference/data-types/float.md)         |
| `DATE`                       | [Date32](../sql-reference/data-types/date32.md)         |
| `TIME (ms)`                  | [DateTime](../sql-reference/data-types/datetime.md)     |
| `TIMESTAMP`, `TIME (us, ns)` | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `STRING`, `BINARY`           | [String](../sql-reference/data-types/string.md)         |
| `DECIMAL`                    | [Decimal](../sql-reference/data-types/decimal.md)       |
| `LIST`                       | [Array](../sql-reference/data-types/array.md)           |
| `STRUCT`                     | [Tuple](../sql-reference/data-types/tuple.md)           |
| `MAP`                        | [Map](../sql-reference/data-types/map.md)               |

その他のParquet型はサポートされていません。デフォルトでは、すべての推測された型は `Nullable` 内にありますが、設定 `schema_inference_make_columns_nullable` を使用して変更できます。

### Arrow {#arrow}

Arrow形式では、ClickHouseがそのスキーマをデータから読み込み、次の型のマッチングを使用してClickHouseスキーマに変換します。

| Arrowデータ型                 | ClickHouseデータ型                                    |
|---------------------------------|---------------------------------------------------------|
| `BOOL`                          | [Bool](../sql-reference/data-types/boolean.md)          |
| `UINT8`                         | [UInt8](../sql-reference/data-types/int-uint.md)        |
| `INT8`                          | [Int8](../sql-reference/data-types/int-uint.md)         |
| `UINT16`                        | [UInt16](../sql-reference/data-types/int-uint.md)       |
| `INT16`                         | [Int16](../sql-reference/data-types/int-uint.md)        |
| `UINT32`                        | [UInt32](../sql-reference/data-types/int-uint.md)       |
| `INT32`                         | [Int32](../sql-reference/data-types/int-uint.md)        |
| `UINT64`                        | [UInt64](../sql-reference/data-types/int-uint.md)       |
| `INT64`                         | [Int64](../sql-reference/data-types/int-uint.md)        |
| `FLOAT`, `HALF_FLOAT`           | [Float32](../sql-reference/data-types/float.md)         |
| `DOUBLE`                        | [Float64](../sql-reference/data-types/float.md)         |
| `DATE32`                        | [Date32](../sql-reference/data-types/date32.md)         |
| `DATE64`                        | [DateTime](../sql-reference/data-types/datetime.md)     |
| `TIMESTAMP`, `TIME32`, `TIME64` | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `STRING`, `BINARY`              | [String](../sql-reference/data-types/string.md)         |
| `DECIMAL128`, `DECIMAL256`      | [Decimal](../sql-reference/data-types/decimal.md)       |
| `LIST`                          | [Array](../sql-reference/data-types/array.md)           |
| `STRUCT`                        | [Tuple](../sql-reference/data-types/tuple.md)           |
| `MAP`                           | [Map](../sql-reference/data-types/map.md)               |

その他のArrow型はサポートされていません。デフォルトでは、すべての推測された型は `Nullable` 内にありますが、設定 `schema_inference_make_columns_nullable` を使用して変更できます。

### ORC {#orc}

ORC形式では、ClickHouseがそのスキーマをデータから読み込み、次の型のマッチングを使用してClickHouseスキーマに変換します。

| ORCデータ型                        | ClickHouseデータ型                                    |
|--------------------------------------|---------------------------------------------------------|
| `Boolean`                            | [Bool](../sql-reference/data-types/boolean.md)          |
| `Tinyint`                            | [Int8](../sql-reference/data-types/int-uint.md)         |
| `Smallint`                           | [Int16](../sql-reference/data-types/int-uint.md)        |
| `Int`                                | [Int32](../sql-reference/data-types/int-uint.md)        |
| `Bigint`                             | [Int64](../sql-reference/data-types/int-uint.md)        |
| `Float`                              | [Float32](../sql-reference/data-types/float.md)         |
| `Double`                             | [Float64](../sql-reference/data-types/float.md)         |
| `Date`                               | [Date32](../sql-reference/data-types/date32.md)         |
| `Timestamp`                          | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `String`, `Char`, `Varchar`,`BINARY` | [String](../sql-reference/data-types/string.md)         |
| `Decimal`                            | [Decimal](../sql-reference/data-types/decimal.md)       |
| `List`                               | [Array](../sql-reference/data-types/array.md)           |
| `Struct`                             | [Tuple](../sql-reference/data-types/tuple.md)           |
| `Map`                                | [Map](../sql-reference/data-types/map.md)               |

その他のORC型はサポートされていません。デフォルトでは、すべての推測された型は `Nullable` 内にありますが、設定 `schema_inference_make_columns_nullable` を使用して変更できます。

### Native {#native}

ネイティブ形式はClickHouse内部で使用され、データにスキーマを含んでいます。
スキーマ推測では、ClickHouseはデータからスキーマを変換なしに読み取ります。

## 外部スキーマを持つフォーマット {#formats-with-external-schema}

そのようなフォーマットでは、特定のスキーマ言語での外部ファイルでデータを記述するスキーマが必要です。
ClickHouseは自動的にこうした形式のファイルからスキーマを推測するために、外部スキーマを別ファイルから読み取り、それをClickHouseテーブルスキーマに変換します。

### Protobuf {#protobuf}

Protobuf形式のスキーマ推測では、次の型のマッチングを使用します:

| Protobufデータ型            | ClickHouseデータ型                              |
|-------------------------------|---------------------------------------------------|
| `bool`                        | [UInt8](../sql-reference/data-types/int-uint.md)  |
| `float`                       | [Float32](../sql-reference/data-types/float.md)   |
| `double`                      | [Float64](../sql-reference/data-types/float.md)   |
| `int32`, `sint32`, `sfixed32` | [Int32](../sql-reference/data-types/int-uint.md)  |
| `int64`, `sint64`, `sfixed64` | [Int64](../sql-reference/data-types/int-uint.md)  |
| `uint32`, `fixed32`           | [UInt32](../sql-reference/data-types/int-uint.md) |
| `uint64`, `fixed64`           | [UInt64](../sql-reference/data-types/int-uint.md) |
| `string`, `bytes`             | [String](../sql-reference/data-types/string.md)   |
| `enum`                        | [Enum](../sql-reference/data-types/enum.md)       |
| `repeated T`                  | [Array(T)](../sql-reference/data-types/array.md)  |
| `message`, `group`            | [Tuple](../sql-reference/data-types/tuple.md)     |

### CapnProto {#capnproto}

CapnProto形式のスキーマ推測では、次の型のマッチングを使用します:

| CapnProtoデータ型                | ClickHouseデータ型                                   |
|------------------------------------|--------------------------------------------------------|
| `Bool`                             | [UInt8](../sql-reference/data-types/int-uint.md)       |
| `Int8`                             | [Int8](../sql-reference/data-types/int-uint.md)        |
| `UInt8`                            | [UInt8](../sql-reference/data-types/int-uint.md)       |
| `Int16`                            | [Int16](../sql-reference/data-types/int-uint.md)       |
| `UInt16`                           | [UInt16](../sql-reference/data-types/int-uint.md)      |
| `Int32`                            | [Int32](../sql-reference/data-types/int-uint.md)       |
| `UInt32`                           | [UInt32](../sql-reference/data-types/int-uint.md)      |
| `Int64`                            | [Int64](../sql-reference/data-types/int-uint.md)       |
| `UInt64`                           | [UInt64](../sql-reference/data-types/int-uint.md)      |
| `Float32`                          | [Float32](../sql-reference/data-types/float.md)        |
| `Float64`                          | [Float64](../sql-reference/data-types/float.md)        |
| `Text`, `Data`                     | [String](../sql-reference/data-types/string.md)        |
| `enum`                             | [Enum](../sql-reference/data-types/enum.md)            |
| `List`                             | [Array](../sql-reference/data-types/array.md)          |
| `struct`                           | [Tuple](../sql-reference/data-types/tuple.md)          |
| `union(T, Void)`, `union(Void, T)` | [Nullable(T)](../sql-reference/data-types/nullable.md) |

## 強い型のバイナリ形式 {#strong-typed-binary-formats}

このような形式では、各シリアライズされた値にその型に関する情報（および場合によってはそれに関する名前）が含まれていますが、テーブル全体に関する情報はありません。
ClickHouseはこのような形式のスキーマ推測を行うため、データを行ごとに読み込み（`input_format_max_rows_to_read_for_schema_inference` 行または `input_format_max_bytes_to_read_for_schema_inference` バイトまで）、
データから各値に対する型（および場合によっては名前）を抽出し、これらの型をClickHouseの型に変換します。

### MsgPack {#msgpack}

MsgPack形式では、行間の区切りがないため、この形式のスキーマ推測を行う場合は、テーブルのカラム数を設定 `input_format_msgpack_number_of_columns` を使用して指定してください。ClickHouseは次の型のマッチングを使用します:

| MessagePackデータ型 (`INSERT`)                                   | ClickHouseデータ型                                      |
|--------------------------------------------------------------------|-----------------------------------------------------------|
| `int N`, `uint N`, `negative fixint`, `positive fixint`            | [Int64](../sql-reference/data-types/int-uint.md)          |
| `bool`                                                             | [UInt8](../sql-reference/data-types/int-uint.md)          |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [String](../sql-reference/data-types/string.md)           |
| `float 32`                                                         | [Float32](../sql-reference/data-types/float.md)           |
| `float 64`                                                         | [Float64](../sql-reference/data-types/float.md)           |
| `uint 16`                                                          | [Date](../sql-reference/data-types/date.md)               |
| `uint 32`                                                          | [DateTime](../sql-reference/data-types/datetime.md)       |
| `uint 64`                                                          | [DateTime64](../sql-reference/data-types/datetime.md)     |
| `fixarray`, `array 16`, `array 32`                                 | [Array](../sql-reference/data-types/array.md)             |
| `fixmap`, `map 16`, `map 32`                                       | [Map](../sql-reference/data-types/map.md)                 |

デフォルトでは、すべての推測された型は `Nullable` 内にありますが、設定 `schema_inference_make_columns_nullable` を使用して変更できます。
### BSONEachRow {#bsoneachrow}

BSONEachRowでは、データの各行がBSONドキュメントとして表示されます。スキーマ推論において、ClickHouseはBSONドキュメントを一つずつ読み取り、データから値、名前、タイプを抽出して、次のタイプマッチを使用してこれらのタイプをClickHouseタイプに変換します：

| BSON Type                                                                                     | ClickHouse type                                                                                                             |
|-----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `\x08` boolean                                                                                | [Bool](../sql-reference/data-types/boolean.md)                                                                              |
| `\x10` int32                                                                                  | [Int32](../sql-reference/data-types/int-uint.md)                                                                            |
| `\x12` int64                                                                                  | [Int64](../sql-reference/data-types/int-uint.md)                                                                            |
| `\x01` double                                                                                 | [Float64](../sql-reference/data-types/float.md)                                                                             |
| `\x09` datetime                                                                               | [DateTime64](../sql-reference/data-types/datetime64.md)                                                                     |
| `\x05` binary with`\x00` binary subtype, `\x02` string, `\x0E` symbol, `\x0D` JavaScript code | [String](../sql-reference/data-types/string.md)                                                                             |
| `\x07` ObjectId,                                                                              | [FixedString(12)](../sql-reference/data-types/fixedstring.md)                                                               |
| `\x05` binary with `\x04` uuid subtype, size = 16                                             | [UUID](../sql-reference/data-types/uuid.md)                                                                                 |
| `\x04` array                                                                                  | [Array](../sql-reference/data-types/array.md)/[Tuple](../sql-reference/data-types/tuple.md) (if nested types are different) |
| `\x03` document                                                                               | [Named Tuple](../sql-reference/data-types/tuple.md)/[Map](../sql-reference/data-types/map.md) (with String keys)            |

デフォルトでは、すべての推論されたタイプは`Nullable`内にありますが、設定`schema_inference_make_columns_nullable`を使用して変更することができます。

## 定数スキーマを持つフォーマット {#formats-with-constant-schema}

このようなフォーマットのデータは常に同じスキーマを持ちます。

### LineAsString {#line-as-string}

このフォーマットでは、ClickHouseはデータから行全体を`String`データ型の単一のカラムとして読み取ります。このフォーマットで推論されたタイプは常に`String`であり、カラム名は`line`です。

**例**

```sql
DESC format(LineAsString, 'Hello\nworld!')
```
```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

### JSONAsString {#json-as-string}

このフォーマットでは、ClickHouseはデータからJSONオブジェクト全体を`String`データ型の単一のカラムとして読み取ります。このフォーマットで推論されたタイプは常に`String`であり、カラム名は`json`です。

**例**

```sql
DESC format(JSONAsString, '{"x" : 42, "y" : "Hello, World!"}')
```
```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ json │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

### JSONAsObject {#json-as-object}

このフォーマットでは、ClickHouseはデータからJSONオブジェクト全体を`Object('json')`データ型の単一のカラムとして読み取ります。推論されたタイプは常に`String`であり、カラム名は`json`です。

注意: このフォーマットは`allow_experimental_object_type`が有効になっている場合のみ動作します。

**例**

```sql
DESC format(JSONAsString, '{"x" : 42, "y" : "Hello, World!"}') SETTINGS allow_experimental_object_type=1
```
```response
┌─name─┬─type───────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ json │ Object('json') │              │                    │         │                  │                │
└──────┴────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

## スキーマ推論モード {#schema-inference-modes}

データファイルのセットからのスキーマ推論は2つの異なるモードで動作します：`default`および`union`。このモードは設定`schema_inference_mode`によって制御されます。

### デフォルトモード {#default-schema-inference-mode}

デフォルトモードでは、ClickHouseはすべてのファイルが同じスキーマを持つと仮定し、ファイルを一つずつ読み取り、成功するまでスキーマを推論しようとします。

例:

次の内容を持つ3つのファイル`data1.jsonl`、`data2.jsonl`、`data3.jsonl`があるとします：

`data1.jsonl`:
```json
{"field1" :  1, "field2" :  null}
{"field1" :  2, "field2" :  null}
{"field1" :  3, "field2" :  null}
```

`data2.jsonl`:
```json
{"field1" :  4, "field2" :  "Data4"}
{"field1" :  5, "field2" :  "Data5"}
{"field1" :  6, "field2" :  "Data5"}
```

`data3.jsonl`:
```json
{"field1" :  7, "field2" :  "Data7", "field3" :  [1, 2, 3]}
{"field1" :  8, "field2" :  "Data8", "field3" :  [4, 5, 6]}
{"field1" :  9, "field2" :  "Data9", "field3" :  [7, 8, 9]}
```

これら3つのファイルにスキーマ推論を試みましょう：
```sql
:) DESCRIBE file('data{1,2,3}.jsonl') SETTINGS schema_inference_mode='default'
```

結果：
```text
┌─name───┬─type─────────────┐
│ field1 │ Nullable(Int64)  │
│ field2 │ Nullable(String) │
└────────┴──────────────────┘
```

見てわかるように、ファイル`data3.jsonl`の`field3`はありません。これは、ClickHouseが最初にファイル`data1.jsonl`からスキーマを推論しようとし、フィールド`field2`のためにnullのみで失敗し、その後`data2.jsonl`からスキーマを推論して成功したため、ファイル`data3.jsonl`からのデータが読み取られなかったためです。

### ユニオンモード {#default-schema-inference-mode}

ユニオンモードでは、ClickHouseはファイルが異なるスキーマを持つ可能性があると仮定し、それぞれのファイルのスキーマを推論し、それらを共通のスキーマに結合します。

次の内容を持つ3つのファイル`data1.jsonl`、`data2.jsonl`、`data3.jsonl`があるとします：

`data1.jsonl`:
```json
{"field1" :  1}
{"field1" :  2}
{"field1" :  3}
```

`data2.jsonl`:
```json
{"field2" :  "Data4"}
{"field2" :  "Data5"}
{"field2" :  "Data5"}
```

`data3.jsonl`:
```json
{"field3" :  [1, 2, 3]}
{"field3" :  [4, 5, 6]}
{"field3" :  [7, 8, 9]}
```

これら3つのファイルにスキーマ推論を試みましょう：
```sql
:) DESCRIBE file('data{1,2,3}.jsonl') SETTINGS schema_inference_mode='union'
```

結果：
```text
┌─name───┬─type───────────────────┐
│ field1 │ Nullable(Int64)        │
│ field2 │ Nullable(String)       │
│ field3 │ Array(Nullable(Int64)) │
└────────┴────────────────────────┘
```

見てわかるように、すべてのファイルからすべてのフィールドが含まれています。

注意:
- 一部のファイルには、結果のスキーマから欠けているカラムが含まれていない場合があるため、ユニオンモードは一部のカラムの読み取りをサポートするフォーマット（JSONEachRow、Parquet、TSVWithNamesなど）のみでサポートされています。他のフォーマット（CSV、TSV、JSONCompactEachRowなど）では動作しません。
- ClickHouseがファイルの一つからスキーマを推論できない場合、例外がスローされます。
- 多くのファイルがある場合、すべてからスキーマを読み取るのに多くの時間がかかることがあります。

## 自動フォーマット検出 {#automatic-format-detection}

データフォーマットが指定されず、ファイル拡張子では決定できない場合、ClickHouseはその内容によってファイルフォーマットを検出しようとします。

**例:**

次のコンテンツを持つ`data`があるとします：
```
"a","b"
1,"Data1"
2,"Data2"
3,"Data3"
```

フォーマットや構造を指定せずにこのファイルを調べたりクエリを行ったりできます：
```sql
:) desc file(data);
```

```text
┌─name─┬─type─────────────┐
│ a    │ Nullable(Int64)  │
│ b    │ Nullable(String) │
└──────┴──────────────────┘
```

```sql
:) select * from file(data);
```

```text
┌─a─┬─b─────┐
│ 1 │ Data1 │
│ 2 │ Data2 │
│ 3 │ Data3 │
└───┴───────┘
```

:::note
ClickHouseは一部のフォーマットのみを検出可能であり、検出には時間がかかるため、フォーマットを明示的に指定することが常に望ましいです。
:::
