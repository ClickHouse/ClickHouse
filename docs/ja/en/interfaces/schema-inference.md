---
description: 'ClickHouse における入力データからの自動スキーマ推論を説明するページ'
sidebar_label: 'スキーマ推論'
slug: /interfaces/schema-inference
title: '入力データからの自動スキーマ推論'
doc_type: 'reference'
---

ClickHouse は、サポートされているほぼすべての[入力フォーマット](formats.md)について、入力データの構造を自動的に判断できます。
このドキュメントでは、スキーマ推論が使用される場面、さまざまな入力フォーマットでの動作、およびその挙動を制御する設定について説明します。

<div id="usage">
  ## 使用法
</div>

スキーマ推論は、ClickHouse が特定のデータフォーマットのデータを読み込む際に、その構造が不明な場合に使用されます。

<div id="table-functions-file-s3-url-hdfs-azureblobstorage">
  ## テーブル関数 [file](../sql-reference/table-functions/file.md), [s3](../sql-reference/table-functions/s3.md), [url](../sql-reference/table-functions/url.md), [hdfs](../sql-reference/table-functions/hdfs.md), [azureBlobStorage](../sql-reference/table-functions/azureBlobStorage.md).
</div>

これらのテーブル関数には、入力データの構造を指定する省略可能な引数 `structure` があります。この引数を指定しないか、`auto` に設定すると、構造はデータから推論されます。

**例:**

`user_files` ディレクトリに、JSONEachRow フォーマットの `hobbies.jsonl` というファイルがあり、その内容が次のとおりだとします。

```json
{"id" :  1, "age" :  25, "name" :  "Josh", "hobbies" :  ["football", "cooking", "music"]}
{"id" :  2, "age" :  19, "name" :  "Alan", "hobbies" :  ["tennis", "art"]}
{"id" :  3, "age" :  32, "name" :  "Lana", "hobbies" :  ["fitness", "reading", "shopping"]}
{"id" :  4, "age" :  47, "name" :  "Brayan", "hobbies" :  ["movies", "skydiving"]}
```

ClickHouse は、このデータの構造を指定しなくても読み込めます。

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

注: フォーマット `JSONEachRow` は、ファイル拡張子 `.jsonl` から自動的に判定されました。

`DESCRIBE` クエリを使うと、自動判定された構造を確認できます:

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

<div id="table-engines-file-s3-url-hdfs-azureblobstorage">
  ## テーブルエンジン [File](../engines/table-engines/special/file.md), [S3](../engines/table-engines/integrations/s3.md), [URL](../engines/table-engines/special/url.md), [HDFS](../engines/table-engines/integrations/hdfs.md), [azureBlobStorage](../engines/table-engines/integrations/azureBlobStorage.md)
</div>

`CREATE TABLE` クエリでカラム一覧が指定されていない場合、テーブルの構造はデータから自動的に推論されます。

**例:**

`hobbies.jsonl` ファイルを使ってみましょう。このファイルのデータを使用して、`File` エンジンのテーブルを作成できます:

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

<div id="clickhouse-local">
  ## clickhouse-local
</div>

`clickhouse-local` には、入力データの構造を指定する省略可能なパラメータ `-S/--structure` があります。このパラメータを指定しない場合、または `auto` に設定した場合は、構造がデータから推論されます。

**例:**

`hobbies.jsonl` ファイルを使ってみましょう。このファイルのデータには、`clickhouse-local` を使ってクエリできます。

```shell
clickhouse-local --file='hobbies.jsonl' --table='hobbies' --query='DESCRIBE TABLE hobbies'
```

```response
id    Nullable(Int64)
age    Nullable(Int64)
name    Nullable(String)
hobbies    Array(Nullable(String))
```

```shell
clickhouse-local --file='hobbies.jsonl' --table='hobbies' --query='SELECT * FROM hobbies'
```

```response
1    25    Josh    ['football','cooking','music']
2    19    Alan    ['tennis','art']
3    32    Lana    ['fitness','reading','shopping']
4    47    Brayan    ['movies','skydiving']
```

<div id="using-structure-from-insertion-table">
  ## 挿入先テーブルの構造を使用する
</div>

テーブル関数 `file/s3/url/hdfs` を使ってテーブルにデータを挿入する際、
データから構造を抽出する代わりに、挿入先テーブルの構造を使用するオプションがあります。
スキーマ推論には時間がかかることがあるため、これにより挿入性能を向上させることができます。また、テーブル側のスキーマが最適化されている場合にも有効で、
型変換は行われません。

この動作を制御する特別な設定 [use&#95;structure&#95;from&#95;insertion&#95;table&#95;in&#95;table&#95;functions](/ja/operations/settings/settings.md/#use_structure_from_insertion_table_in_table_functions)
があります。設定可能な値は 3 つあります：

* 0 - テーブル関数はデータから構造を抽出します。
* 1 - テーブル関数は挿入先テーブルの構造を使用します。
* 2 - ClickHouse は、挿入先テーブルの構造を使用できるか、スキーマ推論を使う必要があるかを自動的に判断します。デフォルト値です。

**例 1：**

次の構造でテーブル `hobbies1` を作成します：

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

次に、`hobbies.jsonl` ファイルからデータを挿入します:

```sql
INSERT INTO hobbies1 SELECT * FROM file(hobbies.jsonl)
```

この場合、ファイル内のすべてのカラムがそのままテーブルに挿入されるため、ClickHouse はスキーマ推論ではなく、挿入先テーブルの構造を使用します。

**例 2:**

次の構造でテーブル `hobbies2` を作成します。

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

次に、ファイル `hobbies.jsonl` からデータを挿入します:

```sql
INSERT INTO hobbies2 SELECT id, age, hobbies FROM file(hobbies.jsonl)
```

この場合、`SELECT` クエリ内のすべてのカラムがテーブルに存在するため、ClickHouse は挿入先テーブルの構造を使用します。
これは、JSONEachRow、TSKV、Parquet など、カラムの一部だけを読み取れる入力フォーマットでのみ機能する点に注意してください (したがって、たとえば TSV フォーマットでは機能しません) 。

**Example 3:**

次の構造でテーブル `hobbies3` を作成しましょう:

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

次に、`hobbies.jsonl` ファイルからデータを挿入します:

```sql
INSERT INTO hobbies3 SELECT id, age, hobbies FROM file(hobbies.jsonl)
```

この場合、`SELECT`クエリではカラム `id` を使用していますが、テーブルにはこのカラムが存在しません (`identifier` という名前のカラムはあります) 。
そのため、ClickHouse は挿入先テーブルの構造を利用できず、スキーマ推論が使用されます。

**例 4:**

次の構造でテーブル `hobbies4` を作成します。

```sql
CREATE TABLE hobbies4
(
  `id` UInt64,
  `any_hobby` Nullable(String)
)
  ENGINE = MergeTree
ORDER BY id;
```

次に、`hobbies.jsonl` ファイルからデータを挿入します:

```sql
INSERT INTO hobbies4 SELECT id, empty(hobbies) ? NULL : hobbies[1] FROM file(hobbies.jsonl)
```

この場合、`SELECT`クエリ内でカラム `hobbies` に対してテーブルへ挿入するための操作がいくつか行われているため、ClickHouse は挿入先テーブルの構造を利用できず、スキーマ推論が使われます。

<div id="schema-inference-cache">
  ## スキーマ推論 cache
</div>

ほとんどの入力フォーマットでは、スキーマ推論のためにデータの一部を読み取ってその構造を判断する必要があり、この処理には時間がかかることがあります。
ClickHouseが同じファイルからデータを読み取るたびに毎回同じスキーマを推論しないよう、推定したスキーマは cache され、同じファイルに再度アクセスした際には ClickHouse は cache 内のスキーマを使用します。

この cache を制御する特別な設定があります。

* `schema_inference_cache_max_elements_for_{file/s3/hdfs/url/azure}` - 対応するテーブル関数ごとに cache されるスキーマの最大数です。デフォルト値は `4096` です。これらの設定は server config で設定する必要があります。
* `schema_inference_use_cache_for_{file,s3,hdfs,url,azure}` - スキーマ推論で cache を使用するかどうかをオン/オフできます。これらの設定はクエリで使用できます。

ファイルのスキーマは、データの変更やフォーマット設定の変更によって変わることがあります。
このため、スキーマ推論 cache では、ファイルソース、format name、使用されるフォーマット設定、およびファイルの最終更新時刻によってスキーマを識別します。

注: `url` テーブル関数で url 経由でアクセスする一部のファイルには、最終更新時刻の情報が含まれていない場合があります。そのため、このケース向けの特別な設定
`schema_inference_cache_require_modification_time_for_url` があります。この設定を無効にすると、そのようなファイルでは最終更新時刻がなくても cache 内のスキーマを使用できます。

現在 cache 内にあるすべてのスキーマを確認できるシステムテーブル [schema&#95;inference&#95;cache](../operations/system-tables/schema_inference_cache.md) と、`SYSTEM CLEAR SCHEMA CACHE [FOR File/S3/URL/HDFS]`
というシステムクエリもあります。
これにより、すべてのソース、または特定のソースのスキーマ cache を消去できます。

**例:**

S3 上のサンプル dataset `github-2022.ndjson.gz` の構造を推論し、スキーマ推論 cache がどのように機能するかを見てみましょう。

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
```

```response
┌─name───────┬─type─────────────────────────────────────────┐
│ type       │ Nullable(String)                             │
│ actor      │ Tuple(                                      ↴│
│            │↳    avatar_url Nullable(String),            ↴│
│            │↳    display_login Nullable(String),         ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    login Nullable(String),                 ↴│
│            │↳    url Nullable(String))                    │
│ repo       │ Tuple(                                      ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    name Nullable(String),                  ↴│
│            │↳    url Nullable(String))                    │
│ created_at │ Nullable(String)                             │
│ payload    │ Tuple(                                      ↴│
│            │↳    action Nullable(String),                ↴│
│            │↳    distinct_size Nullable(Int64),          ↴│
│            │↳    pull_request Tuple(                     ↴│
│            │↳        author_association Nullable(String),↴│
│            │↳        base Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        head Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        number Nullable(Int64),             ↴│
│            │↳        state Nullable(String),             ↴│
│            │↳        title Nullable(String),             ↴│
│            │↳        updated_at Nullable(String),        ↴│
│            │↳        user Tuple(                         ↴│
│            │↳            login Nullable(String))),       ↴│
│            │↳    ref Nullable(String),                   ↴│
│            │↳    ref_type Nullable(String),              ↴│
│            │↳    size Nullable(Int64))                    │
└────────────┴──────────────────────────────────────────────┘
5 rows in set. Elapsed: 0.601 sec.
```

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
```

```response
┌─name───────┬─type─────────────────────────────────────────┐
│ type       │ Nullable(String)                             │
│ actor      │ Tuple(                                      ↴│
│            │↳    avatar_url Nullable(String),            ↴│
│            │↳    display_login Nullable(String),         ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    login Nullable(String),                 ↴│
│            │↳    url Nullable(String))                    │
│ repo       │ Tuple(                                      ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    name Nullable(String),                  ↴│
│            │↳    url Nullable(String))                    │
│ created_at │ Nullable(String)                             │
│ payload    │ Tuple(                                      ↴│
│            │↳    action Nullable(String),                ↴│
│            │↳    distinct_size Nullable(Int64),          ↴│
│            │↳    pull_request Tuple(                     ↴│
│            │↳        author_association Nullable(String),↴│
│            │↳        base Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        head Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        number Nullable(Int64),             ↴│
│            │↳        state Nullable(String),             ↴│
│            │↳        title Nullable(String),             ↴│
│            │↳        updated_at Nullable(String),        ↴│
│            │↳        user Tuple(                         ↴│
│            │↳            login Nullable(String))),       ↴│
│            │↳    ref Nullable(String),                   ↴│
│            │↳    ref_type Nullable(String),              ↴│
│            │↳    size Nullable(Int64))                    │
└────────────┴──────────────────────────────────────────────┘

5 rows in set. Elapsed: 0.059 sec.
```

ご覧のとおり、2 回目のクエリはほぼ瞬時に成功しました。

では、推定したスキーマに影響する可能性のある設定をいくつか変更してみましょう。

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
SETTINGS input_format_json_try_infer_named_tuples_from_objects=0, input_format_json_read_objects_as_strings = 1

┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ type       │ Nullable(String) │              │                    │         │                  │                │
│ actor      │ Nullable(String) │              │                    │         │                  │                │
│ repo       │ Nullable(String) │              │                    │         │                  │                │
│ created_at │ Nullable(String) │              │                    │         │                  │                │
│ payload    │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

5 rows in set. Elapsed: 0.611 sec
```

ご覧のとおり、推定したスキーマに影響する可能性のある設定が変更されたため、同じファイルでも cache 内のスキーマは使用されませんでした。

`system.schema_inference_cache` テーブルの内容を確認してみましょう:

```sql
SELECT schema, format, source FROM system.schema_inference_cache WHERE storage='S3'
```

```response
┌─schema──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─format─┬─source───────────────────────────────────────────────────────────────────────────────────────────────────┐
│ type Nullable(String), actor Tuple(avatar_url Nullable(String), display_login Nullable(String), id Nullable(Int64), login Nullable(String), url Nullable(String)), repo Tuple(id Nullable(Int64), name Nullable(String), url Nullable(String)), created_at Nullable(String), payload Tuple(action Nullable(String), distinct_size Nullable(Int64), pull_request Tuple(author_association Nullable(String), base Tuple(ref Nullable(String), sha Nullable(String)), head Tuple(ref Nullable(String), sha Nullable(String)), number Nullable(Int64), state Nullable(String), title Nullable(String), updated_at Nullable(String), user Tuple(login Nullable(String))), ref Nullable(String), ref_type Nullable(String), size Nullable(Int64)) │ NDJSON │ datasets-documentation.s3.eu-west-3.amazonaws.com443/datasets-documentation/github/github-2022.ndjson.gz │
│ type Nullable(String), actor Nullable(String), repo Nullable(String), created_at Nullable(String), payload Nullable(String)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 │ NDJSON │ datasets-documentation.s3.eu-west-3.amazonaws.com443/datasets-documentation/github/github-2022.ndjson.gz │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

ご覧のとおり、同じファイルに対して 2 つの異なるスキーマがあることがわかります。

システムクエリを使って、スキーマキャッシュをクリアできます。

```sql
SYSTEM CLEAR SCHEMA CACHE FOR S3
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

<div id="text-formats">
  ## テキストフォーマット
</div>

テキストフォーマットでは、ClickHouse はデータを行ごとに読み取り、フォーマットに従ってカラムの値を抽出したうえで、
いくつかの再帰的なパーサーやヒューリスティクスを使用して各値の型を判定します。スキーマ推論でデータから読み取る行数とバイト数の上限は、
設定 `input_format_max_rows_to_read_for_schema_inference` (デフォルトは 25000) および `input_format_max_bytes_to_read_for_schema_inference` (デフォルトは 32Mb) で制御されます。
デフォルトでは、推論されたすべての型は [Nullable](../sql-reference/data-types/nullable.md) ですが、`schema_inference_make_columns_nullable` を設定することで変更できます ([設定](#settings-for-text-formats) セクションの例を参照してください) 。

<div id="json-formats">
  ### JSON フォーマット
</div>

JSON フォーマットでは、ClickHouse は JSON 仕様に従って値を解析し、最も適切なデータ型を見つけようとします。

ここでは、動作の仕組み、推論できる型、およびJSON フォーマットで使用できる具体的な設定について説明します。

**例**

以降の例では、[format](../sql-reference/table-functions/format.md) テーブル関数を使用します。

整数、Float、Bool、String:

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

Date、DateTime 型:

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

Arrays:

```sql
DESC format(JSONEachRow, '{"arr" : [1, 2, 3], "nested_arrays" : [[1, 2, 3], [4, 5, 6], []]}')
```

```response
┌─name──────────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr           │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ nested_arrays │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrayに`null`が含まれている場合、ClickHouseは他のArray要素の型を使用します：

```sql
DESC format(JSONEachRow, '{"arr" : [null, 42, null]}')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

配列に異なる型の値が含まれており、設定 `input_format_json_infer_array_of_dynamic_from_array_of_different_types` が有効な場合 (デフォルトで有効) 、型は `Array(Dynamic)` になります：

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types=1;
DESC format(JSONEachRow, '{"arr" : [42, "hello", [1, 2, 3]]}');
```

```response
┌─name─┬─type───────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Dynamic) │              │                    │         │                  │                │
└──────┴────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

名前付きタプル:

設定 `input_format_json_try_infer_named_tuples_from_objects` が有効になっている場合、スキーマ推論時に ClickHouse は JSON オブジェクトから名前付き Tuple を推論しようとします。
生成される名前付き Tuple には、サンプルデータ内の対応するすべての JSON オブジェクトに含まれる全要素が含まれます。

```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

```response
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

名前のない Tuple:

設定 `input_format_json_infer_array_of_dynamic_from_array_of_different_types` が無効になっている場合、異なる型の要素を持つ Array は、JSON フォーマットでは名前のない Tuple として扱われます。

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types = 0;
DESC format(JSONEachRow, '{"tuple" : [1, "Hello, World!", [1, 2, 3]]}')
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ tuple │ Tuple(Nullable(Int64), Nullable(String), Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

一部の値が`null`または空の場合は、他の行の対応する値の型を使用します。

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types=0;
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

Map型:

JSON では、同じ型の値を持つオブジェクトを Map型として読み取ることができます。
注: これは、設定 `input_format_json_read_objects_as_strings` と `input_format_json_try_infer_named_tuples_from_objects` が無効になっている場合にのみ機能します。

```sql
SET input_format_json_read_objects_as_strings = 0, input_format_json_try_infer_named_tuples_from_objects = 0;
DESC format(JSONEachRow, '{"map" : {"key1" : 42, "key2" : 24, "key3" : 4}}')
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ map  │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Nested 複合データ型:

```sql
DESC format(JSONEachRow, '{"value" : [[[42, 24], []], {"key1" : 42, "key2" : 24}]}')
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Tuple(Array(Array(Nullable(String))), Tuple(key1 Nullable(Int64), key2 Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ClickHouse が一部のキーの型を判定できない場合 (データに null、空のオブジェクト、空の配列しか含まれていない場合など) は、設定 `input_format_json_infer_incomplete_types_as_strings` が有効であれば型 `String` が使用され、そうでない場合は例外がスローされます:

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

<div id="json-settings">
  #### JSON設定
</div>

<div id="input_format_json_try_infer_numbers_from_strings">
  ##### input_format_json_try_infer_numbers_from_strings
</div>

この設定を有効にすると、文字列の値から数値を推論できるようになります。

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

<div id="input_format_json_try_infer_named_tuples_from_objects">
  ##### input_format_json_try_infer_named_tuples_from_objects
</div>

この設定を有効にすると、JSON オブジェクトから名前付き Tuple を推論できるようになります。生成される名前付き Tuple には、サンプルデータ内で対応するすべての JSON オブジェクトに含まれる全要素が含まれます。
これは、JSON データがスパースではなく、データのサンプルにあり得るすべてのオブジェクトキーが含まれている場合に有用です。

この設定はデフォルトで有効です。

**例**

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

```response title="Response"
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"array" : [{"a" : 42, "b" : "Hello"}, {}, {"c" : [1,2,3]}, {"d" : "2020-01-01"}]}')
```

```markdown title="Response"
┌─name──┬─type────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ array │ Array(Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Nullable(Date))) │              │                    │         │                  │                │
└───────┴─────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects">
  ##### input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects
</div>

この設定を有効にすると、JSON オブジェクトから named tuple を推論する際 (`input_format_json_try_infer_named_tuples_from_objects` が有効な場合) 、あいまいなパスに対して例外を発生させる代わりに String type を使用できます。
これにより、あいまいなパスが存在していても、JSON オブジェクトを 名前付き Tuple として読み込めます。

デフォルトでは無効です。

**例**

設定が無効な場合:

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 0;
DESC format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : "Hello"}}}');
```

```response title="Response"
Code: 636. DB::Exception: The table structure cannot be extracted from a JSONEachRow format file. Error:
Code: 117. DB::Exception: JSON objects have ambiguous data: in some objects path 'a' has type 'Int64' and in some - 'Tuple(b String)'. You can enable setting input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects to use String type for path 'a'. (INCORRECT_DATA) (version 24.3.1.1).
You can specify the structure manually. (CANNOT_EXTRACT_TABLE_STRUCTURE)
```

設定を有効にした場合:

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : "a" : 42}, {"obj" : {"a" : {"b" : "Hello"}}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : "Hello"}}}');
```

```response title="Response"
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(String))     │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
┌─obj─────────────────┐
│ ('42')              │
│ ('{"b" : "Hello"}') │
└─────────────────────┘
```

<div id="input_format_json_read_objects_as_strings">
  ##### input_format_json_read_objects_as_strings
</div>

この設定を有効にすると、ネストされた JSON オブジェクトを文字列として読み取れるようになります。
この設定を使用すると、JSON オブジェクト型を使わずにネストされた JSON オブジェクトを読み取ることができます。

この設定はデフォルトで有効です。

注: この設定の有効化が反映されるのは、設定 `input_format_json_try_infer_named_tuples_from_objects` が無効になっている場合のみです。

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

<div id="input_format_json_read_numbers_as_strings">
  ##### input_format_json_read_numbers_as_strings
</div>

この設定を有効にすると、数値をStringとして読み込めるようになります。

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

<div id="input_format_json_read_bools_as_numbers">
  ##### input_format_json_read_bools_as_numbers
</div>

この設定を有効にすると、Bool 値を数値として読み込めるようになります。

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

<div id="input_format_json_read_bools_as_strings">
  ##### input_format_json_read_bools_as_strings
</div>

この設定を有効にすると、Bool 値を文字列として読み込めるようになります。

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

<div id="input_format_json_read_arrays_as_strings">
  ##### input_format_json_read_arrays_as_strings
</div>

この設定を有効にすると、JSON 配列の値を文字列として読み取れるようになります。

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

<div id="input_format_json_infer_incomplete_types_as_strings">
  ##### input_format_json_infer_incomplete_types_as_strings
</div>

この設定を有効にすると、スキーマ推論時に、データサンプル内で `Null`/`{}`/`[]` のみを含む JSON キーに対して String 型を使用できるようになります。
JSON フォーマットでは、対応する設定がすべて有効であれば任意の値を String として読み取ることができ (これらの設定はすべてデフォルトで有効です) 、型が不明なキーに String 型を使用することで、スキーマ推論時に `Cannot determine type for column 'column_name' by first 25000 rows of data, most likely this column contains only Nulls or empty Arrays/Maps` のようなエラーを回避できます。

例:

```sql title="Query"
SET input_format_json_infer_incomplete_types_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 1;
DESCRIBE format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
```

```markdown title="Response"
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Array(Nullable(Int64)), b Nullable(String), c Nullable(String), d Nullable(String), e Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

┌─obj────────────────────────────┐
│ ([1,2,3],'hello',NULL,'{}',[]) │
└────────────────────────────────┘
```

<div id="csv">
  ### CSV
</div>

CSVフォーマットでは、ClickHouse は区切り文字に従って行からカラムの値を抽出します。ClickHouse では、数値と文字列を除くすべての型が二重引用符で囲まれていることを想定しています。値が二重引用符で囲まれている場合、ClickHouse は再帰的パーサーを使って
引用符内のデータを解析し、その後それに最も適したデータ型を見つけようとします。値が二重引用符で囲まれていない場合、ClickHouse はそれを数値として解析しようとし、
数値でない場合は文字列として扱います。

ClickHouse に一部のパーサーやヒューリスティクスを使って複雑な型を判定させたくない場合は、設定 `input_format_csv_use_best_effort_in_schema_inference` を無効にできます。
そうすると、ClickHouse はすべてのカラムを String として扱います。

設定 `input_format_csv_detect_header` が有効な場合、ClickHouse はスキーマの推論中に、カラム名 (場合によっては型も含む) を持つヘッダーを検出しようとします。この設定はデフォルトで有効です。

**例:**

整数、Float、Bools、String:

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

引用符のないString:

```sql
DESC format(CSV, 'Hello world!,World hello!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Date、DateTime 型：

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

Array:

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

配列に null が含まれている場合、ClickHouse は他の配列要素の型を使用します：

```sql
DESC format(CSV, '"[NULL, 42, NULL]"')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Map型の値:

```sql
DESC format(CSV, $$"{'key1' : 42, 'key2' : 24}"$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Nested、Array、Map:

```sql
DESC format(CSV, $$"[{'key1' : [[42, 42], []], 'key2' : [[null], [42]]}]"$$)
```

```response
┌─name─┬─type──────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Array(Nullable(Int64))))) │              │                    │         │                  │                │
└──────┴───────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

データに null 値しか含まれていないため、ClickHouse が引用符で囲まれた値の型を判別できない場合、ClickHouse はそれを String として扱います。

```sql
DESC format(CSV, '"[NULL, NULL]"')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

`input_format_csv_use_best_effort_in_schema_inference` 設定を無効にした場合の例:

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

ヘッダー自動検出の例 (`input_format_csv_detect_header` が有効な場合) :

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

少なくとも1つのカラムが `String` 型以外である場合にのみ、ヘッダー行を検出できます。すべてのカラムが `String` 型の場合、ヘッダー行は検出されません。

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

<div id="csv-settings">
  #### CSV設定
</div>

<div id="input_format_csv_try_infer_numbers_from_strings">
  ##### input_format_csv_try_infer_numbers_from_strings
</div>

この設定を有効にすると、String型の値から数値を推論できるようになります。

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

<div id="tsv-tskv">
  ### TSV/TSKV
</div>

TSV/TSKVフォーマットでは、ClickHouse は表形式の区切り文字に従って行からカラムの値を抽出し、その後、最も適切な型を判定するため、再帰的なパーサーを使って抽出した値を解析します。型を判定できない場合、ClickHouse はその値を String として扱います。

ClickHouse に一部のパーサーやヒューリスティクスを使って複雑な型を判定させたくない場合は、設定 `input_format_tsv_use_best_effort_in_schema_inference` を無効にできます。すると、ClickHouse はすべてのカラムを String として扱います。

設定 `input_format_tsv_detect_header` が有効な場合、ClickHouse はスキーマを推論する際に、カラム名 (場合によっては型も含む) を含むヘッダーの検出を試みます。この設定はデフォルトで有効です。

**例:**

Integers, Floats, Bools, Strings:

```sql
DESC format(TSV, '42    42.42    true    Hello,World!')
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
DESC format(TSKV, 'int=42    float=42.42    bool=true    string=Hello,World!\n')
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ int    │ Nullable(Int64)   │              │                    │         │                  │                │
│ float  │ Nullable(Float64) │              │                    │         │                  │                │
│ bool   │ Nullable(Bool)    │              │                    │         │                  │                │
│ string │ Nullable(String)  │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Date、DateTime 型：

```sql
DESC format(TSV, '2020-01-01    2020-01-01 00:00:00    2022-01-01 00:00:00.000')
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Array:

```sql
DESC format(TSV, '[1,2,3]    [[1, 2], [], [3, 4]]')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(TSV, '[''Hello'', ''world'']    [[''Abc'', ''Def''], []]')
```

```response
┌─name─┬─type───────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(String))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

配列に null が含まれている場合、ClickHouse はほかの配列要素の型を使用します:

```sql
DESC format(TSV, '[NULL, 42, NULL]')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

タプル:

```sql
DESC format(TSV, $$(42, 'Hello, world!')$$)
```

```response
┌─name─┬─type─────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Tuple(Nullable(Int64), Nullable(String)) │              │                    │         │                  │                │
└──────┴──────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Map:

```sql
DESC format(TSV, $${'key1' : 42, 'key2' : 24}$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Nested、Array、Tuple、Map：

```sql
DESC format(TSV, $$[{'key1' : [(42, 'Hello'), (24, NULL)], 'key2' : [(NULL, ','), (42, 'world!')]}]$$)
```

```response
┌─name─┬─type────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Tuple(Nullable(Int64), Nullable(String))))) │              │                    │         │                  │                │
└──────┴─────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

データに null しか含まれておらず ClickHouse が型を判定できない場合、ClickHouse はそれを String として扱います：

```sql
DESC format(TSV, '[NULL, NULL]')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

`input_format_tsv_use_best_effort_in_schema_inference` 設定を無効にした場合の例:

```sql
SET input_format_tsv_use_best_effort_in_schema_inference = 0
DESC format(TSV, '[1,2,3]    42.42    Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ヘッダー自動検出の例 (`input_format_tsv_detect_header` が有効な場合) :

名前のみ:

```sql
SELECT * FROM format(TSV,
$$number    string    array
42    Hello    [1, 2, 3]
43    World    [4, 5, 6]
$$);
```

```response
┌─number─┬─string─┬─array───┐
│     42 │ Hello  │ [1,2,3] │
│     43 │ World  │ [4,5,6] │
└────────┴────────┴─────────┘
```

名前とデータ型:

```sql
DESC format(TSV,
$$number    string    array
UInt32    String    Array(UInt16)
42    Hello    [1, 2, 3]
43    World    [4, 5, 6]
$$)
```

```response
┌─name───┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ UInt32        │              │                    │         │                  │                │
│ string │ String        │              │                    │         │                  │                │
│ array  │ Array(UInt16) │              │                    │         │                  │                │
└────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ヘッダーは、少なくとも1つのカラムが非`String`型である場合にのみ検出されます。すべてのカラムが`String`型の場合、ヘッダーは検出されません。

```sql
SELECT * FROM format(TSV,
$$first_column    second_column
Hello    World
World    Hello
$$)
```

```response
┌─c1───────────┬─c2────────────┐
│ first_column │ second_column │
│ Hello        │ World         │
│ World        │ Hello         │
└──────────────┴───────────────┘
```

<div id="values">
  ### Values
</div>

Values フォーマットでは、ClickHouse は行からカラムの値を抽出し、リテラルの解析と同様の再帰的パーサーを使用して解析します。

**例：**

整数、Float、Bool、String:

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

Date、DateTime 型:

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

Arrays:

```sql
DESC format(Values, '([1,2,3], [[1, 2], [], [3, 4]])')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ArrayにNULLが含まれる場合、ClickHouseは他のArray要素の型を使用します：

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

Map型:

```sql
DESC format(Values, $$({'key1' : 42, 'key2' : 24})$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ネストされたArray、Tuple、Map：

```sql
DESC format(Values, $$([{'key1' : [(42, 'Hello'), (24, NULL)], 'key2' : [(NULL, ','), (42, 'world!')]}])$$)
```

```response
┌─name─┬─type────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Tuple(Nullable(Int64), Nullable(String))))) │              │                    │         │                  │                │
└──────┴─────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

データにNULL値しか含まれていないためClickHouseが型を判定できない場合、例外がスローされます:

```sql
DESC format(Values, '([NULL, NULL])')
```

```response
Code: 652. DB::Exception: Received from localhost:9000. DB::Exception:
Cannot determine type for column 'c1' by first 1 rows of data,
most likely this column contains only Nulls or empty Arrays/Maps.
...
```

`input_format_tsv_use_best_effort_in_schema_inference` 設定を無効にした場合の例:

```sql
SET input_format_tsv_use_best_effort_in_schema_inference = 0
DESC format(TSV, '[1,2,3]    42.42    Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="custom-separated">
  ### CustomSeparated
</div>

CustomSeparated フォーマットでは、ClickHouse はまず、指定された区切り文字に従って行からすべてのカラムの値を抽出し、その後、エスケープ規則に従って各値のデータ型を推論します。

設定 `input_format_custom_detect_header` が有効な場合、ClickHouse はスキーマの推論時に、カラム名 (場合によっては型も含む) を持つヘッダーの検出を試みます。この設定はデフォルトで有効です。

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

ヘッダーの自動検出の例 (`input_format_custom_detect_header` が有効な場合) :

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

<div id="template">
  ### Template
</div>

Template フォーマットでは、ClickHouse はまず指定されたテンプレートに従って行からすべてのカラム値を抽出し、次に各値のエスケープ規則に基づいて、そのデータ型を推論します。

**Example**

`resultset` というファイルに次の内容があるとします。

```bash
<result_before_delimiter>
${data}<result_after_delimiter>
```

そして、次の内容を含むファイル `row_format`:

```text
<row_before_delimiter>${column_1:CSV}<field_delimiter_1>${column_2:Quoted}<field_delimiter_2>${column_3:JSON}<row_after_delimiter>
```

次に、以下のクエリを実行できます：

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

<div id="regexp">
  ### Regexp
</div>

Template と同様に、Regexp フォーマットでは、ClickHouse はまず指定した正規表現に従って行からすべてのカラムの値を抽出し、その後、指定したエスケープ規則に従って各値のデータ型を推論しようとします。

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

<div id="settings-for-text-formats">
  ### テキストフォーマットの設定
</div>

<div id="input-format-max-rows-to-read-for-schema-inference">
  #### input_format_max_rows_to_read_for_schema_inference/input_format_max_bytes_to_read_for_schema_inference
</div>

これらの設定は、スキーマ推論時に読み取るデータ量を制御します。
読み取る行数/バイト数が多いほど、スキーマ推論にかかる時間は長くなりますが、型を正しく判定できる可能性も高くなります
(特に、データに多数の NULL 値が含まれている場合) 。

デフォルト値:

* `input_format_max_rows_to_read_for_schema_inference` は `25000`。
* `input_format_max_bytes_to_read_for_schema_inference` は `33554432` (32 MB) 。

<div id="column-names-for-schema-inference">
  #### column_names_for_schema_inference
</div>

明示的なカラム名を持たないフォーマットで、スキーマ推論に使用するカラム名の一覧です。指定した名前は、デフォルトの `c1,c2,c3,...` の代わりに使用されます。形式: `column1,column2,column3,...`。

**例**

```sql
DESC format(TSV, 'Hello, World!    42    [1, 2, 3]') settings column_names_for_schema_inference = 'str,int,arr'
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ str  │ Nullable(String)       │              │                    │         │                  │                │
│ int  │ Nullable(Int64)        │              │                    │         │                  │                │
│ arr  │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-hints">
  #### schema_inference_hints
</div>

自動判定された型ではなく、スキーマ推論で使用するカラム名と型の一覧です。形式: &#39;column&#95;name1 column&#95;type1, column&#95;name2 column&#95;type2, ...&#39;。
この設定は、自動的に判定できなかったカラムの型を指定したり、スキーマを最適化したりするために使用できます。

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

<div id="schema-inference-make-columns-nullable">
  #### schema_inference_make_columns_nullable $
</div>

NULL許容性に関する情報を持たないフォーマットのスキーマ推論において、推論された型を`Nullable`にするかどうかを制御します。設定可能な値:

* 0 - 推論された型が `Nullable` になることはありません。
* 1 - 推論されるすべての型が `Nullable` になります。
* 2 or &#39;auto&#39; - テキストフォーマットでは、スキーマ推論時に解析されるサンプル内でそのカラムに `NULL` が含まれている場合にのみ、推論された型は `Nullable` になります。厳密な型情報を持つフォーマット (Parquet、ORC、Arrow) では、null 許容性の情報はファイルのメタデータから取得されます。
* 3 - テキストフォーマットでは `Nullable` を使用します。厳密な型情報を持つフォーマットでは、ファイルのメタデータを使用します。

デフォルト: 3。

**例**

```sql
SET schema_inference_make_columns_nullable = 1;
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

<div id="input-format-try-infer-integers">
  #### input_format_try_infer_integers
</div>

:::note
この設定は `JSON` データ型には適用されません。
:::

有効にすると、ClickHouse はテキストフォーマットのスキーマ推論で、浮動小数点数ではなく整数型を推論しようとします。
サンプルデータ内のそのカラムのすべての数値が整数である場合、結果の型は `Int64` になります。少なくとも 1 つの数値が浮動小数点数である場合、結果の型は `Float64` になります。
サンプルデータに整数しか含まれておらず、かつ少なくとも 1 つの整数が正で `Int64` をオーバーフローする場合、ClickHouse は `UInt64` を推論します。

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

<div id="input-format-try-infer-datetimes">
  #### input_format_try_infer_datetimes
</div>

有効な場合、ClickHouse はテキストフォーマットのスキーマ推論で、文字列フィールドから `DateTime` または `DateTime64` 型を推論しようとします。
サンプルデータ内のあるカラムのすべてのフィールドが日時として正常に解析された場合、結果の型は `DateTime` または `DateTime64(9)` になります (いずれかの日時に小数部がある場合) 。
少なくとも 1 つのフィールドが日時として解析されなかった場合、結果の型は `String` になります。

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

<div id="input-format-try-infer-datetimes-only-datetime64">
  #### input_format_try_infer_datetimes_only_datetime64
</div>

有効な場合、日時の値に小数部が含まれていなくても、`input_format_try_infer_datetimes` が有効であれば、ClickHouse は常に `DateTime64(9)` を推論します。

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

```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

注: スキーマ推論時に日時をパースする際は、設定 [date&#95;time&#95;input&#95;format](/ja/operations/settings/settings-formats.md#date_time_input_format) に従います

<div id="input-format-try-infer-dates">
  #### input_format_try_infer_dates
</div>

有効にすると、ClickHouse はテキストフォーマットのスキーマ推論において、文字列フィールドから型 `Date` を推論しようとします。
サンプルデータ内のあるカラムのすべてのフィールドが日付として正常に解析された場合、結果の型は `Date` になります。
1 つでも日付として解析されないフィールドがある場合、結果の型は `String` になります。

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

<div id="input-format-try-infer-exponent-floats">
  #### input_format_try_infer_exponent_floats
</div>

有効にすると、ClickHouse はテキストフォーマットで指数表記の浮動小数点数を推論しようとします (JSON は除きます。JSON では、指数表記の数値は常に推論されます) 。

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

<div id="self-describing-formats">
  ## 自己記述型フォーマット
</div>

自己記述型フォーマットには、データの構造に関する情報がデータ自体に含まれています。
これは、説明を含むヘッダー、バイナリの型ツリー、ある種のテーブルなどの形を取ります。
ClickHouse は、このようなフォーマットのファイルからスキーマを自動的に推論するために、型情報を含むデータの一部を読み取り、
それを ClickHouse テーブルのスキーマに変換します。

<div id="formats-with-names-and-types">
  ### -WithNamesAndTypes 接尾辞付きフォーマット
</div>

ClickHouse は、-WithNamesAndTypes という接尾辞が付いた一部のテキストフォーマットをサポートしています。この接尾辞は、実際のデータの前に、カラム名と型を含む追加の2行が含まれることを意味します。
このようなフォーマットでスキーマ推論を行う際、ClickHouse は先頭の2行を読み取り、カラム名と型を抽出します。

**例**

```sql
DESC format(TSVWithNamesAndTypes,
$$num    str    arr
UInt8    String    Array(UInt8)
42    Hello, World!    [1,2,3]
$$)
```

```response
┌─name─┬─type─────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ num  │ UInt8        │              │                    │         │                  │                │
│ str  │ String       │              │                    │         │                  │                │
│ arr  │ Array(UInt8) │              │                    │         │                  │                │
└──────┴──────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-with-metadata">
  ### メタデータを含む JSON フォーマット
</div>

一部の JSON 入力フォーマット ([JSON](/ja/interfaces/formats/JSON)、[JSONCompact](/ja/interfaces/formats/JSONCompact)、[JSONColumnsWithMetadata](/ja/interfaces/formats/JSONColumnsWithMetadata)) には、カラム名や型に関するメタデータが含まれています。
このようなフォーマットのスキーマ推論では、ClickHouse はこのメタデータを読み取ります。

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

<div id="avro">
  ### Avro
</div>

Avro フォーマットでは、ClickHouse はデータからスキーマを読み取り、以下の型対応に従って ClickHouse スキーマへ変換します。

| Avro データ型                          | ClickHouse データ型                                                                |
| ---------------------------------- | ------------------------------------------------------------------------------ |
| `boolean`                          | [Bool](../sql-reference/data-types/boolean.md)                                 |
| `int`                              | [Int32](../sql-reference/data-types/int-uint.md)                               |
| `int (date)` *                     | [Date32](../sql-reference/data-types/date32.md)                                |
| `long`                             | [Int64](../sql-reference/data-types/int-uint.md)                               |
| `float`                            | [Float32](../sql-reference/data-types/float.md)                                |
| `double`                           | [Float64](../sql-reference/data-types/float.md)                                |
| `bytes`, `string`                  | [String](../sql-reference/data-types/string.md)                                |
| `fixed`                            | [FixedString(N)](../sql-reference/data-types/fixedstring.md)                   |
| `enum`                             | [Enum](../sql-reference/data-types/enum.md)                                    |
| `array(T)`                         | [Array(T)](../sql-reference/data-types/array.md)                               |
| `union(null, T)`, `union(T, null)` | [Nullable(T)](../sql-reference/data-types/date.md)                             |
| `null`                             | [Nullable(Nothing)](../sql-reference/data-types/special-data-types/nothing.md) |
| `string (uuid)` *                  | [UUID](../sql-reference/data-types/uuid.md)                                    |
| `binary (decimal)` *               | [Decimal(P, S)](../sql-reference/data-types/decimal.md)                        |

* [Avro の論理型](https://avro.apache.org/docs/current/spec.html#Logical+Types)

その他の Avro 型はサポートされていません。

<div id="parquet">
  ### Parquet
</div>

Parquet フォーマットでは、ClickHouse はデータからスキーマを読み取り、以下の型対応に基づいて ClickHouse のスキーマへ変換します。

| Parquet データ型            | ClickHouse データ型                                    |
| ---------------------------- | ------------------------------------------------------- |
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

そのほかの Parquet 型はサポートされていません。

<div id="arrow">
  ### Arrow
</div>

Arrow フォーマットでは、ClickHouse はデータからスキーマを読み取り、以下の型対応に基づいて ClickHouse のスキーマへ変換します。

| Arrow データ型                      | ClickHouse データ型                                         |
| ------------------------------- | ------------------------------------------------------- |
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

その他の Arrow 型はサポートされていません。

<div id="orc">
  ### ORC
</div>

ORCフォーマットでは、ClickHouseはデータからスキーマを読み取り、以下の型対応に基づいてClickHouseのスキーマに変換します。

| ORC データ型                             | ClickHouse データ型                                         |
| ------------------------------------ | ------------------------------------------------------- |
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

その他のORC型はサポートされていません。

<div id="native">
  ### Native
</div>

Native format は ClickHouse 内部で使用されるフォーマットで、データ内にスキーマを含みます。
スキーマ推論では、ClickHouse は変換を行うことなくデータからスキーマを読み取ります。

<div id="formats-with-external-schema">
  ## 外部スキーマを使用するフォーマット
</div>

このようなフォーマットでは、データを記述するスキーマを、特定のスキーマ言語で別ファイルとして用意する必要があります。
このようなフォーマットのファイルからスキーマを自動的に推論するために、ClickHouse は別ファイルから外部スキーマを読み取り、ClickHouse のテーブルスキーマに変換します。

<div id="protobuf">
  ### Protobuf
</div>

ClickHouse では、Protobuf フォーマットのスキーマ推論において、次の型対応を使用します。

| Protobuf データ型                 | ClickHouse データ型                                   |
| ----------------------------- | ------------------------------------------------- |
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

<div id="capnproto">
  ### CapnProto
</div>

CapnProto フォーマットのスキーマ推論では、ClickHouse は以下の型対応を使用します。

| CapnProto data type                | ClickHouse データ型                                   |
| ---------------------------------- | ------------------------------------------------------ |
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

<div id="strong-typed-binary-formats">
  ## 厳密に型付けされたバイナリ形式
</div>

このようなフォーマットでは、シリアライズされた各値にその型 (場合によっては名前も) の情報は含まれますが、テーブル全体に関する情報は含まれません。
このようなフォーマットのスキーマ推論では、ClickHouse はデータを行単位で読み取り (最大 `input_format_max_rows_to_read_for_schema_inference` 行、または `input_format_max_bytes_to_read_for_schema_inference` バイトまで) 、各値の型 (場合によっては名前も) をデータから抽出し、その後それらの型を ClickHouse の型に変換します。

<div id="msgpack">
  ### MsgPack
</div>

MsgPack フォーマットでは行間に区切り文字がないため、このフォーマットでスキーマ推論を使用するには、設定 `input_format_msgpack_number_of_columns` を使用してテーブルのカラム数を指定する必要があります。ClickHouse では次の型対応が使用されます。

| MessagePack data type (`INSERT`)                                   | ClickHouse データ型                                  |
| ------------------------------------------------------------------ | ----------------------------------------------------- |
| `int N`, `uint N`, `negative fixint`, `positive fixint`            | [Int64](../sql-reference/data-types/int-uint.md)      |
| `bool`                                                             | [UInt8](../sql-reference/data-types/int-uint.md)      |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [String](../sql-reference/data-types/string.md)       |
| `float 32`                                                         | [Float32](../sql-reference/data-types/float.md)       |
| `float 64`                                                         | [Float64](../sql-reference/data-types/float.md)       |
| `uint 16`                                                          | [Date](../sql-reference/data-types/date.md)           |
| `uint 32`                                                          | [DateTime](../sql-reference/data-types/datetime.md)   |
| `uint 64`                                                          | [DateTime64](../sql-reference/data-types/datetime.md) |
| `fixarray`, `array 16`, `array 32`                                 | [Array](../sql-reference/data-types/array.md)         |
| `fixmap`, `map 16`, `map 32`                                       | [Map](../sql-reference/data-types/map.md)             |

デフォルトでは、推論されたすべての型は `Nullable` 内に含まれますが、設定 `schema_inference_make_columns_nullable` を使用して変更できます。

<div id="bsoneachrow">
  ### BSONEachRow
</div>

BSONEachRow では、各行のデータが BSON ドキュメントとして表されます。スキーマ推論では、ClickHouse は BSON ドキュメントを 1 つずつ読み取り、データから
値、名前、型を抽出したうえで、以下の型対応に従ってそれらの型を ClickHouse の型へ変換します。

| BSON 型                                                                                        | ClickHouse 型                                                                                                  |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `\x08` boolean                                                                                | [Bool](../sql-reference/data-types/boolean.md)                                                                |
| `\x10` int32                                                                                  | [Int32](../sql-reference/data-types/int-uint.md)                                                              |
| `\x12` int64                                                                                  | [Int64](../sql-reference/data-types/int-uint.md)                                                              |
| `\x01` double                                                                                 | [Float64](../sql-reference/data-types/float.md)                                                               |
| `\x09` datetime                                                                               | [DateTime64](../sql-reference/data-types/datetime64.md)                                                       |
| `\x05` binary with`\x00` binary subtype, `\x02` string, `\x0E` symbol, `\x0D` JavaScript code | [String](../sql-reference/data-types/string.md)                                                               |
| `\x07` ObjectId,                                                                              | [FixedString(12)](../sql-reference/data-types/fixedstring.md)                                                 |
| `\x05` binary with `\x04` uuid subtype, size = 16                                             | [UUID](../sql-reference/data-types/uuid.md)                                                                   |
| `\x04` array                                                                                  | [Array](../sql-reference/data-types/array.md)/[Tuple](../sql-reference/data-types/tuple.md) (ネストされた型が異なる場合)   |
| `\x03` document                                                                               | [Named Tuple](../sql-reference/data-types/tuple.md)/[Map](../sql-reference/data-types/map.md) (String キーを使用)  |

デフォルトでは、推論されたすべての型は `Nullable` でラップされますが、設定 `schema_inference_make_columns_nullable` で変更できます。

<div id="formats-with-constant-schema">
  ## スキーマが固定されたフォーマット
</div>

この種のフォーマットのデータは、常に同じスキーマです。

<div id="line-as-string">
  ### LineAsString
</div>

このフォーマットでは、ClickHouse はデータの各行全体を `String` データ型の単一のカラムとして読み取ります。このフォーマットで推論される型は常に `String` で、カラム名は `line` です。

**例**

```sql
DESC format(LineAsString, 'Hello\nworld!')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-as-string">
  ### JSONAsString
</div>

このフォーマットでは、ClickHouse はデータ内の JSON オブジェクト全体を、`String` データ型の 1 つのカラムとして読み取ります。このフォーマットで推論される型は常に `String` で、カラム名は `json` です。

**例**

```sql
DESC format(JSONAsString, '{"x" : 42, "y" : "Hello, World!"}')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ json │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-as-object">
  ### JSONAsObject
</div>

このフォーマットでは、ClickHouse はデータ内の JSON オブジェクト全体を、`JSON` データ型の単一のカラムに読み込みます。このフォーマットで推論される型は常に `JSON` で、カラム名は `json` です。

**例**

```sql
DESC format(JSONAsObject, '{"x" : 42, "y" : "Hello, World!"}');
```

```response
┌─name─┬─type─┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ json │ JSON │              │                    │         │                  │                │
└──────┴──────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-modes">
  ## スキーマ推論モード
</div>

データファイルの集合からのスキーマ推論は、`default` と `union` の 2 つのモードで動作します。
モードは、設定 `schema_inference_mode` で制御されます。

<div id="default-schema-inference-mode">
  ### デフォルトモード
</div>

デフォルトモードでは、ClickHouse はすべてのファイルのスキーマが同一であるとみなし、ファイルを 1 つずつ読み込みながら、成功するまでスキーマを推論しようとします。

例:

`data1.jsonl`、`data2.jsonl`、`data3.jsonl` の 3 つのファイルがあり、内容は次のとおりだとします。

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

以下の3つのファイルでスキーマ推論を試してみましょう：

```sql title="Query"
:) DESCRIBE file('data{1,2,3}.jsonl') SETTINGS schema_inference_mode='default'
```

```response title="Response"
┌─name───┬─type─────────────┐
│ field1 │ Nullable(Int64)  │
│ field2 │ Nullable(String) │
└────────┴──────────────────┘
```

ご覧のとおり、ファイル `data3.jsonl` の `field3` は含まれていません。
これは、ClickHouse が最初にファイル `data1.jsonl` からスキーマを推論しようとしたものの、フィールド `field2` が NULL 値しか含まれていなかったために失敗し、
その後 `data2.jsonl` からのスキーマ推論には成功したため、ファイル `data3.jsonl` のデータは読み込まれなかったからです。

<div id="default-schema-inference-mode-1">
  ### ユニオンモード
</div>

ユニオンモードでは、ClickHouse はファイルごとに異なるスキーマを持つ可能性があるとみなし、すべてのファイルのスキーマを推論したうえで、それらを 1 つの共通スキーマにユニオンします。

次の内容を持つ 3 つのファイル `data1.jsonl`、`data2.jsonl`、`data3.jsonl` があるとします。

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

これら3つのファイルで、スキーマ推論を試してみましょう：

```sql title="Query"
:) DESCRIBE file('data{1,2,3}.jsonl') SETTINGS schema_inference_mode='union'
```

```response title="Response"
┌─name───┬─type───────────────────┐
│ field1 │ Nullable(Int64)        │
│ field2 │ Nullable(String)       │
│ field3 │ Array(Nullable(Int64)) │
└────────┴────────────────────────┘
```

ご覧のとおり、すべてのファイルにあるフィールドがすべて含まれています。

注意:

* 一部のファイルには、結果として得られるスキーマ内の一部のカラムが含まれていない場合があるため、union mode はカラムの部分集合の読み取りをサポートするフォーマット (JSONEachRow、Parquet、TSVWithNames など) でのみ利用でき、他のフォーマット (CSV、TSV、JSONCompactEachRow など) では動作しません。
* ClickHouse がいずれかのファイルからスキーマを推論できない場合は、例外が発生します。
* ファイル数が多い場合、すべてのファイルからスキーマを読み取るのにかなり時間がかかることがあります。

<div id="automatic-format-detection">
  ## フォーマットの自動検出
</div>

データのフォーマットが指定されておらず、ファイル拡張子からも判別できない場合、ClickHouse は内容に基づいてファイルのフォーマットを自動的に検出しようとします。

**例:**

次の内容を含む `data` があるとします。

```csv
"a","b"
1,"Data1"
2,"Data2"
3,"Data3"
```

このファイルは、フォーマットや構造を指定しなくても、内容を確認したりクエリしたりできます：

```sql
:) desc file(data);
```

```response
┌─name─┬─type─────────────┐
│ a    │ Nullable(Int64)  │
│ b    │ Nullable(String) │
└──────┴──────────────────┘
```

```sql
:) select * from file(data);
```

```response
┌─a─┬─b─────┐
│ 1 │ Data1 │
│ 2 │ Data2 │
│ 3 │ Data3 │
└───┴───────┘
```

:::note
ClickHouse が検出できるのは一部のフォーマットだけで、検出にも時間がかかるため、フォーマットは常に明示的に指定することをおすすめします。
:::