---
title: JSONをモデル化する他のアプローチ
slug: /ja/integrations/data-formats/json/other-approaches
description: JSONをモデル化する他のアプローチ
keywords: [json, formats]
---

# JSONをモデル化する他のアプローチ

**以下は、ClickHouseでJSONをモデル化する際の代替手段です。これらは完全性のために文書化されていますが、ほとんどの使用ケースでは推奨されず、適用されません。**

## ネストを使用する

[Nested型](/docs/ja/sql-reference/data-types/nested-data-structures/nested)は、変更されることが少ない静的なオブジェクトをモデル化するために使用できます。これは`Tuple`および`Array(Tuple)`の代替手段を提供します。一般的に、この型はその動作がしばしば混乱を招くため、JSONに使用することは避けることをお勧めします。`Nested`の主な利点は、サブカラムがソートキーで使用できることです。

以下に、静的オブジェクトをモデル化するためのNested型の使用例を示します。次のような単純なログエントリをJSONで考えます：

```json
{
  "timestamp": 897819077,
  "clientip": "45.212.12.0",
  "request": {
    "method": "GET",
    "path": "/french/images/hm_nav_bar.gif",
    "version": "HTTP/1.0"
  },
  "status": 200,
  "size": 3305
}
```

`request`キーを`Nested`として宣言できます。`Tuple`と同様に、サブカラムを指定する必要があります。

```sql
-- デフォルト
SET flatten_nested=1
CREATE table http
(
   timestamp Int32,
   clientip     IPv4,
   request Nested(method LowCardinality(String), path String, version LowCardinality(String)),
   status       UInt16,
   size         UInt32,
) ENGINE = MergeTree() ORDER BY (status, timestamp);
```

### flatten_nested

設定`flatten_nested`はネストの動作を制御します。

#### flatten_nested=1

`1`（デフォルト）の値は、任意のレベルのネストをサポートしません。この値では、ネストされたデータ構造を同じ長さの複数の[Array](/docs/ja/sql-reference/data-types/array)カラムとして考えるのが最も簡単です。`method`、`path`、および`version`のフィールドは全て別々の`Array(Type)`カラムとして機能し、重要な制約として **`method`、`path`、および`version`フィールドの長さは同じでなければなりません。** これを説明するために、`SHOW CREATE TABLE`を使用すると以下のようになります：

```sql
SHOW CREATE TABLE http

CREATE TABLE http
(
    `timestamp` Int32,
    `clientip` IPv4,
    `request.method` Array(LowCardinality(String)),
    `request.path` Array(String),
    `request.version` Array(LowCardinality(String)),
    `status` UInt16,
    `size` UInt32
)
ENGINE = MergeTree
ORDER BY (status, timestamp)
```

以下、このテーブルに挿入します：

```sql
SET input_format_import_nested_json = 1;
INSERT INTO http
FORMAT JSONEachRow
{"timestamp":897819077,"clientip":"45.212.12.0","request":[{"method":"GET","path":"/french/images/hm_nav_bar.gif","version":"HTTP/1.0"}],"status":200,"size":3305}
```

ここでのいくつかの重要なポイント：

* JSONをネストされた構造として挿入するために、`input_format_import_nested_json`設定を使用する必要があります。これがない場合、JSONをフラットにする必要があります。例：

    ```sql
    INSERT INTO http FORMAT JSONEachRow
    {"timestamp":897819077,"clientip":"45.212.12.0","request":{"method":["GET"],"path":["/french/images/hm_nav_bar.gif"],"version":["HTTP/1.0"]},"status":200,"size":3305}
    ```
* ネストされたフィールド`method`、`path`、および`version`はJSON配列として渡す必要があります。例：

  ```json
  {
    "@timestamp": 897819077,
    "clientip": "45.212.12.0",
    "request": {
      "method": [
        "GET"
      ],
      "path": [
        "/french/images/hm_nav_bar.gif"
      ],
      "version": [
        "HTTP/1.0"
      ]
    },
    "status": 200,
    "size": 3305
  }
  ```

カラムはドット表記を使用してクエリできます：

```sql
SELECT clientip, status, size, `request.method` FROM http WHERE has(request.method, 'GET');

┌─clientip────┬─status─┬─size─┬─request.method─┐
│ 45.212.12.0 │    200 │ 3305 │ ['GET']        │
└─────────────┴────────┴──────┴────────────────┘
1 row in set. Elapsed: 0.002 sec.
```

サブカラムの`Array`使用は、完全な[Array関数](/docs/ja/sql-reference/functions/array-functions)を潜在的に活用できることを意味し、カラムに複数の値がある場合には[`ARRAY JOIN`](/docs/ja/sql-reference/statements/select/array-join)句が役立ちます。

#### flatten_nested=0

これは任意のレベルのネストを許可し、ネストされたカラムは`Tuple`の単一の配列として保持されます - 実質的に`Array(Tuple)`と同じになります。

**これが`Nested`を使ったJSONの使用において推奨される方法であり、しばしば最も単純な方法です。以下に示すように、オブジェクトすべてがリストであることを必要とするだけです。**

以下では、テーブルを再作成し、行を再挿入します：

```sql
CREATE TABLE http
(
    `timestamp` Int32,
    `clientip` IPv4,
    `request` Nested(method LowCardinality(String), path String, version LowCardinality(String)),
    `status` UInt16,
    `size` UInt32
)
ENGINE = MergeTree
ORDER BY (status, timestamp)

SHOW CREATE TABLE http

-- note Nested type is preserved.
CREATE TABLE default.http
(
    `timestamp` Int32,
    `clientip` IPv4,
    `request` Nested(method LowCardinality(String), path String, version LowCardinality(String)),
    `status` UInt16,
    `size` UInt32
)
ENGINE = MergeTree
ORDER BY (status, timestamp)

INSERT INTO http
FORMAT JSONEachRow
{"timestamp":897819077,"clientip":"45.212.12.0","request":[{"method":"GET","path":"/french/images/hm_nav_bar.gif","version":"HTTP/1.0"}],"status":200,"size":3305}
```

ここでのいくつかの重要なポイント：

* `input_format_import_nested_json`は挿入に必要ありません。
* `SHOW CREATE TABLE`では`Nested`型が保持されます。このカラムの内部には実質的には`Array(Tuple(Nested(method LowCardinality(String), path String, version LowCardinality(String))))`があります。
* 結果として、`request`を配列として挿入する必要があります。例：

  ```json
  {
    "timestamp": 897819077,
    "clientip": "45.212.12.0",
    "request": [
      {
        "method": "GET",
        "path": "/french/images/hm_nav_bar.gif",
        "version": "HTTP/1.0"
      }
    ],
    "status": 200,
    "size": 3305
  }
  ```

カラムは再度、ドット表記を使用してクエリできます：

```sql
SELECT clientip, status, size, `request.method` FROM http WHERE has(request.method, 'GET');

┌─clientip────┬─status─┬─size─┬─request.method─┐
│ 45.212.12.0 │    200 │ 3305 │ ['GET']        │
└─────────────┴────────┴──────┴────────────────┘
1 row in set. Elapsed: 0.002 sec.
```

### 例

上記のデータの大きな例は、s3のパブリックバケットにあります：`s3://datasets-documentation/http/`。

```sql
SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/http/documents-01.ndjson.gz', 'JSONEachRow')
LIMIT 1
FORMAT PrettyJSONEachRow

{
    "@timestamp": "893964617",
    "clientip": "40.135.0.0",
    "request": {
        "method": "GET",
        "path": "\/images\/hm_bg.jpg",
        "version": "HTTP\/1.0"
    },
    "status": "200",
    "size": "24736"
}

1 row in set. Elapsed: 0.312 sec.
```

JSONの制約と入力形式を考慮して、このサンプルデータセットを次のクエリを使用して挿入します。ここでは、`flatten_nested=0`を設定します。

次の文は1000万行を挿入するため、実行に数分かかる場合があります。必要であれば`LIMIT`を適用してください。

```sql
INSERT INTO http
SELECT `@timestamp` AS `timestamp`, clientip, [request], status,
size FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/http/documents-01.ndjson.gz',
'JSONEachRow');
```

このデータをクエリするには、リクエストフィールドを配列としてアクセスする必要があります。以下では、固定された期間にわたるエラーとhttpメソッドをまとめています。

```sql
SELECT status, request.method[1] as method, count() as c
FROM http
WHERE status >= 400
  AND toDateTime(timestamp) BETWEEN '1998-01-01 00:00:00' AND '1998-06-01 00:00:00'
GROUP by method, status
ORDER BY c DESC LIMIT 5;

┌─status─┬─method─┬─────c─┐
│    404 │ GET    │ 11267 │
│    404 │ HEAD   │   276 │
│    500 │ GET    │   160 │
│    500 │ POST   │   115 │
│    400 │ GET    │    81 │
└────────┴────────┴───────┘

5 rows in set. Elapsed: 0.007 sec.
```

### ペアワイズ配列を使用する

ペアワイズ配列は、JSONをStringとして表現する柔軟性と、より構造化されたアプローチのパフォーマンスとのバランスを提供します。スキーマは柔軟で、ルートに新しいフィールドを追加することができます。ただし、これは非常に複雑なクエリ構文を必要とし、ネストされた構造と互換性がありません。

例として、次のテーブルを考えてみます：

```sql
CREATE TABLE http_with_arrays (
   keys Array(String),
   values Array(String)
)
ENGINE = MergeTree  ORDER BY tuple();
```

このテーブルに挿入するには、JSONをキーと値のリストとして構造化する必要があります。以下のクエリは、これを達成するための`JSONExtractKeysAndValues`の使用例を示しています：

```sql
SELECT
    arrayMap(x -> (x.1), JSONExtractKeysAndValues(json, 'String')) AS keys,
    arrayMap(x -> (x.2), JSONExtractKeysAndValues(json, 'String')) AS values
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/http/documents-01.ndjson.gz', 'JSONAsString')
LIMIT 1
FORMAT Vertical

Row 1:
──────
keys:   ['@timestamp','clientip','request','status','size']
values: ['893964617','40.135.0.0','{"method":"GET","path":"/images/hm_bg.jpg","version":"HTTP/1.0"}','200','24736']

1 row in set. Elapsed: 0.416 sec.
```

リクエストカラムがネストされた構造として文字列で表現されている点に注意してください。ルートに任意の新しいキーを挿入できます。また、JSON自体に任意の違いを持たせることもできます。ローカルテーブルに挿入するには、次を実行します：

```sql
INSERT INTO http_with_arrays
SELECT
    arrayMap(x -> (x.1), JSONExtractKeysAndValues(json, 'String')) AS keys,
    arrayMap(x -> (x.2), JSONExtractKeysAndValues(json, 'String')) AS values
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/http/documents-01.ndjson.gz', 'JSONAsString')

0 rows in set. Elapsed: 12.121 sec. Processed 10.00 million rows, 107.30 MB (825.01 thousand rows/s., 8.85 MB/s.)
```

この構造をクエリするには、必要なキーのインデックスを識別するために[`indexOf`](/ja/sql-reference/functions/array-functions#indexofarr-x)関数を使用する必要があります（これは値の順序に一致するはずです）。これを使って値の配列カラムにアクセスできます。つまり、`values[indexOf(keys, 'status')]`。リクエストカラムには引き続きJSONの解析メソッドが必要です - この場合、`simpleJSONExtractString`を使用します。

```sql
SELECT toUInt16(values[indexOf(keys, 'status')])                           as status,
       simpleJSONExtractString(values[indexOf(keys, 'request')], 'method') as method,
       count()                                                             as c
FROM http_with_arrays
WHERE status >= 400
  AND toDateTime(values[indexOf(keys, '@timestamp')]) BETWEEN '1998-01-01 00:00:00' AND '1998-06-01 00:00:00'
GROUP by method, status ORDER BY c DESC LIMIT 5;

┌─status─┬─method─┬─────c─┐
│    404 │ GET    │ 11267 │
│    404 │ HEAD   │   276 │
│    500 │ GET    │   160 │
│    500 │ POST   │   115 │
│    400 │ GET    │    81 │
└────────┴────────┴───────┘

5 rows in set. Elapsed: 0.383 sec. Processed 8.22 million rows, 1.97 GB (21.45 million rows/s., 5.15 GB/s.)
Peak memory usage: 51.35 MiB.
```
