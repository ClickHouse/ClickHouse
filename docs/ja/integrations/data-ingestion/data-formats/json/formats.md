---
title: 他のJSON形式の処理
slug: /ja/integrations/data-formats/json/other-formats
description: 他のJSON形式の処理
keywords: [json, formats, json formats]
---

# 他の形式の処理

以前のJSONデータのロード例では、[`JSONEachRow`](/ja/interfaces/formats#jsoneachrow) (ndjson) の使用を前提としています。以下に一般的な形式でのJSONのロード例を示します。

## JSONオブジェクトの配列

JSONデータの最も一般的な形式の1つは、JSON配列にJSONオブジェクトのリストがある形式です。[この例](../assets/list.json)のように：

```bash
> cat list.json
[
  {
    "path": "Akiba_Hebrew_Academy",
    "month": "2017-08-01",
    "hits": 241
  },
  {
    "path": "Aegithina_tiphia",
    "month": "2018-02-01",
    "hits": 34
  },
  ...
]
```

このようなデータのためのテーブルを作成しましょう：

```sql
CREATE TABLE sometable
(
    `path` String,
    `month` Date,
    `hits` UInt32
)
ENGINE = MergeTree
ORDER BY tuple(month, path)
```

JSONオブジェクトのリストをインポートするには、[`JSONEachRow`](/docs/ja/interfaces/formats.md/#jsoneachrow)形式を使用します（[list.json](../assets/list.json) ファイルからデータを挿入する）：

```sql
INSERT INTO sometable
FROM INFILE 'list.json'
FORMAT JSONEachRow
```

`FROM INFILE`句を使用してローカルファイルからデータをロードし、インポートが成功したことを確認できます：

```sql
SELECT *
FROM sometable
```
```response
┌─path──────────────────────┬──────month─┬─hits─┐
│ 1971-72_Utah_Stars_season │ 2016-10-01 │    1 │
│ Akiba_Hebrew_Academy      │ 2017-08-01 │  241 │
│ Aegithina_tiphia          │ 2018-02-01 │   34 │
└───────────────────────────┴────────────┴──────┘
```

## NDJSON (行区切りJSON) の処理

多くのアプリケーションがJSON形式でデータをログに記録し、各ログ行が個別のJSONオブジェクトとなることがあります。[このファイル](../assets/object-per-line.json) のように：

```bash
cat object-per-line.json
```
```response
{"path":"1-krona","month":"2017-01-01","hits":4}
{"path":"Ahmadabad-e_Kalij-e_Sofla","month":"2017-01-01","hits":3}
{"path":"Bob_Dolman","month":"2016-11-01","hits":245}
```

同じ`JSONEachRow`形式はこのようなファイルにも対応できます：

```sql
INSERT INTO sometable FROM INFILE 'object-per-line.json' FORMAT JSONEachRow;
SELECT * FROM sometable;
```
```response
┌─path──────────────────────┬──────month─┬─hits─┐
│ Bob_Dolman                │ 2016-11-01 │  245 │
│ 1-krona                   │ 2017-01-01 │    4 │
│ Ahmadabad-e_Kalij-e_Sofla │ 2017-01-01 │    3 │
└───────────────────────────┴────────────┴──────┘
```

## JSONオブジェクトキー

場合によっては、JSONオブジェクトのリストが配列要素ではなくオブジェクトプロパティとしてエンコードされることがあります（例として[objects.json](../assets/objects.json) を参照）：

```
cat objects.json
```
```response
{
  "a": {
    "path":"April_25,_2017",
    "month":"2018-01-01",
    "hits":2
  },
  "b": {
    "path":"Akahori_Station",
    "month":"2016-06-01",
    "hits":11
  },
  ...
}
```

ClickHouseは、この種のデータを[`JSONObjectEachRow`](/docs/ja/interfaces/formats.md/#jsonobjecteachrow)形式を使用してロードできます：

```sql
INSERT INTO sometable FROM INFILE 'objects.json' FORMAT JSONObjectEachRow;
SELECT * FROM sometable;
```
```response
┌─path────────────┬──────month─┬─hits─┐
│ Abducens_palsy  │ 2016-05-01 │   28 │
│ Akahori_Station │ 2016-06-01 │   11 │
│ April_25,_2017  │ 2018-01-01 │    2 │
└─────────────────┴────────────┴──────┘
```

### 親オブジェクトキーの値を指定する

テーブルに親オブジェクトキーの値を保存したい場合は、[次のオプション](/docs/ja/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name)を使用してキー値を保存する列の名前を定義できます：

```sql
SET format_json_object_each_row_column_for_object_name = 'id'
```

元のJSONファイルからどのデータがロードされるかを[`file()`](/docs/ja/sql-reference/functions/files.md/#file) 関数を使用して確認できます：

```sql
SELECT * FROM file('objects.json', JSONObjectEachRow)
```
```response
┌─id─┬─path────────────┬──────month─┬─hits─┐
│ a  │ April_25,_2017  │ 2018-01-01 │    2 │
│ b  │ Akahori_Station │ 2016-06-01 │   11 │
│ c  │ Abducens_palsy  │ 2016-05-01 │   28 │
└────┴─────────────────┴────────────┴──────┘
```

`id` カラムがキー値によって正しく埋められていることに注意してください。

## JSON配列

時には、スペースを節約するために、JSONファイルがオブジェクトではなく配列としてエンコードされることがあります。この場合、[JSON配列のリスト](../assets/arrays.json) を扱います：

```bash
cat arrays.json
```
```response
["Akiba_Hebrew_Academy", "2017-08-01", 241],
["Aegithina_tiphia", "2018-02-01", 34],
["1971-72_Utah_Stars_season", "2016-10-01", 1]
```

この場合、ClickHouseはこのデータをロードし、配列内の順番に基づいて各値を対応するカラムに割り当てます。[`JSONCompactEachRow`](/docs/ja/interfaces/formats.md/#jsoncompacteachrow)形式を使用します：

```sql
SELECT * FROM sometable
```
```response
┌─c1────────────────────────┬─────────c2─┬──c3─┐
│ Akiba_Hebrew_Academy      │ 2017-08-01 │ 241 │
│ Aegithina_tiphia          │ 2018-02-01 │  34 │
│ 1971-72_Utah_Stars_season │ 2016-10-01 │   1 │
└───────────────────────────┴────────────┴─────┘
```

### JSON配列から個々のカラムをインポートする

場合によっては、データが行単位ではなくカラム単位でエンコードされていることがあります。この場合、親JSONオブジェクトに値を持つカラムが含まれています。[次のファイル](../assets/columns.json)を見てみましょう：

```bash
cat columns.json
```
```response
{
  "path": ["2007_Copa_America", "Car_dealerships_in_the_USA", "Dihydromyricetin_reductase"],
  "month": ["2016-07-01", "2015-07-01", "2015-07-01"],
  "hits": [178, 11, 1]
}
```

ClickHouseは、このようにフォーマットされたデータを解析するために[`JSONColumns`](/docs/ja/interfaces/formats.md/#jsoncolumns)形式を使用します：

```sql
SELECT * FROM file('columns.json', JSONColumns)
```
```response
┌─path───────────────────────┬──────month─┬─hits─┐
│ 2007_Copa_America          │ 2016-07-01 │  178 │
│ Car_dealerships_in_the_USA │ 2015-07-01 │   11 │
│ Dihydromyricetin_reductase │ 2015-07-01 │    1 │
└────────────────────────────┴────────────┴──────┘
```

よりコンパクトな形式もサポートされており、オブジェクトではなく[列の配列](../assets/columns-array.json)を扱う場合には[`JSONCompactColumns`](/docs/ja/interfaces/formats.md/#jsoncompactcolumns)形式を使用します：

```sql
SELECT * FROM file('columns-array.json', JSONCompactColumns)
```
```response
┌─c1──────────────┬─────────c2─┬─c3─┐
│ Heidenrod       │ 2017-01-01 │ 10 │
│ Arthur_Henrique │ 2016-11-01 │ 12 │
│ Alan_Ebnother   │ 2015-11-01 │ 66 │
└─────────────────┴────────────┴────┘
```

## 解析せずにJSONオブジェクトを保存する

場合によっては、JSONオブジェクトを解析せずに単一の`String`（またはJSON） カラムに保存したいことがあります。異なる構造のJSONオブジェクトのリストを扱う場合に便利です。[このファイル](../assets/custom.json)を見てみましょう。ここには親リストの中に異なるJSONオブジェクトがあります：

```bash
cat custom.json
```
```response
[
  {"name": "Joe", "age": 99, "type": "person"},
  {"url": "/my.post.MD", "hits": 1263, "type": "post"},
  {"message": "Warning on disk usage", "type": "log"}
]
```

これらの元のJSONオブジェクトを次のテーブルに保存したいと思います：

```sql
CREATE TABLE events
(
    `data` String
)
ENGINE = MergeTree
ORDER BY ()
```

そして、JSONオブジェクトを解析せずに保持するために[`JSONAsString`](/docs/ja/interfaces/formats.md/#jsonasstring)形式を使用してファイルからこのテーブルにデータをロードできます：

```sql
INSERT INTO events (data)
FROM INFILE 'custom.json'
FORMAT JSONAsString
```

保存されたオブジェクトをクエリするために[JSON functions](/docs/ja/sql-reference/functions/json-functions.md)を使用できます：

```sql
SELECT
    JSONExtractString(data, 'type') AS type,
    data
FROM events
```
```response
┌─type───┬─data─────────────────────────────────────────────────┐
│ person │ {"name": "Joe", "age": 99, "type": "person"}         │
│ post   │ {"url": "/my.post.MD", "hits": 1263, "type": "post"} │
│ log    │ {"message": "Warning on disk usage", "type": "log"}  │
└────────┴──────────────────────────────────────────────────────┘
```

`JSONAsString`は通常`JSONEachRow`形式で使用されるJSONオブジェクトが行ごとにフォーマットされたファイルでも問題なく動作します。

## 入れ子になったオブジェクトのスキーマ

[入れ子になったJSONオブジェクト](../assets/list-nested.json)を扱う場合は、追加でスキーマを定義し、複合型（[`Array`](/docs/ja/sql-reference/data-types/array.md/)、[`Object Data Type`](/ja/sql-reference/data-types/object-data-type)または[`Tuple`](/docs/ja/sql-reference/data-types/tuple.md/)）を使用してデータをロードできます：

```sql
SELECT *
FROM file('list-nested.json', JSONEachRow, 'page Tuple(path String, title String, owner_id UInt16), month Date, hits UInt32')
LIMIT 1
```
```response
┌─page───────────────────────────────────────────────┬──────month─┬─hits─┐
│ ('Akiba_Hebrew_Academy','Akiba Hebrew Academy',12) │ 2017-08-01 │  241 │
└────────────────────────────────────────────────────┴────────────┴──────┘
```

## 入れ子になったJSONオブジェクトへのアクセス

入れ子になったJSONキーを参照するために、[次の設定オプション](/docs/ja/operations/settings/settings-formats.md/#input_format_import_nested_json)を有効にします：

```sql
SET input_format_import_nested_json = 1
```

これにより、入れ子になったJSONオブジェクトキーをドット表記法を使用して参照できます（機能させるためにバックティック記号でラップすることを忘れないでください）：

```sql
SELECT *
FROM file('list-nested.json', JSONEachRow, '`page.owner_id` UInt32, `page.title` String, month Date, hits UInt32')
LIMIT 1
```
```results
┌─page.owner_id─┬─page.title───────────┬──────month─┬─hits─┐
│            12 │ Akiba Hebrew Academy │ 2017-08-01 │  241 │
└───────────────┴──────────────────────┴────────────┴──────┘
```

この方法で、入れ子になったJSONオブジェクトをフラット化したり、いくつかの入れ子になった値を使用してそれらを個別のカラムとして保存できます。

## 不明なカラムをスキップする

デフォルトでは、ClickHouseはJSONデータのインポート時に不明なカラムを無視します。`month`カラムがないテーブルに元のファイルをインポートしてみましょう：

```sql
CREATE TABLE shorttable
(
    `path` String,
    `hits` UInt32
)
ENGINE = MergeTree
ORDER BY path
```

元の3カラムのJSONデータをこのテーブルに挿入することができます：

```sql
INSERT INTO shorttable FROM INFILE 'list.json' FORMAT JSONEachRow;
SELECT * FROM shorttable
```
```response
┌─path──────────────────────┬─hits─┐
│ 1971-72_Utah_Stars_season │    1 │
│ Aegithina_tiphia          │   34 │
│ Akiba_Hebrew_Academy      │  241 │
└───────────────────────────┴──────┘
```

ClickHouseはインポート時に不明なカラムを無視します。これは[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)設定オプションによって無効にできます：

```sql
SET input_format_skip_unknown_fields = 0;
INSERT INTO shorttable FROM INFILE 'list.json' FORMAT JSONEachRow;
```
```response
Ok.
Exception on client:
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: month: (in file/uri /data/clickhouse/user_files/list.json): (at row 1)
```

ClickHouseはJSONとテーブルのカラム構造が不一致の際に例外をスローします。

## BSON

ClickHouseは、[BSON](https://bsonspec.org/)でエンコードされたファイルのデータのエクスポートとインポートを許可します。この形式は、いくつかのデータベース管理システム（例： [MongoDB](https://github.com/mongodb/mongo) データベース）で使用されます。

BSONデータをインポートするには、[BSONEachRow](/docs/ja/interfaces/formats.md/#bsoneachrow)形式を使用します。[このBSONファイル](../assets/data.bson)からデータをインポートしてみましょう：

```sql
SELECT * FROM file('data.bson', BSONEachRow)
```
```response
┌─path──────────────────────┬─month─┬─hits─┐
│ Bob_Dolman                │ 17106 │  245 │
│ 1-krona                   │ 17167 │    4 │
│ Ahmadabad-e_Kalij-e_Sofla │ 17167 │    3 │
└───────────────────────────┴───────┴──────┘
```

同じ形式を使用してBSONファイルにエクスポートすることもできます：

```sql
SELECT *
FROM sometable
INTO OUTFILE 'out.bson'
FORMAT BSONEachRow
```

その後、データが`out.bson`ファイルにエクスポートされます。
