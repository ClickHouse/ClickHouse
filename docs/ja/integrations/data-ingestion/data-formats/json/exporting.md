---
title: JSONのエクスポート
slug: /ja/integrations/data-formats/json/exporting
description: ClickHouseからJSONデータをエクスポートする方法
keywords: [json, clickhouse, formats, exporting]
---

# JSONのエクスポート

インポート用に使用されるほぼすべてのJSONフォーマットは、エクスポートにも使用できます。最も一般的なのは[`JSONEachRow`](/docs/ja/interfaces/formats.md/#jsoneachrow)です:

```sql
SELECT * FROM sometable FORMAT JSONEachRow
```
```response
{"path":"Bob_Dolman","month":"2016-11-01","hits":245}
{"path":"1-krona","month":"2017-01-01","hits":4}
{"path":"Ahmadabad-e_Kalij-e_Sofla","month":"2017-01-01","hits":3}
```

または、カラム名を省略してディスク容量を節約するために[`JSONCompactEachRow`](/ja/interfaces/formats#jsoncompacteachrow)を使用できます:

```sql
SELECT * FROM sometable FORMAT JSONCompactEachRow
```
```response
["Bob_Dolman", "2016-11-01", 245]
["1-krona", "2017-01-01", 4]
["Ahmadabad-e_Kalij-e_Sofla", "2017-01-01", 3]
```

## データ型を文字列としてオーバーライドする {#overriding-data-types-as-strings}

ClickHouseはデータ型を尊重し、JSONを標準に従ってエクスポートします。しかし、すべての値を文字列としてエンコードする必要がある場合は、[JSONStringsEachRow](/docs/ja/interfaces/formats.md/#jsonstringseachrow)フォーマットを使用できます:

```sql
SELECT * FROM sometable FORMAT JSONStringsEachRow
```
```response
{"path":"Bob_Dolman","month":"2016-11-01","hits":"245"}
{"path":"1-krona","month":"2017-01-01","hits":"4"}
{"path":"Ahmadabad-e_Kalij-e_Sofla","month":"2017-01-01","hits":"3"}
```

ここでは、数値カラム`hits`が文字列としてエンコードされています。文字列としてのエクスポートはすべてのJSON*フォーマットでサポートされており、`JSONStrings\*`や`JSONCompactStrings\*`フォーマットを探索できます:

```sql
SELECT * FROM sometable FORMAT JSONCompactStringsEachRow
```
```response
["Bob_Dolman", "2016-11-01", "245"]
["1-krona", "2017-01-01", "4"]
["Ahmadabad-e_Kalij-e_Sofla", "2017-01-01", "3"]
```

## データと共にメタデータをエクスポートする

アプリで一般的な[JSON](/docs/ja/interfaces/formats.md/#json)フォーマットは、結果データだけでなくカラムタイプやクエリ統計もエクスポートします:

```sql
SELECT * FROM sometable FORMAT JSON
```
```response
{
	"meta":
	[
		{
			"name": "path",
			"type": "String"
		},
		…
	],

	"data":
	[
		{
			"path": "Bob_Dolman",
			"month": "2016-11-01",
			"hits": 245
		},
		…
	],

	"rows": 3,

	"statistics":
	{
		"elapsed": 0.000497457,
		"rows_read": 3,
		"bytes_read": 87
	}
}
```

[JSONCompact](/docs/ja/interfaces/formats.md/#jsoncompact)フォーマットは、同じメタデータを表示しますが、データ自体をコンパクトな形式で出力します:

```sql
SELECT * FROM sometable FORMAT JSONCompact
```
```response
{
	"meta":
	[
		{
			"name": "path",
			"type": "String"
		},
		…
	],

	"data":
	[
		["Bob_Dolman", "2016-11-01", 245],
		["1-krona", "2017-01-01", 4],
		["Ahmadabad-e_Kalij-e_Sofla", "2017-01-01", 3]
	],

	"rows": 3,

	"statistics":
	{
		"elapsed": 0.00074981,
		"rows_read": 3,
		"bytes_read": 87
	}
}
```

すべての値を文字列としてエンコードするために、[`JSONStrings`](/docs/ja/interfaces/formats.md/#jsonstrings)または[`JSONCompactStrings`](/docs/ja/interfaces/formats.md/#jsoncompactstrings)バリアントを検討してください。

## JSONデータと構造をコンパクトにエクスポートする方法

データとその構造の両方を持つより効率的な方法は、[`JSONCompactEachRowWithNamesAndTypes`](/docs/ja/interfaces/formats.md/#jsoncompacteachrowwithnamesandtypes)フォーマットを使用することです:

```sql
SELECT * FROM sometable FORMAT JSONCompactEachRowWithNamesAndTypes
```
```response
["path", "month", "hits"]
["String", "Date", "UInt32"]
["Bob_Dolman", "2016-11-01", 245]
["1-krona", "2017-01-01", 4]
["Ahmadabad-e_Kalij-e_Sofla", "2017-01-01", 3]
```

この形式は、カラム名とタイプを持つ2つのヘッダ行を前置したコンパクトなJSON形式を使用します。このフォーマットは別のClickHouseインスタンス（または他のアプリ）にデータをインジェストするために使用できます。

## JSONをファイルにエクスポートする

JSONデータをファイルに保存するには、[INTO OUTFILE](/docs/ja/sql-reference/statements/select/into-outfile.md)句を使用できます:

```sql
SELECT * FROM sometable INTO OUTFILE 'out.json' FORMAT JSONEachRow
```
```response
36838935 rows in set. Elapsed: 2.220 sec. Processed 36.84 million rows, 1.27 GB (16.60 million rows/s., 572.47 MB/s.)
```

ClickHouseは37百万件近いレコードをJSONファイルにエクスポートするのにわずか2秒しかかかりません。また、`COMPRESSION`句を使用してオンザフライの圧縮を有効にしてエクスポートすることもできます:

```sql
SELECT * FROM sometable INTO OUTFILE 'out.json.gz' FORMAT JSONEachRow
```
```response
36838935 rows in set. Elapsed: 22.680 sec. Processed 36.84 million rows, 1.27 GB (1.62 million rows/s., 56.02 MB/s.)
```

達成するのにより多くの時間がかかりますが、はるかに小さな圧縮ファイルが生成されます:

```bash
2.2G	out.json
576M	out.json.gz
```
