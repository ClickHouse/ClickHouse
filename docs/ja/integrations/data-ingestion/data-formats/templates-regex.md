---
sidebar_label: 正規表現とテンプレート
sidebar_position: 3
slug: /ja/integrations/data-formats/templates-regexp
---

# ClickHouseでテンプレートと正規表現を使用したカスタムテキストデータのインポートとエクスポート

私たちはしばしばカスタムテキストフォーマットのデータを扱わなければならないことがあります。それは標準ではないフォーマット、不正なJSON、壊れたCSVなどです。CSVやJSONのような標準パーサを使用しても、これらのすべてのケースで動作するわけではありません。しかし、ClickHouseには強力なテンプレートと正規表現フォーマットがあります。

## テンプレートに基づいたインポート
次の[ログファイル](assets/error.log)からデータをインポートしたいとします。

```bash
head error.log
```
```response
2023/01/15 14:51:17 [error]  client: 7.2.8.1, server: example.com "GET /apple-touch-icon-120x120.png HTTP/1.1"
2023/01/16 06:02:09 [error]  client: 8.4.2.7, server: example.com "GET /apple-touch-icon-120x120.png HTTP/1.1"
2023/01/15 13:46:13 [error]  client: 6.9.3.7, server: example.com "GET /apple-touch-icon.png HTTP/1.1"
2023/01/16 05:34:55 [error]  client: 9.9.7.6, server: example.com "GET /h5/static/cert/icon_yanzhengma.png HTTP/1.1"
```

このデータをインポートするために、[テンプレート](/docs/ja/interfaces/formats.md/#format-template)フォーマットを使用することができます。入力データの各行に値のプレースホルダーを持つテンプレート文字列を定義する必要があります。

```
<time> [error] client: <ip>, server: <host> "<request>"
```

データをインポートするためのテーブルを作成します。
```sql
CREATE TABLE error_log
(
    `time` DateTime,
    `ip` String,
    `host` String,
    `request` String
)
ENGINE = MergeTree
ORDER BY (host, request, time)
```

与えられたテンプレートを使用してデータをインポートするためには、テンプレート文字列をファイル([row.template](assets/row.template)という名前のファイル)に保存する必要があります。
```
${time:Escaped} [error]  client: ${ip:CSV}, server: ${host:CSV} ${request:JSON}
```

`${name:escaping}`フォーマットでカラムの名前とエスケープルールを定義します。ここではCSV、JSON、Escaped、Quotedなどが利用可能で、[それぞれのエスケープルール](/docs/ja/interfaces/formats.md/#format-template)を実装しています。

データをインポートする際に、ファイルを`format_template_row`設定オプションの引数として使用できます（*注意: テンプレートファイルとデータファイルはファイルの末尾に余分な`\n`記号が**ないこと**を確認してください*）:

```sql
INSERT INTO error_log FROM INFILE 'error.log'
SETTINGS format_template_row = 'row.template'
FORMAT Template
```

テーブルにデータがロードされたことを確認することができます。

```sql
SELECT
    request,
    count(*)
FROM error_log
GROUP BY request
```
```response
┌─request──────────────────────────────────────────┬─count()─┐
│ GET /img/close.png HTTP/1.1                      │     176 │
│ GET /h5/static/cert/icon_yanzhengma.png HTTP/1.1 │     172 │
│ GET /phone/images/icon_01.png HTTP/1.1           │     139 │
│ GET /apple-touch-icon-precomposed.png HTTP/1.1   │     161 │
│ GET /apple-touch-icon.png HTTP/1.1               │     162 │
│ GET /apple-touch-icon-120x120.png HTTP/1.1       │     190 │
└──────────────────────────────────────────────────┴─────────┘
```

### 空白のスキップ
テンプレート内のデリミタ間の空白をスキップすることができる[TemplateIgnoreSpaces](/docs/ja/interfaces/formats.md/#templateignorespaces)の使用を検討してください。
```
Template:               -->  "p1: ${p1:CSV}, p2: ${p2:CSV}"
TemplateIgnoreSpaces    -->  "p1:${p1:CSV}, p2:${p2:CSV}"
```

## テンプレートを使用したデータのエクスポート

テンプレートを使用して任意のテキストフォーマットにデータをエクスポートすることもできます。この場合、次の2つのファイルを作成する必要があります。

[結果セットのテンプレート](assets/output.results)では、結果セット全体のレイアウトを定義します。
```
== Top 10 IPs ==
${data}
--- ${rows_read:XML} rows read in ${time:XML} ---
```

ここで、`rows_read`と`time`は各クエリに利用できるシステムメトリックです。`data`は生成された行を表しており、[**行テンプレートファイル**](assets/output.rows)で定義されたテンプレートに基づきます。

```
${ip:Escaped} generated ${total:Escaped} requests
```

これらのテンプレートを使用して、次のクエリをエクスポートします。

```sql
SELECT
    ip,
    count() AS total
FROM error_log GROUP BY ip ORDER BY total DESC LIMIT 10
FORMAT Template SETTINGS format_template_resultset = 'output.results',
                         format_template_row = 'output.rows';

== Top 10 IPs ==

9.8.4.6 generated 3 requests
9.5.1.1 generated 3 requests
2.4.8.9 generated 3 requests
4.8.8.2 generated 3 requests
4.5.4.4 generated 3 requests
3.3.6.4 generated 2 requests
8.9.5.9 generated 2 requests
2.5.1.8 generated 2 requests
6.8.3.6 generated 2 requests
6.6.3.5 generated 2 requests

--- 1000 rows read in 0.001380604 ---
```

### HTMLファイルへのエクスポート
テンプレートベースの結果は、[`INTO OUTFILE`](/docs/ja/sql-reference/statements/select/into-outfile.md/)句を使用してファイルにエクスポートすることもできます。指定された[結果セット](assets/html.results)と[行](assets/html.row)フォーマットに基づいてHTMLファイルを生成しましょう。

```sql
SELECT
    ip,
    count() AS total
FROM error_log GROUP BY ip ORDER BY total DESC LIMIT 10
INTO OUTFILE 'out.html'
FORMAT Template
SETTINGS format_template_resultset = 'html.results',
         format_template_row = 'html.row'
```

### XMLへのエクスポート

テンプレートフォーマットを使用すれば、XMLを含むすべての想像可能なテキストフォーマットファイルを生成することができます。関連するテンプレートを入れてエクスポートするだけです。

また、メタデータを含む標準のXML結果を取得するために[XML](/docs/ja/interfaces/formats.md/#xml)フォーマットを使用することを検討してください。

```sql
SELECT *
FROM error_log
LIMIT 3
FORMAT XML
```
```xml
<?xml version='1.0' encoding='UTF-8' ?>
<result>
	<meta>
		<columns>
			<column>
				<name>time</name>
				<type>DateTime</type>
			</column>
			...
		</columns>
	</meta>
	<data>
		<row>
			<time>2023-01-15 13:00:01</time>
			<ip>3.5.9.2</ip>
			<host>example.com</host>
			<request>GET /apple-touch-icon-120x120.png HTTP/1.1</request>
		</row>
		...
	</data>
	<rows>3</rows>
	<rows_before_limit_at_least>1000</rows_before_limit_at_least>
	<statistics>
		<elapsed>0.000745001</elapsed>
		<rows_read>1000</rows_read>
		<bytes_read>88184</bytes_read>
	</statistics>
</result>
```

## 正規表現に基づくデータのインポート

[正規表現](/docs/ja/interfaces/formats.md/#data-format-regexp)フォーマットは、入力データをより複雑な方法で解析する必要がある場合の高度なケースに対応しています。ここでは、[error.log](assets/error.log)のサンプルファイルを解析し、ファイル名とプロトコルをキャプチャして別々のカラムに保存してみましょう。まず、それを受け入れる新しいテーブルを準備します。

```sql
CREATE TABLE error_log
(
    `time` DateTime,
    `ip` String,
    `host` String,
    `file` String,
    `protocol` String
)
ENGINE = MergeTree
ORDER BY (host, file, time)
```

次に、正規表現に基づいてデータをインポートします。

```sql
INSERT INTO error_log FROM INFILE 'error.log'
SETTINGS
  format_regexp = '(.+?) \\[error\\]  client: (.+), server: (.+?) "GET .+?([^/]+\\.[^ ]+) (.+?)"'
FORMAT Regexp
```

ClickHouseは、キャプチャグループごとに各列に関連する順序でデータを挿入します。データを確認してみましょう。

```sql
SELECT * FROM error_log LIMIT 5
```
```response
┌────────────────time─┬─ip──────┬─host────────┬─file─────────────────────────┬─protocol─┐
│ 2023-01-15 13:00:01 │ 3.5.9.2 │ example.com │ apple-touch-icon-120x120.png │ HTTP/1.1 │
│ 2023-01-15 13:01:40 │ 3.7.2.5 │ example.com │ apple-touch-icon-120x120.png │ HTTP/1.1 │
│ 2023-01-15 13:16:49 │ 9.2.9.2 │ example.com │ apple-touch-icon-120x120.png │ HTTP/1.1 │
│ 2023-01-15 13:21:38 │ 8.8.5.3 │ example.com │ apple-touch-icon-120x120.png │ HTTP/1.1 │
│ 2023-01-15 13:31:27 │ 9.5.8.4 │ example.com │ apple-touch-icon-120x120.png │ HTTP/1.1 │
└─────────────────────┴─────────┴─────────────┴──────────────────────────────┴──────────┘
```

デフォルトでは、一致しない行がある場合、ClickHouseはエラーを発生させます。一致しない行をスキップしたい場合は、[format_regexp_skip_unmatched](/docs/ja/operations/settings/settings-formats.md/#format_regexp_skip_unmatched)オプションを有効にします。

```sql
SET format_regexp_skip_unmatched = 1;
```

## その他のフォーマット

ClickHouseは、多くのシナリオやプラットフォームに対応するために、テキストおよびバイナリの多くのフォーマットをサポートしています。以下の記事で、さらに多くのフォーマットとその使用方法を探ってみてください。

- [CSVおよびTSVフォーマット](csv-tsv.md)
- [Parquet](parquet.md)
- [JSONフォーマット](/docs/ja/integrations/data-ingestion/data-formats/json/intro.md)
- **正規表現とテンプレート**
- [ネイティブおよびバイナリフォーマット](binary.md)
- [SQLフォーマット](sql.md)

さらに、[clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)もチェックしましょう。これは、ClickHouseサーバーを必要とせず、ローカル/リモートファイルで作業するためのポータブルなフル機能ツールです。
