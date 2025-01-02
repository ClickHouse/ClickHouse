---
sidebar_label: JSONの読み込み
sidebar_position: 20
title: JSONの操作
slug: /ja/integrations/data-formats/json/loading
description: JSONの読み込み
keywords: [json, clickhouse, 挿入, 読み込み]
---

# JSONの読み込み

このセクションでは、JSONデータが[NDJSON](https://github.com/ndjson/ndjson-spec)（改行区切りJSON）形式、ClickHouseでいうところの[`JSONEachRow`](/ja/interfaces/formats#jsoneachrow)であると仮定します。これは、その簡潔さと効率的なスペース使用からJSONを読み込むための推奨形式ですが、他の形式も[入力と出力](/docs/ja/interfaces/formats#json)に対してサポートされています。

次のJSONサンプルを考えます。これは[Python PyPIデータセット](https://clickpy.clickhouse.com/)の行を表しています：

```json
{
  "date": "2022-11-15",
  "country_code": "ES",
  "project": "clickhouse-connect",
  "type": "bdist_wheel",
  "installer": "pip",
  "python_minor": "3.9",
  "system": "Linux",
  "version": "0.3.0"
}
```

このJSONオブジェクトをClickHouseにロードするためには、テーブルスキーマを定義する必要があります。以下に示されているのは、**JSONキーがカラム名にマッピングされる**シンプルなスキーマです：

```sql
CREATE TABLE pypi (
  `date` Date,
  `country_code` String,
  `project` String,
  `type` String,
  `installer` String,
  `python_minor` String,
  `system` String,
  `version` String
)
ENGINE = MergeTree
ORDER BY (project, date)
```

:::note 並び替えキーについて
ここでは、`ORDER BY`句を使用して並び替えキーを選択しています。並び替えキーの詳細と選択方法については、[こちら](/docs/ja/data-modeling/schema-design#choosing-an-ordering-key)を参照してください。
:::

ClickHouseは複数の形式でJSONデータを読み込むことができ、拡張子と内容から自動的に型を推測します。上記のテーブルに対してJSONファイルを読み取るには、[S3関数](/docs/ja/sql-reference/table-functions/s3)を使用します：

```sql
SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/json/*.json.gz')
LIMIT 1
┌───────date─┬─country_code─┬─project────────────┬─type────────┬─installer────┬─python_minor─┬─system─┬─version─┐
│ 2022-11-15 │ CN           │ clickhouse-connect │ bdist_wheel │ bandersnatch │              │        │ 0.2.8   │
└────────────┴──────────────┴────────────────────┴─────────────┴──────────────┴──────────────┴────────┴─────────┘

1 row in set. Elapsed: 1.232 sec.
```

ここではファイル形式を指定する必要がないことに注目してください。代わりに、バケット内のすべての`*.json.gz`ファイルを読み取るためにglobパターンを使用しています。ClickHouseは拡張子と内容から形式が`JSONEachRow`（ndjson）であることを自動的に推測します。ClickHouseが形式を検出できない場合でも、パラメータ関数を使用して手動で形式を指定できます。

```sql
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/json/*.json.gz', JSONEachRow)
```

:::note 圧縮ファイルについて
上記のファイルは圧縮されていますが、これはClickHouseによって自動的に検出および処理されます。
:::

これらのファイルの行を読み込むには、[`INSERT INTO SELECT`](/ja/sql-reference/statements/insert-into#inserting-the-results-of-select)を使用します：

```sql
INSERT INTO pypi SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/json/*.json.gz')
Ok.

0 rows in set. Elapsed: 10.445 sec. Processed 19.49 million rows, 35.71 MB (1.87 million rows/s., 3.42 MB/s.)

SELECT * FROM pypi LIMIT 2

┌───────date─┬─country_code─┬─project────────────┐
│ 2022-05-26 │ CN       	│ clickhouse-connect │
│ 2022-05-26 │ CN       	│ clickhouse-connect │
└────────────┴──────────────┴────────────────────┘

2 rows in set. Elapsed: 0.005 sec. Processed 8.19 thousand rows, 908.03 KB (1.63 million rows/s., 180.38 MB/s.)
```

行は[`FORMAT`句](/ja/sql-reference/statements/select/format)を使ってインラインで読み込むこともできます。例えば

```sql
INSERT INTO pypi
FORMAT JSONEachRow
{"date":"2022-11-15","country_code":"CN","project":"clickhouse-connect","type":"bdist_wheel","installer":"bandersnatch","python_minor":"","system":"","version":"0.2.8"}
```

これらの例はJSONEachRow形式の使用を前提としています。他の一般的なJSON形式もサポートされており、それらのロード方法についての例は[こちら](/docs/ja/integrations/data-formats/json/other-formats)で提供されています。

上記では非常にシンプルなJSONデータの読み込み例を提供しました。ネストされた構造を含むより複雑なJSONについては、[**JSONスキーマの設計**](/docs/ja/integrations/data-formats/json/schema)ガイドを参照してください。
