---
title: JSON スキーマの推論
slug: /ja/integrations/data-formats/json/inference
description: JSON スキーマの推論の使用方法
keywords: [json, schema, inference, schema inference]
---

ClickHouse は JSON データの構造を自動的に判別できます。これを利用して、`clickhouse-local` や S3バケット上のディスクに直接 JSON データをクエリ**する、またはデータを ClickHouse にロードする前にスキーマを自動的に作成することが可能です。

## 型推論を使用する場合

* **一貫した構造** - 型を推論しようとしているデータに、興味のある全カラムが含まれている場合。推論後に追加されたカラムを含むデータは無視され、クエリできません。
* **一貫した型** - 特定のカラムのデータ型が互換性を持たなければなりません。

:::note 重要
もし新しいキーがスキーマを変更する十分な警告なしに追加される動的な JSON をお持ちの場合は、例えば Kubernetes のログラベルなど、[**JSON スキーマの設計**](/docs/ja/integrations/data-formats/json/schema)の読解を推奨します。
:::

## 型の検出

以前の例では、NDJSON形式の[Python PyPI データセット](https://clickpy.clickhouse.com/)の簡単なバージョンを使用していました。このセクションでは、2.5百万の学術論文を含むネストされた構造を持つより複雑な[arXiv データセット](https://www.kaggle.com/datasets/Cornell-University/arxiv?resource=download)を調査します。このデータセットは NDJSON として配布され、各行が発表された学術論文を表しています。以下に例となる行を示します。

```json
{
  "id": "2101.11408",
  "submitter": "Daniel Lemire",
  "authors": "Daniel Lemire",
  "title": "Number Parsing at a Gigabyte per Second",
  "comments": "Software at https://github.com/fastfloat/fast_float and\n  https://github.com/lemire/simple_fastfloat_benchmark/",
  "journal-ref": "Software: Practice and Experience 51 (8), 2021",
  "doi": "10.1002/spe.2984",
  "report-no": null,
  "categories": "cs.DS cs.MS",
  "license": "http://creativecommons.org/licenses/by/4.0/",
  "abstract": "With disks and networks providing gigabytes per second ....\n",
  "versions": [
    {
      "created": "Mon, 11 Jan 2021 20:31:27 GMT",
      "version": "v1"
    },
    {
      "created": "Sat, 30 Jan 2021 23:57:29 GMT",
      "version": "v2"
    }
  ],
  "update_date": "2022-11-07",
  "authors_parsed": [
    [
      "Lemire",
      "Daniel",
      ""
    ]
  ]
}
```

このデータは、以前の例よりも複雑なスキーマを必要とします。このスキーマを定義するプロセスを以下に示し、`Tuple` や `Array` などの複雑な型を導入します。

このデータセットは公開されている S3 バケット `s3://datasets-documentation/arxiv/arxiv.json.gz` に保存されています。

上記のデータセットにはネストされた JSON オブジェクトが含まれていることがわかります。ユーザーはスキーマを作成しバージョン管理するべきですが、推論によりデータから型を推測することができます。これによりスキーマ DDL が自動生成され、手動で作成する必要がなくなり、開発プロセスが加速されます。

:::note 自動フォーマット検出
スキーマを検出するだけでなく、JSON スキーマ推論はファイルの拡張子や内容からデータのフォーマットを自動的に推測します。上記のファイルは、その結果として自動的に NDJSON として認識されます。
:::

[s3 関数](/ja/sql-reference/table-functions/s3)と `DESCRIBE` コマンドを使用すると、推論される型を確認できます。

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/arxiv/arxiv.json.gz')
SETTINGS describe_compact_output = 1
```
```response
┌─name───────────┬─type────────────────────────────────────────────────────────────────────┐
│ id             │ Nullable(String)                                                        │
│ submitter      │ Nullable(String)                                                        │
│ authors        │ Nullable(String)                                                        │
│ title          │ Nullable(String)                                                        │
│ comments       │ Nullable(String)                                                        │
│ journal-ref    │ Nullable(String)                                                        │
│ doi            │ Nullable(String)                                                        │
│ report-no      │ Nullable(String)                                                        │
│ categories     │ Nullable(String)                                                        │
│ license        │ Nullable(String)                                                        │
│ abstract       │ Nullable(String)                                                        │
│ versions       │ Array(Tuple(created Nullable(String),version Nullable(String)))         │
│ update_date    │ Nullable(Date)                                                          │
│ authors_parsed │ Array(Array(Nullable(String)))                                          │
└────────────────┴─────────────────────────────────────────────────────────────────────────┘
```

:::note null の回避
多くのカラムが Nullable として検出されていることがわかります。[Nullable の使用は推奨されていません](https://clickhouse.com/docs/ja/sql-reference/data-types/nullable#storage-features)が、絶対に必要な場合を除いて使用は避けてください。[schema_inference_make_columns_nullable](https://clickhouse.com/docs/ja/interfaces/schema-inference#schema_inference_make_columns_nullable)を使用して、Nullable が適用されるときの挙動を制御できます。
:::

ほとんどのカラムが自動的に `String` として検出されている一方、`update_date` カラムは正しく `Date` として検出されています。`versions` カラムはオブジェクトのリストを格納するために `Array(Tuple(created String, version String))` として作成され、`authors_parsed` はネストされた配列のために `Array(Array(String))` として定義されています。

:::note 型検出の制御
日付や日時の自動検出は、設定 [`input_format_try_infer_dates`](/docs/ja/interfaces/schema-inference#input_format_try_infer_dates)および[`input_format_try_infer_datetimes`](/docs/ja/interfaces/schema-inference#input_format_try_infer_datetimes)（いずれもデフォルトで有効）を通して制御できます。オブジェクトをタプルとして推測することは、設定 [`input_format_json_try_infer_named_tuples_from_objects`](/docs/ja/operations/settings/formats#input_format_json_try_infer_named_tuples_from_objects)によって制御されます。JSON のスキーマ推論を制御するその他の設定、例えば数字の自動検出は[こちらを参照してください](/docs/ja/interfaces/schema-inference#text-formats)。
:::

## JSON のクエリ

スキーマ推論に依存して JSON データをその場でクエリできます。下記にあるように、日付と配列が自動的に検出されることを利用して、各年のトップ著者を探します。

```sql
SELECT
    toYear(update_date) AS year,
    authors,
    count() AS c
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/arxiv/arxiv.json.gz')
GROUP BY
    year,
    authors
ORDER BY
    year ASC,
    c DESC
LIMIT 1 BY year

┌─year─┬─authors────────────────────────────────────┬───c─┐
│ 2007 │ The BABAR Collaboration, B. Aubert, et al  │  98 │
│ 2008 │ The OPAL collaboration, G. Abbiendi, et al │  59 │
│ 2009 │ Ashoke Sen                                 │  77 │
│ 2010 │ The BABAR Collaboration, B. Aubert, et al  │ 117 │
│ 2011 │ Amelia Carolina Sparavigna                 │  21 │
│ 2012 │ ZEUS Collaboration                         │ 140 │
│ 2013 │ CMS Collaboration                          │ 125 │
│ 2014 │ CMS Collaboration                          │  87 │
│ 2015 │ ATLAS Collaboration                        │ 118 │
│ 2016 │ ATLAS Collaboration                        │ 126 │
│ 2017 │ CMS Collaboration                          │ 122 │
│ 2018 │ CMS Collaboration                          │ 138 │
│ 2019 │ CMS Collaboration                          │ 113 │
│ 2020 │ CMS Collaboration                          │  94 │
│ 2021 │ CMS Collaboration                          │  69 │
│ 2022 │ CMS Collaboration                          │  62 │
│ 2023 │ ATLAS Collaboration                        │ 128 │
│ 2024 │ ATLAS Collaboration                        │ 120 │
└──────┴────────────────────────────────────────────┴─────┘

18 rows in set. Elapsed: 20.172 sec. Processed 2.52 million rows, 1.39 GB (124.72 thousand rows/s., 68.76 MB/s.)
```

スキーマ推論により、スキーマを指定することなく JSON ファイルをクエリできるため、アドホックなデータ解析作業が加速されます。

## テーブルの作成

スキーマ推論に依存して、テーブルのスキーマを作成することができます。以下の `CREATE AS EMPTY` コマンドは、テーブルの DDL を推論し、テーブルが作成されることを引き起こします。これはデータをロードしません：

```sql
CREATE TABLE arxiv
ENGINE = MergeTree
ORDER BY update_date EMPTY
AS SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/arxiv/arxiv.json.gz')
SETTINGS schema_inference_make_columns_nullable = 0
```

テーブルスキーマを確認するには、`SHOW CREATE TABLE` コマンドを使用します：

```sql
SHOW CREATE TABLE arxiv

CREATE TABLE arxiv
(
    `id` String,
    `submitter` String,
    `authors` String,
    `title` String,
    `comments` String,
    `journal-ref` String,
    `doi` String,
    `report-no` String,
    `categories` String,
    `license` String,
    `abstract` String,
    `versions` Array(Tuple(created String, version String)),
    `update_date` Date,
    `authors_parsed` Array(Array(String))
)
ENGINE = MergeTree
ORDER BY update_date
SETTINGS index_granularity = 8192
```

上記は、このデータに適した正しいスキーマです。スキーマ推論は、データのサンプリングと行単位での読み取りに基づいて行われます。カラム値はフォーマットに従って抽出され、再帰パーサーとヒューリスティックが各値の型を判定するために使用されます。スキーマ推論でデータから読み取る行数とバイト数の最大値は、設定 [`input_format_max_rows_to_read_for_schema_inference`](/docs/ja/interfaces/schema-inference#input_format_max_rows_to_read_for_schema_inferenceinput_format_max_bytes_to_read_for_schema_inference)（デフォルトで25000）および [`input_format_max_bytes_to_read_for_schema_inference`](/docs/ja/interfaces/schema-inference#input_format_max_rows_to_read_for_schema_inferenceinput_format_max_bytes_to_read_for_schema_inference)（デフォルトで32MB）によって制御されます。検出が正しくない場合、ユーザーは[こちら](/docs/ja/interfaces/schema-inference#schema_inference_hints)で説明されているようにヒントを提供できます。

### スニペットからテーブルを作成する

上記の例では、S3 のファイルを使用してテーブルスキーマを作成しています。ユーザーは単一行のスニペットからスキーマを作成したいかもしれません。これは、[format](/docs/ja/sql-reference/table-functions/format)関数を使用して実現できます:

```sql
CREATE TABLE arxiv
ENGINE = MergeTree
ORDER BY update_date EMPTY
AS SELECT *
FROM format(JSONEachRow, '{"id":"2101.11408","submitter":"Daniel Lemire","authors":"Daniel Lemire","title":"Number Parsing at a Gigabyte per Second","comments":"Software at https://github.com/fastfloat/fast_float and","doi":"10.1002/spe.2984","report-no":null,"categories":"cs.DS cs.MS","license":"http://creativecommons.org/licenses/by/4.0/","abstract":"Withdisks and networks providing gigabytes per second ","versions":[{"created":"Mon, 11 Jan 2021 20:31:27 GMT","version":"v1"},{"created":"Sat, 30 Jan 2021 23:57:29 GMT","version":"v2"}],"update_date":"2022-11-07","authors_parsed":[["Lemire","Daniel",""]]}') SETTINGS schema_inference_make_columns_nullable = 0

SHOW CREATE TABLE arxiv

CREATE TABLE arxiv
(
    `id` String,
    `submitter` String,
    `authors` String,
    `title` String,
    `comments` String,
    `doi` String,
    `report-no` String,
    `categories` String,
    `license` String,
    `abstract` String,
    `versions` Array(Tuple(created String, version String)),
    `update_date` Date,
    `authors_parsed` Array(Array(String))
)
ENGINE = MergeTree
ORDER BY update_date
```

## JSON データのロード

前述のコマンドはデータをロードできるテーブルを作成しました。以下の `INSERT INTO SELECT` を使用してデータをテーブルに挿入できます：

```sql
INSERT INTO arxiv SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/arxiv/arxiv.json.gz')

0 rows in set. Elapsed: 38.498 sec. Processed 2.52 million rows, 1.39 GB (65.35 thousand rows/s., 36.03 MB/s.)
Peak memory usage: 870.67 MiB.
```

その他のソース、たとえばファイルからのデータロードの例は[こちら](/docs/ja/sql-reference/statements/insert-into)を参照してください。

ロード後、フォーマット `PrettyJSONEachRow` を使用して行を元の構造で表示し、データをクエリできます：

```sql
SELECT *
FROM arxiv
LIMIT 1
FORMAT PrettyJSONEachRow

{
    "id": "0704.0004",
    "submitter": "David Callan",
    "authors": "David Callan",
    "title": "A determinant of Stirling cycle numbers counts unlabeled acyclic",
    "comments": "11 pages",
    "journal-ref": "",
    "doi": "",
    "report-no": "",
    "categories": "math.CO",
    "license": "",
    "abstract": "  We show that a determinant of Stirling cycle numbers counts unlabeled acyclic\nsingle-source automata.",
    "versions": [
        {
            "created": "Sat, 31 Mar 2007 03:16:14 GMT",
            "version": "v1"
        }
    ],
    "update_date": "2007-05-23",
    "authors_parsed": [
        [
            "Callan",
            "David"
        ]
    ]
}

1 row in set. Elapsed: 0.009 sec.
```

## エラーの処理

時折、不適切なデータに遭遇することがあります。特定のカラムが適切な型を持っていない場合や不適切にフォーマットされた JSON などです。そのため、挿入エラーを引き起こすデータがある場合に特定の行を無視するための [`input_format_allow_errors_ratio`](/docs/ja/operations/settings/formats#input_format_allow_errors_ratio) 設定を使用できます。さらに、推論を支援するための[ヒント](/docs/ja/interfaces/schema-inference#schema_inference_hints)を提供することも可能です。

## さらなる学習

データ型推論についての詳細は、[こちら](/ja/interfaces/schema-inference)のドキュメントページを参照してください。
