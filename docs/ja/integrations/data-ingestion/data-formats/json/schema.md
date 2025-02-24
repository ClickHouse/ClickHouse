---
title: JSON スキーマの設計
slug: /ja/integrations/data-formats/json/schema
description: JSON スキーマを最適に設計する方法
keywords: [json, clickhouse, 挿入, ロード, フォーマット, スキーマ]
---

# スキーマを設計する

[スキーマ推論](/docs/ja/integrations/data-formats/JSON/inference) を使用して JSON データの初期スキーマを設定し、S3 などの場所で JSON データファイルをクエリできますが、ユーザーはデータの最適化されたバージョン管理スキーマを確立することを目指すべきです。以下では、JSON 構造をモデリングするためのオプションについて説明します。

## 可能な限り抽出する

可能な限り、JSON キーをスキーマのルートにあるカラムに抽出することをお勧めします。これによりクエリ構文が簡素化されるだけでなく、必要に応じてこれらのカラムを `ORDER BY` 句で使用したり、[二次インデックス](/docs/ja/optimize/skipping-indexes) を指定したりすることができます。

ガイド [**JSON スキーマ推論**](/docs/ja/integrations/data-formats/json/inference) で探求された [arxiv データセット](https://www.kaggle.com/datasets/Cornell-University/arxiv?resource=download) を考えてみましょう:

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

`versions.created` の最初の値を `published_date` という名前でメインの注文キーにするとします。これを挿入前または挿入時に ClickHouse の [マテリアライズドビュー](/ja/guides/developer/cascading-materialized-views) または [マテリアライズドカラム](/ja/sql-reference/statements/alter/column#materialize-column) を使用して抽出する必要があります。

マテリアライズドカラムは、クエリ時にデータを抽出する最も簡単な方法を提供し、抽出ロジックが単純な SQL 式としてキャプチャできる場合に最も推奨されます。例として、`published_date` を arxiv スキーマにマテリアライズドカラムとして追加し、以下のように注文キーとして定義できます：

```sql
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
    `authors_parsed` Array(Array(String)),
    `published_date` DateTime DEFAULT parseDateTimeBestEffort(versions[1].1)
)
ENGINE = MergeTree
ORDER BY published_date
```

<!--TODO: Find a better way-->
:::note ネストされたカラム式
上記の方法では、位置で `created` カラムを参照する `versions[1].1` の表記を使用してタプルにアクセスする必要があります。これは、推奨される構文 `versions.created_at[1]` よりも簡単ではありません。
:::

データをロードすると、カラムが抽出されます：

```sql
INSERT INTO arxiv SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/arxiv/arxiv.json.gz')
0 rows in set. Elapsed: 39.827 sec. Processed 2.52 million rows, 1.39 GB (63.17 thousand rows/s., 34.83 MB/s.)

SELECT published_date
FROM arxiv_2
LIMIT 2
┌──────published_date─┐
│ 2007-03-31 02:26:18 │
│ 2007-03-31 03:16:14 │
└─────────────────────┘

2 rows in set. Elapsed: 0.001 sec.
```

:::note マテリアライズドカラムの動作
マテリアライズドカラムの値は常に挿入時に計算され、`INSERT` クエリで指定することはできません。マテリアライズドカラムはデフォルトでは `SELECT *` で返されません。これは、`SELECT *` の結果を常にテーブルに戻して `INSERT` できるという不変性を維持するためです。この動作は `asterisk_include_materialized_columns=1` を設定することで無効にできます。
:::

より複雑なフィルタリングと変換タスクには、[マテリアライズドビュー](/docs/ja/materialized-view) の使用をお勧めします。

## 静的 JSON と動的 JSON

JSON のスキーマを定義する主なタスクは、それぞれのキーの値に対して適切な型を選定することです。ユーザーは次のルールを JSON 階層内の各キーに適用して、それぞれのキーに対して適切な型を決定することをお勧めします。

1. **プリミティブ型** - キーの値がプリミティブ型である場合、それがサブオブジェクトの一部であろうとルートであろうと、一般的なスキーマ[設計のベストプラクティス](/docs/ja/data-modeling/schema-design)と[型最適化ルール](/docs/ja/data-modeling/schema-design#optimizing-types)に従ってその型を選択してください。 以下の`phone_numbers`のようなプリミティブの配列は、`Array(<type>)` 例えば `Array(String)` としてモデル化することができます。
2. **静的か動的か** - キーの値が複雑なオブジェクト、すなわちオブジェクトまたはオブジェクトの配列である場合、それが変更対象であるかどうか確認してください。新しいキーがめったに追加されないオブジェクトで、新しいキーの追加が予測可能で [`ALTER TABLE ADD COLUMN`](/docs/ja/sql-reference/statements/alter/column#add-column) によるスキーマ変更で対応できる場合は、**静的**とみなすことができます。これは、いくつかの JSON ドキュメントでのみキーのサブセットが提供されるオブジェクトを含みます。新しいキーが頻繁に追加され、または予測できないオブジェクトは **動的**とみなされるべきです。値が **静的** か **動的** かを確認するには、関連するセクション [**静的オブジェクトの処理**](/docs/ja/integrations/data-formats/json/schema#handling-static-objects) および [**動的オブジェクトの処理**](/docs/ja/integrations/data-formats/json/schema#handling-dynamic-objects) を参照してください。

<p></p>

**重要:** 上述のルールは再帰的に適用されるべきです。キーの値が動的であると判断された場合、それ以上の評価は不要であり、[**動的オブジェクトの処理**](/docs/ja/integrations/data-formats/json/schema#handling-dynamic-objects) のガイドラインに従うことができます。オブジェクトが静的である場合、キーの値がプリミティブであるか動的キーに遭遇するまでサブキーを評価し続けます。

これらのルールを説明するために、人を表す次の JSON 例を使用します:

```json
{
  "id": 1,
  "name": "Clicky McCliickHouse",
  "username": "Clicky",
  "email": "clicky@clickhouse.com",
  "address": [
    {
      "street": "Victor Plains",
      "suite": "Suite 879",
      "city": "Wisokyburgh",
      "zipcode": "90566-7771",
      "geo": {
        "lat": -43.9509,
        "lng": -34.4618
      }
    }
  ],
  "phone_numbers": ["010-692-6593", "020-192-3333"],
  "website": "clickhouse.com",
  "company": {
    "name": "ClickHouse",
    "catchPhrase": "The real-time data warehouse for analytics",
    "labels": {
      "type": "database systems",
      "founded": "2021"
    }
  },
  "dob": "2007-03-31",
  "tags": {
    "hobby": "Databases",
    "holidays": [
      {
        "year": 2024,
        "location": "Azores, Portugal"
      }
    ],
    "car": {
      "model": "Tesla",
      "year": 2023
    }
  }
}
```

これらのルールを適用すると：

- ルートキー `name`、`username`、`email`、`website` は `String` 型として表現できます。`phone_numbers` カラムはタイプ `Array(String)` のプリミティブの配列であり、`dob` と `id` はそれぞれ `Date` と `UInt32` 型です。
- `address` オブジェクトには新しいキーが追加されません（新しいアドレスオブジェクトのみ）。したがって、**静的**とみなすことができます。再帰すると、すべてのサブカラムは（`geo` を除いて）プリミティブ（および `String` 型）とみなすことができます。これも `lat` および `lon` の 2 つの `Float32` カラムを持つ静的構造です。
- `tags` カラムは **動的** です。このオブジェクトに任意のタグが追加され、構造の変更があると仮定します。
- `company` オブジェクトは **静的** で、指定された最大 3 つのキーしか持ちません。サブキー `name` および `catchPhrase` は `String` 型です。キー `labels` は **動的** です。このオブジェクトに任意のタグを追加できると仮定します。値は常にタイプ文字列のキーと値のペアになります。

## 静的オブジェクトの処理

静的オブジェクトには名前付きタプル、すなわち `Tuple` を使用することをお勧めします。オブジェクトの配列はタプルの配列、すなわち `Array(Tuple)` を使用して保持できます。タプル内では、カラムとそれに対応する型は同じルールを使用して定義されるべきです。これは、以下に示すように、ネストされたオブジェクトを表すネストされた `Tuple` を導く可能性があります。

これを示すために、前述の JSON の人の例を使用し、動的オブジェクトを省略します：

```json
{
  "id": 1,
  "name": "Clicky McCliickHouse",
  "username": "Clicky",
  "email": "clicky@clickhouse.com",
  "address": [
    {
      "street": "Victor Plains",
      "suite": "Suite 879",
      "city": "Wisokyburgh",
      "zipcode": "90566-7771",
      "geo": {
        "lat": -43.9509,
        "lng": -34.4618
      }
    }
  ],
  "phone_numbers": ["010-692-6593", "020-192-3333"],
  "website": "clickhouse.com",
  "company": {
    "name": "ClickHouse",
    "catchPhrase": "The real-time data warehouse for analytics"
  },
  "dob": "2007-03-31"
}
```

このテーブルのスキーマは以下のようになります：

```sql
CREATE TABLE people
(
    `id` Int64,
    `name` String,
    `username` String,
    `email` String,
    `address` Array(Tuple(city String, geo Tuple(lat Float32, lng Float32), street String, suite String, zipcode String)),
    `phone_numbers` Array(String),
    `website` String,
    `company` Tuple(catchPhrase String, name String),
    `dob` Date
)
ENGINE = MergeTree
ORDER BY username
```

`company` カラムが `Tuple(catchPhrase String, name String)` として定義されていることに注意してください。`address` フィールドはネストされた `Tuple` を持つ `Array(Tuple)` を使用しています。

JSON は現状の構造でこのテーブルに挿入できます：

```sql
INSERT INTO people FORMAT JSONEachRow
{"id":1,"name":"Clicky McCliickHouse","username":"Clicky","email":"clicky@clickhouse.com","address":[{"street":"Victor Plains","suite":"Suite 879","city":"Wisokyburgh","zipcode":"90566-7771","geo":{"lat":-43.9509,"lng":-34.4618}}],"phone_numbers":["010-692-6593","020-192-3333"],"website":"clickhouse.com","company":{"name":"ClickHouse","catchPhrase":"The real-time data warehouse for analytics"},"dob":"2007-03-31"}
```

上記の例ではデータが最小限ですが、以下に示すように、タプルフィールドをピリオド区切り名でクエリできます。

```sql
SELECT
    address.street,
    company.name
FROM people

┌─address.street────┬─company.name─┐
│ ['Victor Plains'] │ ClickHouse   │
└───────────────────┴──────────────┘
```

`address.street` カラムが `Array` として返される方法に注意してください。配列内の特定のオブジェクトを位置によってクエリするには、配列のオフセットをカラム名の後に指定する必要があります。例えば、最初の住所から通りを取得するには：

```sql
SELECT address.street[1] AS street
FROM people

┌─street────────┐
│ Victor Plains │
└───────────────┘

1 row in set. Elapsed: 0.001 sec.
```

タプルの主な欠点は、サブカラムを注文キーとして使用できないことです。したがって、以下は失敗します：

```sql
CREATE TABLE people
(
    `id` Int64,
    `name` String,
    `username` String,
    `email` String,
    `address` Array(Tuple(city String, geo Tuple(lat Float32, lng Float32), street String, suite String, zipcode String)),
    `phone_numbers` Array(String),
    `website` String,
    `company` Tuple(catchPhrase String, name String),
    `dob` Date
)
ENGINE = MergeTree
ORDER BY company.name

Code: 47. DB::Exception: Missing columns: 'company.name' while processing query: 'company.name', required columns: 'company.name' 'company.name'. (UNKNOWN_IDENTIFIER)
```

:::note 注文キー内のタプル
タプルカラムは注文キーに使用できませんが、タプル全体を使用することができます。ただし、これはあまり意味をなさない場合が多いです。
:::

### デフォルト値の処理

JSON オブジェクトは構造化されていても、しばしば既知のキーのサブセットしか提供されません。幸いにも、`Tuple` 型は JSON ペイロードのすべてのカラムを要求するわけではありません。指定されていない場合、デフォルトの値が使用されます。

前述の `people` テーブルと、`suite`、`geo`、`phone_numbers` および `catchPhrase` キーが欠けている以下のスパースな JSON を考えてみてください。

```json
{
  "id": 1,
  "name": "Clicky McCliickHouse",
  "username": "Clicky",
  "email": "clicky@clickhouse.com",
  "address": [
    {
      "street": "Victor Plains",
      "city": "Wisokyburgh",
      "zipcode": "90566-7771"
    }
  ],
  "website": "clickhouse.com",
  "company": {
    "name": "ClickHouse"
  },
  "dob": "2007-03-31"
}
```

この行が正常に挿入されることが以下に示されています：

```sql
INSERT INTO people FORMAT JSONEachRow
{"id":1,"name":"Clicky McCliickHouse","username":"Clicky","email":"clicky@clickhouse.com","address":[{"street":"Victor Plains","city":"Wisokyburgh","zipcode":"90566-7771"}],"website":"clickhouse.com","company":{"name":"ClickHouse"},"dob":"2007-03-31"}

Ok.

1 row in set. Elapsed: 0.002 sec.
```

この 1 行をクエリすると、（サブオブジェクトを含む）省略されたカラムに対してデフォルト値が使用されていることが確認できます：

```sql
SELECT *
FROM people
FORMAT PrettyJSONEachRow

{
    "id": "1",
    "name": "Clicky McCliickHouse",
    "username": "Clicky",
    "email": "clicky@clickhouse.com",
    "address": [
        {
            "city": "Wisokyburgh",
            "geo": {
                "lat": 0,
                "lng": 0
            },
            "street": "Victor Plains",
            "suite": "",
            "zipcode": "90566-7771"
        }
    ],
    "phone_numbers": [],
    "website": "clickhouse.com",
    "company": {
        "catchPhrase": "",
        "name": "ClickHouse"
    },
    "dob": "2007-03-31"
}

1 row in set. Elapsed: 0.001 sec.
```

:::note 空と null の区別
値が空であることと提供されないことを区別する必要がある場合は、[Nullable 型](/docs/ja/sql-reference/data-types/nullable) を使用することができます。これが絶対に必要でない限り、[Nullable カラムは避ける](/docs/ja/cloud/bestpractices/avoid-nullable-columns) べきです。なぜなら、これによりこれらのカラムのストレージとクエリのパフォーマンスが悪化するためです。
:::

### 新しいカラムの処理

JSON キーが静的である場合、構造化されたアプローチが最も簡単ですが、新しいキーが事前に知られていて、スキーマに応じて変更可能な場合、このアプローチを使用することも可能です。

ClickHouse はデフォルトで、スキーマに存在しない JSON キーをペイロードに提供しても無視します。以下のように、`nickname` キーが追加された修正済み JSON ペイロードを考えてみてください：

```json
{
  "id": 1,
  "name": "Clicky McCliickHouse",
  "nickname": "Clicky",
  "username": "Clicky",
  "email": "clicky@clickhouse.com",
  "address": [
    {
      "street": "Victor Plains",
      "suite": "Suite 879",
      "city": "Wisokyburgh",
      "zipcode": "90566-7771",
      "geo": {
        "lat": -43.9509,
        "lng": -34.4618
      }
    }
  ],
  "phone_numbers": ["010-692-6593", "020-192-3333"],
  "website": "clickhouse.com",
  "company": {
    "name": "ClickHouse",
    "catchPhrase": "The real-time data warehouse for analytics"
  },
  "dob": "2007-03-31"
}
```

`nickname` キーが無視された状態でこの JSON を正常に挿入できます：

```sql
INSERT INTO people FORMAT JSONEachRow
{"id":1,"name":"Clicky McCliickHouse","nickname":"Clicky","username":"Clicky","email":"clicky@clickhouse.com","address":[{"street":"Victor Plains","suite":"Suite 879","city":"Wisokyburgh","zipcode":"90566-7771","geo":{"lat":-43.9509,"lng":-34.4618}}],"phone_numbers":["010-692-6593","020-192-3333"],"website":"clickhouse.com","company":{"name":"ClickHouse","catchPhrase":"The real-time data warehouse for analytics"},"dob":"2007-03-31"}

Ok.

1 row in set. Elapsed: 0.002 sec.
```

カラムは [`ALTER TABLE ADD COLUMN`](/ja/sql-reference/statements/alter/column#add-column) コマンドを使用してスキーマに追加できます。 `DEFAULT` 句を介してデフォルトを指定することができ、これはその後の挿入で指定されていない場合に使用されます。この値が存在しない行（作成前に挿入された行）に対しても、このデフォルト値が返されます。 `DEFAULT` 値が指定されていない場合、型に対するデフォルト値が使用されます。

例えば：

```sql
-- 初期行を挿入（nickname は無視されます）
INSERT INTO people FORMAT JSONEachRow
{"id":1,"name":"Clicky McCliickHouse","nickname":"Clicky","username":"Clicky","email":"clicky@clickhouse.com","address":[{"street":"Victor Plains","suite":"Suite 879","city":"Wisokyburgh","zipcode":"90566-7771","geo":{"lat":-43.9509,"lng":-34.4618}}],"phone_numbers":["010-692-6593","020-192-3333"],"website":"clickhouse.com","company":{"name":"ClickHouse","catchPhrase":"The real-time data warehouse for analytics"},"dob":"2007-03-31"}

-- カラムを追加
ALTER TABLE people
    (ADD COLUMN `nickname` String DEFAULT 'no_nickname')

-- 新しい行を挿入（同じデータ、異なるID）
INSERT INTO people FORMAT JSONEachRow
{"id":2,"name":"Clicky McCliickHouse","nickname":"Clicky","username":"Clicky","email":"clicky@clickhouse.com","address":[{"street":"Victor Plains","suite":"Suite 879","city":"Wisokyburgh","zipcode":"90566-7771","geo":{"lat":-43.9509,"lng":-34.4618}}],"phone_numbers":["010-692-6593","020-192-3333"],"website":"clickhouse.com","company":{"name":"ClickHouse","catchPhrase":"The real-time data warehouse for analytics"},"dob":"2007-03-31"}

-- 2 行を選択
SELECT id, nickname FROM people

┌─id─┬─nickname────┐
│  2 │ Clicky      │
│  1 │ no_nickname │
└────┴─────────────┘

2 rows in set. Elapsed: 0.001 sec.
```

## 動的オブジェクトの処理

動的オブジェクトの処理には、次の 2 つの推奨アプローチがあります：

- [Map(String,V)](/docs/ja/sql-reference/data-types/map) 型
- [String](/docs/ja/sql-reference/data-types/string) を使用した JSON 関数

以下のルールを適用して、最も適切なものを決定できます。

1. オブジェクトが非常に動的で、予測可能な構造がなく、任意のネストされたオブジェクトを含む場合、`String` 型を使用することをお勧めします。必要なフィールドはクエリ時に JSON 関数を使用して抽出できます。
2. オブジェクトが主に 1 つのタイプの任意のキーを格納するために使用されている場合、`Map` 型を検討します。理想的には、ユニークなキーの数は数百を超えないべきです。`Map` 型は、ラベルやタグ、例えばログデータ内の Kubernetes ポッドラベルに使用されるべきです。

<br />

:::note オブジェクトレベルのアプローチの適用
同じスキーマ内で異なるオブジェクトに異なる技術が適用される可能性があります。一部のオブジェクトは `String` で最善に解決し、他のオブジェクトは `Map` を適用します。`String` 型が使用されると、これ以上スキーマの決定を行う必要はありません。対照的に、`Map` キーとしてサブオブジェクト（JSON を表す `String` を含む）をネストすることが可能です。
:::

### String を使用する

動的な JSON を持つユーザーにとって、構造化されたアプローチを使用することはしばしば現実的ではないため、スキーマが十分に理解されていないか変更対象となるためです。絶対の柔軟性を得るために、ユーザーはデータを `String` として格納し、必要に応じて関数を使用してフィールドを抽出することができます。これは、構造化されたオブジェクトとして JSON を処理することの対極を表します。この柔軟性は、クエリ構文の複雑化およびパフォーマンスの低下という形でコストを負います。

前述の、[オリジナルの person オブジェクト](/docs/ja/integrations/data-formats/json/schema#static-vs-dynamic-json) を例にすると、`tags` カラムの構造が確保できません。オリジナルの行を挿入し（`company.labels` も含めますが、ここでは無視します）、`Tags` カラムを `String` と宣言します：

```sql
CREATE TABLE people
(
    `id` Int64,
    `name` String,
    `username` String,
    `email` String,
    `address` Array(Tuple(city String, geo Tuple(lat Float32, lng Float32), street String, suite String, zipcode String)),
    `phone_numbers` Array(String),
    `website` String,
    `company` Tuple(catchPhrase String, name String),
    `dob` Date,
    `tags` String
)
ENGINE = MergeTree
ORDER BY username

INSERT INTO people FORMAT JSONEachRow
{"id":1,"name":"Clicky McCliickHouse","username":"Clicky","email":"clicky@clickhouse.com","address":[{"street":"Victor Plains","suite":"Suite 879","city":"Wisokyburgh","zipcode":"90566-7771","geo":{"lat":-43.9509,"lng":-34.4618}}],"phone_numbers":["010-692-6593","020-192-3333"],"website":"clickhouse.com","company":{"name":"ClickHouse","catchPhrase":"The real-time data warehouse for analytics","labels":{"type":"database systems","founded":"2021"}},"dob":"2007-03-31","tags":{"hobby":"Databases","holidays":[{"year":2024,"location":"Azores, Portugal"}],"car":{"model":"Tesla","year":2023}}}

Ok.
1 row in set. Elapsed: 0.002 sec.
```

`tags` カラムを選択すると、JSON が文字列として挿入されたことが見て取れます：

```sql
SELECT tags
FROM people

┌─tags───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"hobby":"Databases","holidays":[{"year":2024,"location":"Azores, Portugal"}],"car":{"model":"Tesla","year":2023}} │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

[JSONExtract](/docs/ja/sql-reference/functions/json-functions#jsonextract-functions) 関数を使用して、この JSON から値を取得できます。以下の簡単な例をご覧ください：

```sql
SELECT JSONExtractString(tags, 'holidays') as holidays FROM people

┌─holidays──────────────────────────────────────┐
│ [{"year":2024,"location":"Azores, Portugal"}] │
└───────────────────────────────────────────────┘

1 row in set. Elapsed: 0.002 sec.
```

関数が JSON 内のパスを抽出するために `String` カラム `tags` と JSON 内のパスが必要なことに注意してください。ネストされたパスには、関数をネストする必要があります。例えば、 `tags.car.year` カラムを抽出するための `JSONExtractUInt(JSONExtractString(tags, 'car'), 'year')` です。ネストされたパスの抽出は、関数 [JSON_QUERY](/docs/ja/sql-reference/functions/json-functions.md/#json_queryjson-path) もしくは [JSON_VALUE](/docs/ja/sql-reference/functions/json-functions.md/#json_valuejson-path) を通じて簡素化することができます。

`arxiv` データセットを考えてみて、すべての本文を `String` として扱うケースの極端な例を考えてみましょう。

```sql
CREATE TABLE arxiv (
  body String
)
ENGINE = MergeTree ORDER BY ()
```

このスキーマに挿入するには、`JSONAsString` フォーマットを使用する必要があります：

```sql
INSERT INTO arxiv SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/arxiv/arxiv.json.gz', 'JSONAsString')

0 rows in set. Elapsed: 25.186 sec. Processed 2.52 million rows, 1.38 GB (99.89 thousand rows/s., 54.79 MB/s.)
```

年ごとにリリースされた論文の数をカウントする必要があるとします。スキーマの [構造化バージョン](/docs/ja/integrations/data-formats/json/inference#creating-tables) と、単一の文字列を使用した場合のクエリを比較してみましょう：

```sql
-- 構造化スキーマを使用
SELECT
    toYear(parseDateTimeBestEffort(versions.created[1])) AS published_year,
    count() AS c
FROM arxiv_v2
GROUP BY published_year
ORDER BY c ASC
LIMIT 10

┌─published_year─┬─────c─┐
│           1986 │     1 │
│           1988 │     1 │
│           1989 │     6 │
│           1990 │    26 │
│           1991 │   353 │
│           1992 │  3190 │
│           1993 │  6729 │
│           1994 │ 10078 │
│           1995 │ 13006 │
│           1996 │ 15872 │
└────────────────┴───────┘

10 rows in set. Elapsed: 0.264 sec. Processed 2.31 million rows, 153.57 MB (8.75 million rows/s., 582.58 MB/s.)

-- 非構造化文字列を使用

SELECT
    toYear(parseDateTimeBestEffort(JSON_VALUE(body, '$.versions[0].created'))) AS published_year,
    count() AS c
FROM arxiv
GROUP BY published_year
ORDER BY published_year ASC
LIMIT 10

┌─published_year─┬─────c─┐
│           1986 │     1 │
│           1988 │     1 │
│           1989 │     6 │
│           1990 │    26 │
│           1991 │   353 │
│           1992 │  3190 │
│           1993 │  6729 │
│           1994 │ 10078 │
│           1995 │ 13006 │
│           1996 │ 15872 │
└────────────────┴───────┘

10 rows in set. Elapsed: 1.281 sec. Processed 2.49 million rows, 4.22 GB (1.94 million rows/s., 3.29 GB/s.)
Peak memory usage: 205.98 MiB.
```

ここで、`JSON_VALUE(body, '$.versions[0].created')` のようにクエリで JSON をメソッドによってフィルターするために xpath 式を使用していることに注意してください。

文字列関数は、インデックスを伴う明示的な型変換よりも顕著に遅いため、上記のクエリは常にフルテーブルスキャンとすべての行の処理を必要とします。このような小さなデータセットでは、クエリは依然として高速である可能性がありますが、大規模なデータセットにおいてはパフォーマンスが低下する可能性があります。

このアプローチの柔軟性は明確なパフォーマンスと構文のコストがかかるため、スキーマ内の非常に動的なオブジェクトにのみ使用するべきです。

#### シンプルJSON関数

上記の例では、JSON* 関数ファミリーを使用しています。これらの関数は、[simdjson](https://github.com/simdjson/simdjson) に基づいた厳密な JSON パーサーを利用し、異なるレベルでネストされた場合のフィールドを区別します。これらの関数は、構文的には正しいがフォーマットが整っていない JSON、例としてフィールド間に二重スペースがある場合を処理することができます。

より高速で厳格な一連の関数が利用可能です。これらの `simpleJSON*` 関数は、JSON の構造とフォーマットに対して厳しい前提を作ることによって主に性能を向上させます。具体的には：

* フィールド名は定数である必要があります。
* フィールド名のエンコーディングが一貫していること。例：`simpleJSONHas('{"abc":"def"}', 'abc') = 1` ですが、`visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0` です。
* フィールド名はすべてのネストされた構造で一意である必要があります。ネストレベル間の区別はなく、マッチングは無差別です。複数のマッチングフィールドがある場合、最初の出現が使用されます。
* ストリングリテラル外での特殊文字はありません。これにはスペースが含まれます。次の例は無効であり、パースされません。

    ```json
    {"@timestamp": 893964617, "clientip": "40.135.0.0", "request": {"method": "GET",
    "path": "/images/hm_bg.jpg", "version": "HTTP/1.0"}, "status": 200, "size": 24736}
    ```

    一方、次の例は正しく解析されます：

    ```json
    {"@timestamp":893964617,"clientip":"40.135.0.0","request":{"method":"GET",
    "path":"/images/hm_bg.jpg","version":"HTTP/1.0"},"status":200,"size":24736}
    ```

これらの関数が適切であり、性能が重要で JSON が上記の要件を満たす場合に、使用することができます。以下は、`simpleJSON*` 関数を使用して再書かれた前述のクエリの例です：

```sql
SELECT
    toYear(parseDateTimeBestEffort(simpleJSONExtractString(simpleJSONExtractRaw(body, 'versions'), 'created'))) AS published_year,
    count() AS c
FROM arxiv
GROUP BY published_year
ORDER BY published_year ASC
LIMIT 10

┌─published_year─┬─────c─┐
│           1986 │     1 │
│           1988 │     1 │
│           1989 │     6 │
│           1990 │    26 │
│           1991 │   353 │
│           1992 │  3190 │
│           1993 │  6729 │
│           1994 │ 10078 │
│           1995 │ 13006 │
│           1996 │ 15872 │
└────────────────┴───────┘

10 rows in set. Elapsed: 0.964 sec. Processed 2.48 million rows, 4.21 GB (2.58 million rows/s., 4.36 GB/s.)
Peak memory usage: 211.49 MiB.
```

上記の例では、公開日付用に最初の値を取得するために `simpleJSONExtractString` を使用して `created` キーを抽出しています。この場合、性能向上のため `simpleJSON*` 関数の制限が受け入れられます。

### Map を使用する

オブジェクトが主に 1 つのタイプの任意のキーを格納するために使用されている場合、`Map` 型を検討します。理想的には、ユニークなキーの数は数百を超えないべきです。`Map` 型は、ラベルやタグ、例えばログデータ内の Kubernetes ポッドラベルに使用されるべきです。`Map` がサポートするオブジェクトの構築にはいくつかの制限があります：

- フィールドはすべて同じ型である必要があります。
- サブカラムにアクセスするには、特殊なマップ構文が必要です。フィールドはカラムとして存在せず、オブジェクト全体がカラムです。
- サブカラムにアクセスすると、`Map` 値全体、すなわちすべての兄弟とその各々の値がロードされます。大きなマップでは、これが重大なパフォーマンスペナルティになる可能性があります。

:::note 文字列キー
オブジェクトを `Map` としてモデリングする場合、JSON キー名を格納するために `String` キーが使用されます。したがって、マップは常に `Map(String, T)` となり、`T` はデータによって異なります。
:::

#### プリミティブ値

`Map` を最も単純に適用する方法は、オブジェクトが同じプリミティブ型の値を含む場合です。ほとんどのケースでは、値 `T` に `String` 型を使用することが含まれます。

前述の [people の JSON](/docs/ja/integrations/data-formats/json/schema#static-vs-dynamic-json) を考えた際、`company.labels` オブジェクトが動的であると判断しました。重要なのは、このオブジェクトには `String` 型のキー値ペアが追加されると考えられることです。したがって、これを `Map(String, String)` として宣言できます：

```sql
CREATE TABLE people
(
    `id` Int64,
    `name` String,
    `username` String,
    `email` String,
    `address` Array(Tuple(city String, geo Tuple(lat Float32, lng Float32), street String, suite String, zipcode String)),
    `phone_numbers` Array(String),
    `website` String,
    `company` Tuple(catchPhrase String, name String, labels Map(String,String)),
    `dob` Date,
    `tags` String
)
ENGINE = MergeTree
ORDER BY username
```

元の完全な JSON オブジェクトを挿入できます：

```sql
INSERT INTO people FORMAT JSONEachRow
{"id":1,"name":"Clicky McCliickHouse","username":"Clicky","email":"clicky@clickhouse.com","address":[{"street":"Victor Plains","suite":"Suite 879","city":"Wisokyburgh","zipcode":"90566-7771","geo":{"lat":-43.9509,"lng":-34.4618}}],"phone_numbers":["010-692-6593","020-192-3333"],"website":"clickhouse.com","company":{"name":"ClickHouse","catchPhrase":"The real-time data warehouse for analytics","labels":{"type":"database systems","founded":"2021"}},"dob":"2007-03-31","tags":{"hobby":"Databases","holidays":[{"year":2024,"location":"Azores, Portugal"}],"car":{"model":"Tesla","year":2023}}}

Ok.

1 row in set. Elapsed: 0.002 sec.
```

リクエストオブジェクト内のフィールドをクエリする際には、以下のようなマップ構文が必要です：

```sql
SELECT company.labels FROM people

┌─company.labels───────────────────────────────┐
│ {'type':'database systems','founded':'2021'} │
└──────────────────────────────────────────────┘

1 row in set. Elapsed: 0.001 sec.

SELECT company.labels['type'] AS type FROM people

┌─type─────────────┐
│ database systems │
└──────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

この型をクエリするための `Map` 関数の完全なセットが [ここに](/docs/ja/sql-reference/functions/tuple-map-functions.md) 示されています。データが一貫した型でない場合、[必要な型の型変換](/docs/ja/sql-reference/functions/type-conversion-functions) を行う関数が存在します。

#### オブジェクト値

オブジェクトが一貫性のあるタイプを持つサブオブジェクトを持つ場合も、`Map` 型を考慮できます。

たとえば、`tags` キー用の `persons` オブジェクトが一貫した構造を要求する場合、各 `tag` のサブオブジェクトには `name` と `time` カラムが含まれます。簡素化されたこのような JSON ドキュメントの例は以下のようになります：

```json
{
  "id": 1,
  "name": "Clicky McCliickHouse",
  "username": "Clicky",
  "email": "clicky@clickhouse.com",
  "tags": {
    "hobby": {
      "name": "Diving",
      "time": "2024-07-11 14:18:01"
    },
    "car": {
      "name": "Tesla",
      "time": "2024-07-11 15:18:23"
    }
  }
}
```

これは、`Map(String, Tuple(name String, time DateTime))` を使用してモデリングすることができます。以下に示します：

```sql
CREATE TABLE people
(
    `id` Int64,
    `name` String,
    `username` String,
    `email` String,
    `tags` Map(String, Tuple(name String, time DateTime))
)
ENGINE = MergeTree
ORDER BY username

INSERT INTO people FORMAT JSONEachRow
{"id":1,"name":"Clicky McCliickHouse","username":"Clicky","email":"clicky@clickhouse.com","tags":{"hobby":{"name":"Diving","time":"2024-07-11 14:18:01"},"car":{"name":"Tesla","time":"2024-07-11 15:18:23"}}}

Ok.

1 row in set. Elapsed: 0.002 sec.

SELECT tags['hobby'] AS hobby
FROM people
FORMAT JSONEachRow

{"hobby":{"name":"Diving","time":"2024-07-11 14:18:01"}}

1 row in set. Elapsed: 0.001 sec.
```

このケースでのマップの適用は一般的には稀であり、データが再モデリングされ、動的キー名がサブオブジェクトを持たないようにすることが適切であることを示唆します。例えば、上記の例を以下のように再モデリングすることで、`Array(Tuple(key String, name String, time DateTime))` を使用できるようになります。

```json
{
  "id": 1,
  "name": "Clicky McCliickHouse",
  "username": "Clicky",
  "email": "clicky@clickhouse.com",
  "tags": [
    {
      "key": "hobby",
      "name": "Diving",
      "time": "2024-07-11 14:18:01"
    },
    {
      "key": "car",
      "name": "Tesla",
      "time": "2024-07-11 15:18:23"
    }
  ]
}
```
