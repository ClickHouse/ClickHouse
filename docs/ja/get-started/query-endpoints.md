---
sidebar_title: クエリ API エンドポイント
slug: /ja/get-started/query-endpoints
description: 保存したクエリから簡単にREST APIエンドポイントを作成
keywords: [api, query api endpoints, query endpoints, query rest api]
---

import BetaBadge from '@theme/badges/BetaBadge';

# クエリ API エンドポイント

<BetaBadge />

**クエリ API エンドポイント** 機能を使用すると、ClickHouse Cloud コンソール内の任意の保存した SQL クエリから直接 API エンドポイントを作成できます。ネイティブドライバを使ってClickHouse Cloudサービスに接続せずに、HTTP経由でAPIエンドポイントにアクセスして保存したクエリを実行することが可能になります。

## クイックスタートガイド

始める前に、APIキーと管理コンソールのロールを確認してください。[APIキーを作成する](/docs/ja/cloud/manage/openapi)方法についてはこちらを参照してください。

### 保存されたクエリを作成

すでに保存されたクエリがある場合、このステップをスキップできます。

新しいクエリタブを開きます。デモンストレーションのために、約45億レコードを含む [youtube データセット](/docs/ja/getting-started/example-datasets/youtube-dislikes) を使用します。以下の例では、ユーザーが入力する `year` パラメーターに基づいて、ビデオあたりの平均視聴数によるトップ10のアップローダーを返します：

```sql
with sum(view_count) as view_sum,
    round(view_sum / num_uploads, 2) as per_upload
select
    uploader,
    count() as num_uploads,
    formatReadableQuantity(view_sum) as total_views,
    formatReadableQuantity(per_upload) as views_per_video
from
    youtube
where
    toYear(upload_date) = {year: UInt16}
group by uploader
order by per_upload desc
limit 10
```

注意すべきは、このクエリはパラメーター（`year`）を含んでいることです。SQLコンソールのクエリエディタはClickHouseのクエリパラメーター式を自動的に検出し、各パラメーターの入力を提供します。このクエリが機能することを確認するために、速やかに実行してみましょう：

![例クエリのテスト](@site/docs/ja/cloud/images/sqlconsole/endpoints-testquery.png)

次に、クエリを保存します：

![例クエリの保存](@site/docs/ja/cloud/images/sqlconsole/endpoints-savequery.png)

保存されたクエリに関するさらに詳しいドキュメントは[こちら](/docs/ja/get-started/sql-console#saving-a-query)から確認できます。

### クエリ API エンドポイントの設定

クエリ API エンドポイントは、クエリビューから直接、**共有** ボタンをクリックし、`API Endpoint` を選択することで設定できます。どのAPIキーがエンドポイントにアクセス可能であるかを指定するようプロンプトが表示されます：

![クエリエンドポイントの設定](@site/docs/ja/cloud/images/sqlconsole/endpoints-configure.png)

APIキーを選択すると、クエリAPIエンドポイントが自動的に設定されます。テストリクエストを送信できるように例として `curl` コマンドが表示されます：

![エンドポイントcurlコマンド](@site/docs/ja/cloud/images/sqlconsole/endpoints-completed.png)

### クエリ API パラメーター

クエリ内のパラメーターは `{parameter_name: type}` の構文で指定できます。これらのパラメーターは自動的に検出され、例のリクエストペイロードにはこれらのパラメーターを渡せる `queryVariables` オブジェクトが含まれます。

### テストと監視

クエリ API エンドポイントが作成されたら、`curl`や他のHTTPクライアントを使って機能することを確認できます：
<img src={require('@site/docs/ja/cloud/images/sqlconsole/endpoints-curltest.png').default} class="image" alt="エンドポイントcurlテスト" style={{width: '80%', background:'none'}} />

最初のリクエストを送信すると、**共有** ボタンの右側に新しいボタンが直ちに表示されるはずです。これをクリックすると、クエリに関する監視データを含むフライアウトが開きます：

![エンドポイント監視](@site/docs/ja/cloud/images/sqlconsole/endpoints-monitoring.png)

## 実装の詳細

### 説明

このルートは、指定されたクエリエンドポイントでクエリを実行します。異なるバージョン、フォーマット、クエリ変数をサポートしています。応答はストリームとして送信 (_バージョン2のみ_) できるか、単一のペイロードとして返されます。

### 認証

- **必須**: はい
- **メソッド**: OpenAPIキー/シークレット経由の基本認証
- **権限**: クエリエンドポイントに対する適切な権限を持つ

### URL パラメーター

- `queryEndpointId` （必須）：実行するクエリエンドポイントの一意の識別子。

### クエリパラメーター

#### V1

なし

#### V2

- `format` （任意）：応答のフォーマット。ClickHouseがサポートするすべてのフォーマットをサポート。
- `param_:name` クエリ内で使用するクエリ変数。`name`はクエリ内の変数名に一致する必要があります。リクエストの本文がストリームの場合にのみ使用します。
- `:clickhouse_setting` どの[ClickHouse設定](https://clickhouse.com/docs/ja/operations/settings/settings)でもクエリパラメーターとして渡すことが可能です。

### ヘッダー

- `x-clickhouse-endpoint-version` （任意）：クエリエンドポイントのバージョン。サポートされているバージョンは`1`と`2`です。提供されない場合、エンドポイントに対して最後に保存されたバージョンがデフォルトで使用されます。
- `x-clickhouse-endpoint-upgrade` （任意）：このヘッダーを設定して、エンドポイントバージョンをアップグレードします。このヘッダーは `x-clickhouse-endpoint-version` ヘッダーと連携します。

### リクエストボディ

- `queryVariables` （任意）：クエリ内で使用する変数を含むオブジェクト。
- `format` （任意）：応答のフォーマット。Query API エンドポイントがバージョン2の場合、ClickHouseがサポートする任意のフォーマットが可能。v1でサポートされるフォーマットは：
  - TabSeparated
  - TabSeparatedWithNames
  - TabSeparatedWithNamesAndTypes
  - JSON
  - JSONEachRow
  - CSV
  - CSVWithNames
  - CSVWithNamesAndTypes

### 応答

- **200 OK**: クエリが正常に実行された。
- **400 Bad Request**: リクエストが不正であった。
- **401 Unauthorized**: 認証なしまたは権限が不十分でリクエストが行われた。
- **404 Not Found**: 指定されたクエリエンドポイントが見つからなかった。

### エラーハンドリング

- リクエストに有効な認証資格情報が含まれていることを確認してください。
- `queryEndpointId` および `queryVariables` が正しいことを確認してください。
- サーバーエラーを適切に処理し、適切なエラーメッセージを返します。

### エンドポイントバージョンのアップグレード

エンドポイントバージョンを`v1`から`v2`にアップグレードするには、リクエストに `x-clickhouse-endpoint-upgrade` ヘッダーを含め、それを`1`に設定します。これによりアップグレードプロセスがトリガされ、`v2`で利用可能な機能や改善点を使用できるようになります。

## 例

### 基本的なリクエスト

**クエリ API エンドポイント SQL:**

```sql
SELECT database, name as num_tables FROM system.tables limit 3;
```

#### バージョン 1

**cURL:**

```bash
curl -X POST 'https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run' \
--user '<openApiKeyId:openApiKeySecret>' \
-H 'Content-Type: application/json' \
-d '{ "format": "JSONEachRow" }'
```

**JavaScript:**

```javascript
fetch(
  "https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run",
  {
    method: "POST",
    headers: {
      Authorization: "Basic <base64_encoded_credentials>",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      format: "JSONEachRow",
    }),
  }
)
  .then((response) => response.json())
  .then((data) => console.log(data))
  .catch((error) => console.error("Error:", error));
```

**応答:**

```json
{
  "data": {
    "columns": [
      {
        "name": "database",
        "type": "String"
      },
      {
        "name": "num_tables",
        "type": "String"
      }
    ],
    "rows": [
      ["INFORMATION_SCHEMA", "COLUMNS"],
      ["INFORMATION_SCHEMA", "KEY_COLUMN_USAGE"],
      ["INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS"]
    ]
  }
}
```

#### バージョン 2

**cURL:**

```bash
curl -X POST 'https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run?format=JSONEachRow' \
--user '<openApiKeyId:openApiKeySecret>' \
-H 'Content-Type: application/json' \
-H 'x-clickhouse-endpoint-version: 2'
```

**JavaScript:**

```javascript
fetch(
  "https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run?format=JSONEachRow",
  {
    method: "POST",
    headers: {
      Authorization: "Basic <base64_encoded_credentials>",
      "Content-Type": "application/json",
      "x-clickhouse-endpoint-version": "2",
    },
  }
)
  .then((response) => response.json())
  .then((data) => console.log(data))
  .catch((error) => console.error("Error:", error));
```

**応答:**

```application/x-ndjson
{"database":"INFORMATION_SCHEMA","num_tables":"COLUMNS"}
{"database":"INFORMATION_SCHEMA","num_tables":"KEY_COLUMN_USAGE"}
{"database":"INFORMATION_SCHEMA","num_tables":"REFERENTIAL_CONSTRAINTS"}
```

### クエリ変数を使用したリクエストおよびJSONCompactEachRowフォーマットのバージョン2

**クエリ API エンドポイント SQL:**

```sql
SELECT name, database FROM system.tables WHERE match(name, {tableNameRegex: String}) AND database = {database: String};
```

**cURL:**

```bash
curl -X POST 'https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run?format=JSONCompactEachRow' \
--user '<openApiKeyId:openApiKeySecret>' \
-H 'Content-Type: application/json' \
-H 'x-clickhouse-endpoint-version: 2' \
-d '{ "queryVariables": { "tableNameRegex": "query.*", "database": "system" } }'
```

**JavaScript:**

```javascript
fetch(
  "https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run?format=JSONCompactEachRow",
  {
    method: "POST",
    headers: {
      Authorization: "Basic <base64_encoded_credentials>",
      "Content-Type": "application/json",
      "x-clickhouse-endpoint-version": "2",
    },
    body: JSON.stringify({
      queryVariables: {
        tableNameRegex: "query.*",
        database: "system",
      },
    }),
  }
)
  .then((response) => response.json())
  .then((data) => console.log(data))
  .catch((error) => console.error("Error:", error));
```

**応答:**

```application/x-ndjson
["query_cache", "system"]
["query_log", "system"]
["query_views_log", "system"]
```

### テーブルにデータを挿入するクエリ変数内で配列を使用したリクエスト

**テーブル SQL:**

```SQL
CREATE TABLE default.t_arr
(
    `arr` Array(Array(Array(UInt32)))
)
ENGINE = MergeTree
ORDER BY tuple()
```

**クエリ API エンドポイント SQL:**

```sql
  INSERT INTO default.t_arr VALUES ({arr: Array(Array(Array(UInt32)))});
```

**cURL:**

```bash
curl -X POST 'https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run' \
--user '<openApiKeyId:openApiKeySecret>' \
-H 'Content-Type: application/json' \
-H 'x-clickhouse-endpoint-version: 2' \
-d '{
  "queryVariables": {
    "arr": [[[12, 13, 0, 1], [12]]]
  }
}'
```

**JavaScript:**

```javascript
fetch(
  "https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run",
  {
    method: "POST",
    headers: {
      Authorization: "Basic <base64_encoded_credentials>",
      "Content-Type": "application/json",
      "x-clickhouse-endpoint-version": "2",
    },
    body: JSON.stringify({
      queryVariables: {
        arr: [[[12, 13, 0, 1], [12]]],
      },
    }),
  }
)
  .then((response) => response.json())
  .then((data) => console.log(data))
  .catch((error) => console.error("Error:", error));
```

**応答:**

```text
OK
```

### max_threads を8にセットしたClickHouse設定を使ったリクエスト

**クエリ API エンドポイント SQL:**

```sql
SELECT * from system.tables;
```

**cURL:**

```bash
curl -X POST 'https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run?max_threads=8,' \
--user '<openApiKeyId:openApiKeySecret>' \
-H 'Content-Type: application/json' \
-H 'x-clickhouse-endpoint-version: 2' \
```

**JavaScript:**

```javascript
fetch(
  "https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run?max_threads=8",
  {
    method: "POST",
    headers: {
      Authorization: "Basic <base64_encoded_credentials>",
      "Content-Type": "application/json",
      "x-clickhouse-endpoint-version": "2",
    },
  }
)
  .then((response) => response.json())
  .then((data) => console.log(data))
  .catch((error) => console.error("Error:", error));
```

### ストリームとしての応答をリクエストおよび解析

**クエリ API エンドポイント SQL:**

```sql
SELECT name, database from system.tables;
```

**Typescript:**

```typescript
async function fetchAndLogChunks(
  url: string,
  openApiKeyId: string,
  openApiKeySecret: string
) {
  const auth = Buffer.from(`${openApiKeyId}:${openApiKeySecret}`).toString(
    "base64"
  );

  const headers = {
    Authorization: `Basic ${auth}`,
    "x-clickhouse-endpoint-version": "2",
  };

  const response = await fetch(url, {
    headers,
    method: "POST",
    body: JSON.stringify({ format: "JSONEachRow" }),
  });

  if (!response.ok) {
    console.error(`HTTP error! Status: ${response.status}`);
    return;
  }

  const reader = response.body as unknown as Readable;
  reader.on("data", (chunk) => {
    console.log(chunk.toString());
  });

  reader.on("end", () => {
    console.log("Stream ended.");
  });

  reader.on("error", (err) => {
    console.error("Stream error:", err);
  });
}

const endpointUrl =
  "https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run?format=JSONEachRow";
const openApiKeyId = "<myOpenApiKeyId>";
const openApiKeySecret = "<myOpenApiKeySecret>";
// Usage example
fetchAndLogChunks(endpointUrl, openApiKeyId, openApiKeySecret).catch((err) =>
  console.error(err)
);
```

**出力**

```shell
> npx tsx index.ts
> {"name":"COLUMNS","database":"INFORMATION_SCHEMA"}
> {"name":"KEY_COLUMN_USAGE","database":"INFORMATION_SCHEMA"}
...
> Stream ended.
```

### ファイルからテーブルにストリームを挿入

以下のコンテンツで、ファイル ./samples/my_first_table_2024-07-11.csv を作成します：

```csv
"user_id","json","name"
"1","{""name"":""John"",""age"":30}","John"
"2","{""name"":""Jane"",""age"":25}","Jane"
```

**テーブル作成 SQL:**

```sql
create table default.my_first_table
(
    user_id String,
    json String,
    name String,
) ENGINE = MergeTree()
ORDER BY user_id;
```

**クエリ API エンドポイント SQL:**

```sql
INSERT INTO default.my_first_table
```

**cURL:**

```bash
cat ./samples/my_first_table_2024-07-11.csv | curl --user '<openApiKeyId:openApiKeySecret>' \
                                                   -X POST \
                                                   -H 'Content-Type: application/octet-stream' \
                                                   -H 'x-clickhouse-endpoint-version: 2' \
                                                   "https://console-api.clickhouse.cloud/.api/query-endpoints/<endpoint id>/run?format=CSV" \
                                                   --data-binary @-
```
