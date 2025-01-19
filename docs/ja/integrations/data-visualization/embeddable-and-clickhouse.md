---
sidebar_label: Embeddable
slug: /ja/integrations/embeddable
keywords: [clickhouse, embeddable, connect, integrate, ui]
description: Embeddableは、迅速でインタラクティブな完全カスタム分析体験をアプリに直接統合するための開発者向けツールキットです。
---

import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# Embeddable と ClickHouse の接続

[Embeddable](https://embeddable.com/) では、コード内で [データモデル](https://trevorio.notion.site/Data-modeling-35637bbbc01046a1bc47715456bfa1d8) と [コンポーネント](https://trevorio.notion.site/Using-components-761f52ac2d0743b488371088a1024e49) を定義し（コードリポジトリに保存）、**SDK** を使用してこれらを強力なEmbeddableの**コード不要ビルダー**で利用できるようにします。

この結果、製品チームがデザインし、エンジニアリングチームが構築し、顧客対応およびデータチームが維持する、迅速でインタラクティブな顧客向けの分析を製品内に直接提供できるようになります。これが理想的な形です。

組み込みの行レベルセキュリティにより、各ユーザーは許可されたデータのみを見ることができます。また、完全に構成可能な2層のキャッシングにより、スケールに応じた迅速なリアルタイム分析を提供できます。

## 1. 接続情報を収集する
<ConnectionDetails />

## 2. ClickHouse接続タイプを作成する

データベース接続は、Embeddable APIを使用して追加します。この接続は、ClickHouseサービスに接続するために使用されます。次のAPI呼び出しを使用して接続を追加できます：

```javascript
// セキュリティ上の理由から、これはクライアント側から*一切*呼び出してはいけません
fetch('https://api.embeddable.com/api/v1/connections', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    Accept: 'application/json',
    Authorization: `Bearer ${apiKey}` /* APIキーを安全に保持してください */,
  },
  body: JSON.stringify({
    name: 'my-clickhouse-db',
    type: 'clickhouse',
    credentials: {
      host: 'my.clickhouse.host',
      user: 'clickhouse_user',
      port: 8443,
      password: '*****',
    },
  }),
});


Response:
Status 201 { errorMessage: null }
```

上記は `CREATE` アクションを示していますが、すべての `CRUD` 操作が利用可能です。

`apiKey` は、Embeddable ダッシュボードの1つで「**Publish**」をクリックして見つけることができます。

`name` は、この接続を識別するための一意の名前です。
- デフォルトでは、データモデルは「default」と呼ばれる接続を探しますが、異なる `data_source` 名をモデルに供給することで、異なる接続に異なるデータモデルを接続できます（単にモデル内でdata_source名を指定してください）。

`type` は、Embeddableが使用するドライバーを指定します。

- ここでは `clickhouse` を使用しますが、1つのEmbeddable ワークスペースに複数の異なるデータソース（例： `postgres`, `bigquery`, `mongodb` など）を接続することができます。

`credentials` は、ドライバーが期待する必要な認証情報を含むJavaScriptオブジェクトです。
- これらは安全に暗号化され、データモデルで記述したとおりのデータだけが取得されます。
Embeddableは、各接続に対して読み取り専用のデータベースユーザーを作成することを強く推奨しています（Embeddableはデータベースから読み取るだけで、書き込みは行いません）。

本番、QA、テストなどの異なるデータベースに接続をサポートするため（または異なる顧客のために異なるデータベースをサポートするために）、各接続を環境に割り当てることができます（[Environments API](https://www.notion.so/Environments-API-497169036b5148b38f7936aa75e62949?pvs=21)を参照）。
