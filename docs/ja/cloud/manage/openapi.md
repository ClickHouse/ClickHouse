---
sidebar_label: APIキーの管理
slug: /ja/cloud/manage/openapi
title: APIキーの管理
---

ClickHouse Cloudは、OpenAPIを利用したAPIを提供しており、アカウントおよびサービスの各種要素をプログラムで管理することができます。

# APIキーの管理

:::note
このドキュメントは、ClickHouse Cloud APIについて説明しています。データベースAPIのエンドポイントについては、[Cloud Endpoints API](/docs/ja/cloud/security/cloud-endpoints-api.md)をご覧ください。
:::

1. 左側のメニューの**API Keys**タブを使用して、APIキーの作成と管理ができます。

  ![ClickHouse Cloud API Keys Tab](@site/docs/ja/_snippets/images/openapi1.png)

2. **API Keys**ページは、最初にAPIキーを作成するためのプロンプトを表示します。最初のキーを作成した後は、画面右上に表示される `New API Key` ボタンを使用して新しいキーを作成できます。

  ![Initial API Screen](@site/docs/ja/_snippets/images/openapi2.png) 
  
3. APIキーを作成するには、キー名、キーの権限、および有効期限を指定し、`Generate API Key` をクリックします。

  ![Create API Key](@site/docs/ja/_snippets/images/openapi3.png)
  
4. 次の画面では、Key ID と Key secret が表示されます。この値をコピーして、金庫などの安全な場所に保管してください。この画面を離れた後は、これらの値は表示されません。

  ![API Key ID and Key Secret](@site/docs/ja/_snippets/images/openapi4.png)

5. ClickHouse Cloud APIは、[HTTP Basic Authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication)を使用してAPIキーの有効性を確認します。以下は、`curl`を使用してClickHouse Cloud APIにリクエストを送信する例です：

```bash
$ KEY_ID=mykeyid
$ KEY_SECRET=mykeysecret

$ curl --user $KEY_ID:$KEY_SECRET https://api.clickhouse.cloud/v1/organizations
```

6. **API Keys**ページに戻ると、キーの名前、Key IDの最後の4文字、権限、ステータス、満期日、および作成者が表示されます。この画面からキー名、権限、および有効期限を編集することができます。キーはこの画面から無効化または削除することもできます。

:::note
APIキーを削除すると、この操作は取り消すことができません。このキーを使用しているサービスは、すぐにClickHouse Cloudへのアクセスができなくなります。
:::

  ![API Key Management](@site/docs/ja/_snippets/images/openapi5.png)

## エンドポイント

[エンドポイントドキュメントはこちら](/docs/ja/cloud/manage/api/invitations-api-reference.md)。API Key と API Secret を利用して、ベースURL `https://api.clickhouse.cloud/v1` を使用してください。
