---
sidebar_label: ClickHouse Playground
sidebar_position: 2
keywords: [clickhouse, playground, getting, started, docs]
description: ClickHouse Playgroundを使用すると、サーバーやクラスタを設定せずにクエリを即座に実行して、ClickHouseを試すことができます。
slug: /ja/getting-started/playground
---

# ClickHouse Playground

[ClickHouse Playground](https://sql.clickhouse.com)を使用すると、サーバーやクラスタを設定せずにクエリを即座に実行して、ClickHouseを試すことができます。
Playgroundにはいくつかのサンプルデータセットが用意されています。

Playgroundに対してクエリを行うには、任意のHTTPクライアントを使用できます。例えば、[curl](https://curl.haxx.se)や[wget](https://www.gnu.org/software/wget/)を使用する方法や、[JDBC](../interfaces/jdbc.md)や[ODBC](../interfaces/odbc.md)ドライバを使用して接続を設定する方法があります。ClickHouseをサポートするソフトウェア製品に関する詳細情報は[こちら](../integrations/index.mdx)で確認できます。

## 認証情報 {#credentials}

| パラメータ          | 値                                 |
|:-------------------|:-----------------------------------|
| HTTPSエンドポイント | `https://play.clickhouse.com:443/` |
| ネイティブTCPエンドポイント | `play.clickhouse.com:9440`         |
| ユーザー            | `explorer` または `play`          |
| パスワード          | （空）                             |

## 制限事項 {#limitations}

クエリは読み取り専用のユーザーとして実行されます。それはいくつかの制限を意味します：

- DDLクエリは許可されていません
- INSERTクエリは許可されていません

サービスの使用にはクォータもあります。

## 例 {#examples}

`curl`を使用したHTTPSエンドポイントの例：

``` bash
curl "https://play.clickhouse.com/?user=explorer" --data-binary "SELECT 'Play ClickHouse'"
```

[CLI](../interfaces/cli.md)を使用したTCPエンドポイントの例：

``` bash
clickhouse client --secure --host play.clickhouse.com --user explorer
```
