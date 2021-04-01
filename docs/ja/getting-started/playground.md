---
toc_priority: 14
toc_title: Playground
---

# ClickHouse Playground {#clickhouse-playground}

[ClickHouse Playground](https://play.clickhouse.tech) では、サーバーやクラスタを設定することなく、即座にクエリを実行して ClickHouse を試すことができます。
いくつかの例のデータセットは、Playground だけでなく、ClickHouse の機能を示すサンプルクエリとして利用可能です. また、 ClickHouse の LTS リリースで試すこともできます。

ClickHouse Playground は、[Yandex.Cloud](https://cloud.yandex.com/)にホストされている m2.small [Managed Service for ClickHouse](https://cloud.yandex.com/services/managed-clickhouse) インスタンス(4 vCPU, 32 GB RAM) で提供されています。クラウドプロバイダの詳細情報については[こちら](../commercial/cloud.md)。

任意の HTTP クライアントを使用してプレイグラウンドへのクエリを作成することができます。例えば[curl](https://curl.haxx.se)、[wget](https://www.gnu.org/software/wget/)、[JDBC](../interfaces/jdbc.md)または[ODBC](../interfaces/odbc.md)ドライバを使用して接続を設定します。
ClickHouse をサポートするソフトウェア製品の詳細情報は[こちら](../interfaces/index.md)をご覧ください。

## 資格情報 {#credentials}

| パラメータ                    | 値                                      |
| :---------------------------- | :-------------------------------------- |
| HTTPS エンドポイント          | `https://play-api.clickhouse.tech:8443` |
| ネイティブ TCP エンドポイント | `play-api.clickhouse.tech:9440`         |
| ユーザ名                      | `playgrounnd`                           |
| パスワード                    | `clickhouse`                            |


特定のClickHouseのリリースで試すために、追加のエンドポイントがあります。（ポートとユーザー/パスワードは上記と同じです）。

- 20.3 LTS: `play-api-v20-3.clickhouse.tech`
- 19.14 LTS: `play-api-v19-14.clickhouse.tech`

!!! note "備考"
これらのエンドポイントはすべて、安全なTLS接続が必要です。


## 制限事項 {#limitations}

クエリは読み取り専用のユーザとして実行されます。これにはいくつかの制限があります。

- DDL クエリは許可されていません。
- INSERT クエリは許可されていません。

また、以下の設定がなされています。

- [max_result_bytes=10485760](../operations/settings/query_complexity/#max-result-bytes)
- [max_result_rows=2000](../operations/settings/query_complexity/#setting-max_result_rows)
- [result_overflow_mode=break](../operations/settings/query_complexity/#result-overflow-mode)
- [max_execution_time=60000](../operations/settings/query_complexity/#max-execution-time)

## 例 {#examples}

`curl` を用いて HTTPSエンドポイントへ接続する例:

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse\!';&user=playground&password=clickhouse&database=datasets"
```

[CLI](../interfaces/cli.md) で TCP エンドポイントへ接続する例:

``` bash
clickhouse client --secure -h play-api.clickhouse.tech --port 9440 -u playground --password clickhouse -q "SELECT 'Play ClickHouse\!'"
```

## 実装の詳細 {#implementation-details}

ClickHouse PlaygroundのWebインタフェースは、ClickHouse [HTTP API](../interfaces/http.md)を介してリクエストを行います。
Playgroundのバックエンドは、追加のサーバーサイドのアプリケーションを伴わない、ただのClickHouseクラスタです。
上記のように, ClickHouse HTTPSとTCP/TLSのエンドポイントは Playground の一部としても公開されており、
いずれも、上記の保護とよりよいグローバルな接続のためのレイヤを追加するために、[Cloudflare Spectrum](https://www.cloudflare.com/products/cloudflare-spectrum/) を介してプロキシされています。

!!! warning "注意"
    いかなる場合においても、インターネットにClickHouseサーバを公開することは **非推奨です**。
    プライベートネットワーク上でのみ接続を待機し、適切に設定されたファイアウォールによって保護されていることを確認してください。
