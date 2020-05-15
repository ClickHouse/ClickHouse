---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 14
toc_title: "\u904A\u3073\u5834"
---

# ClickHouseの運動場 {#clickhouse-playground}

[ClickHouseの運動場](https://play.clickhouse.tech?file=welcome) "いちばん、人を考える実験ClickHouseによる走行クエリを瞬時にな設定をサーバまたはクラスター
複数の例ではデータセットの遊び場などのサンプルのクエリを表すClickHouse特徴です。

クエリは読み取り専用ユーザーとして実行されます。 いくつかの制限を意味します:

-   DDLクエリは許可されません
-   挿入クエリは許可されません

次の設定も適用されます:
- [`max_result_bytes=10485760`](../operations/settings/query_complexity/#max-result-bytes)
- [`max_result_rows=2000`](../operations/settings/query_complexity/#setting-max_result_rows)
- [`result_overflow_mode=break`](../operations/settings/query_complexity/#result-overflow-mode)
- [`max_execution_time=60000`](../operations/settings/query_complexity/#max-execution-time)

ClickHouseの運動場はm2の経験を与える。小さい
[ClickHouseの管理サービス](https://cloud.yandex.com/services/managed-clickhouse)
インスタンス [Yandex.クラウド](https://cloud.yandex.com/).
詳細について [クラウドプロバイダー](../commercial/cloud.md).

ClickHouse遊びwebインタフェースによって、要求によClickHouse [HTTP API](../interfaces/http.md).
コミュニケーションが円滑にバックエンドがありClickHouseクラスターになサーバーサイド願います。
ClickHouse HTTPSエンドポイントは、遊び場の一部としても利用できます。

任意のHTTPクライアントを使用してplaygroundにクエリを実行できます。 [カール](https://curl.haxx.se) または [wget](https://www.gnu.org/software/wget/) または、次を使用して接続を設定します [JDBC](../interfaces/jdbc.md) または [ODBC](../interfaces/odbc.md) ドライバー
ClickHouseをサポートするソフトウェア製品の詳細については、 [ここに](../interfaces/index.md).

| パラメータ | 値                                       |
|:-----------|:-----------------------------------------|
| 端点       | https://play-api.クリックハウス技術:8443 |
| ユーザ     | `playground`                             |
| パスワード | `clickhouse`                             |

このエンドポイントには安全な接続が必要です。

例:

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
