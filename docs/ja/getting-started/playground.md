---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 14
toc_title: "\u904A\u3073\u5834"
---

# ClickHouseの遊び場 {#clickhouse-playground}

[ClickHouseの遊び場](https://play.clickhouse.tech?file=welcome) サーバーやクラスターを設定せずに、クエリを即座に実行することで、ClickHouseを試すことができます。
複数の例ではデータセットの遊び場などのサンプルのクエリを表すclickhouse特徴です。

クエリは読み取り専用ユーザーとして実行されます。 では一部制限:

-   DDLクエリは許可されません
-   挿入クエリは許可されません

次の設定も適用されます:
- [`max_result_bytes=10485760`](../operations/settings/query_complexity/#max-result-bytes)
- [`max_result_rows=2000`](../operations/settings/query_complexity/#setting-max_result_rows)
- [`result_overflow_mode=break`](../operations/settings/query_complexity/#result-overflow-mode)
- [`max_execution_time=60000`](../operations/settings/query_complexity/#max-execution-time)

ClickHouseの運動場はm2の経験を与える。小さい
[管理サービスclickhouse](https://cloud.yandex.com/services/managed-clickhouse)
インスタンス [Yandexの。クラウド](https://cloud.yandex.com/).
詳細については、 [クラウドプロバイダー](../commercial/cloud.md).

ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂつｹ [HTTP API](../interfaces/http.md).
コミュニケーションが円滑にバックエンドがありclickhouseクラスターになサーバーサイド願います。
ClickHouse HTTPS評価項目としても利用可能ですの一部が遊べない日々が続いていました。

できるクエリーの遊び場をhttpお客様は、例えば [カール](https://curl.haxx.se) または [wget](https://www.gnu.org/software/wget/)、または以下を使用して接続を設定する [JDBC](../interfaces/jdbc.md) または [ODBC](../interfaces/odbc.md) ドライバー
情報ソフトウェア製品を支えるclickhouse可能 [ここに](../interfaces/index.md).

| パラメータ | 値                                            |
|:-----------|:----------------------------------------------|
| エンドポイ | https://play-api。クリックハウス。テック:8443 |
| ユーザ     | `playground`                                  |
| パスワード | `clickhouse`                                  |

このエンドポイントには安全な接続が必要です。

例えば:

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
