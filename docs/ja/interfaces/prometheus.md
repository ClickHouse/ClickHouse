---
slug: /ja/interfaces/prometheus
sidebar_position: 19
sidebar_label: Prometheusプロトコル
---

# Prometheusプロトコル

## メトリクスの公開 {#expose}

:::note
ClickHouse Cloudを使用している場合、[Prometheus Integration](/ja/integrations/prometheus)を使用してPrometheusにメトリクスを公開できます。
:::

ClickHouseはその自身のメトリクスをPrometheusからのスクレイピングのために公開できます：

```xml
<prometheus>
    <port>9363</port>
    <endpoint>/metrics</endpoint>
    <metrics>true</metrics>
    <asynchronous_metrics>true</asynchronous_metrics>
    <events>true</events>
    <errors>true</errors>
</prometheus>
```

`<prometheus.handlers>`セクションを使用して、より詳細なハンドラーを作成できます。
このセクションは[<http_handlers>](/ja/interfaces/http)に似ていますが、Prometheusプロトコル用に機能します：

```xml
<prometheus>
    <port>9363</port>
    <handlers>
        <my_rule_1>
            <url>/metrics</url>
            <handler>
                <type>expose_metrics</type>
                <metrics>true</metrics>
                <asynchronous_metrics>true</asynchronous_metrics>
                <events>true</events>
                <errors>true</errors>
            </handler>
        </my_rule_1>
    </handlers>
</prometheus>
```

設定:

| 名前 | デフォルト | 説明 |
|---|---|---|
| `port` | なし | メトリクス公開プロトコルを提供するためのポート。 |
| `endpoint` | `/metrics` | Prometheusサーバーによるメトリクスのスクレイピング用HTTPエンドポイント。`/`で始まります。`<handlers>`セクションと併用してはいけません。 |
| `url` / `headers` / `method` | なし | リクエストに対する適合ハンドラーを見つけるために使用されるフィルター。[<http_handlers>](/ja/interfaces/http)セクションの同じ名前のフィールドに似ています。 |
| `metrics` | true | [system.metrics](/ja/operations/system-tables/metrics)テーブルからのメトリクスを公開します。 |
| `asynchronous_metrics` | true | [system.asynchronous_metrics](/ja/operations/system-tables/asynchronous_metrics)テーブルから現在のメトリクス値を公開します。 |
| `events` | true | [system.events](/ja/operations/system-tables/events)テーブルからのメトリクスを公開します。 |
| `errors` | true | 最後のサーバー再起動以降に発生したエラーコードごとのエラー数を公開します。この情報は[system.errors](/ja/operations/system-tables/errors)からも取得できます。 |

チェック（`127.0.0.1`をClickHouseサーバーのIPアドレスまたはホスト名に置き換えてください）：
```bash
curl 127.0.0.1:9363/metrics
```

## リモート書き込みプロトコル {#remote-write}

ClickHouseは[remote-write](https://prometheus.io/docs/specs/remote_write_spec/)プロトコルをサポートしています。
このプロトコルによって受信したデータは、（あらかじめ作成しておく必要がある）[TimeSeries](/ja/engines/table-engines/special/time_series)テーブルに書き込まれます。

```xml
<prometheus>
    <port>9363</port>
    <handlers>
        <my_rule_1>
            <url>/write</url>
            <handler>
                <type>remote_write</type>
                <database>db_name</database>
                <table>time_series_table</table>
            </handler>
        </my_rule_1>
    </handlers>
</prometheus>
```

設定:

| 名前 | デフォルト | 説明 |
|---|---|---|
| `port` | なし | `remote-write`プロトコルを提供するためのポート。 |
| `url` / `headers` / `method` | なし | リクエストに対する適合ハンドラーを見つけるために使用されるフィルター。[<http_handlers>](/ja/interfaces/http)セクションの同じ名前のフィールドに似ています。 |
| `table` | なし | `remote-write`プロトコルで受信したデータを書き込む[TimeSeries](/ja/engines/table-engines/special/time_series)テーブルの名前。この名前にはオプションでデータベース名を含めることができます。 |
| `database` | なし | `table`設定で指定されていない場合、`table`設定で指定されたテーブルがあるデータベースの名前。 |

## リモート読み取りプロトコル {#remote-read}

ClickHouseは[remote-read](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/)プロトコルをサポートしています。
このプロトコルを通じて、[TimeSeries](/ja/engines/table-engines/special/time_series)テーブルからデータを読み取り、送信します。

```xml
<prometheus>
    <port>9363</port>
    <handlers>
        <my_rule_1>
            <url>/read</url>
            <handler>
                <type>remote_read</type>
                <database>db_name</database>
                <table>time_series_table</table>
            </handler>
        </my_rule_1>
    </handlers>
</prometheus>
```

設定:

| 名前 | デフォルト | 説明 |
|---|---|---|
| `port` | なし | `remote-read`プロトコルを提供するためのポート。 |
| `url` / `headers` / `method` | なし | リクエストに対する適合ハンドラーを見つけるために使用されるフィルター。[<http_handlers>](/ja/interfaces/http)セクションの同じ名前のフィールドに似ています。 |
| `table` | なし | `remote-read`プロトコルで送信するために読み取る[TimeSeries](/ja/engines/table-engines/special/time_series)テーブルの名前。この名前にはオプションでデータベース名を含めることができます。 |
| `database` | なし | `table`設定で指定されていない場合、`table`設定で指定されたテーブルがあるデータベースの名前。 |

## 複数プロトコル用の設定 {#multiple-protocols}

複数のプロトコルを一つの場所に一緒に指定することができます：

```xml
<prometheus>
    <port>9363</port>
    <handlers>
        <my_rule_1>
            <url>/metrics</url>
            <handler>
                <type>expose_metrics</type>
                <metrics>true</metrics>
                <asynchronous_metrics>true</asynchronous_metrics>
                <events>true</events>
                <errors>true</errors>
            </handler>
        </my_rule_1>
        <my_rule_2>
            <url>/write</url>
            <handler>
                <type>remote_write</type>
                <table>db_name.time_series_table</table>
            </handler>
        </my_rule_2>
        <my_rule_3>
            <url>/read</url>
            <handler>
                <type>remote_read</type>
                <table>db_name.time_series_table</table>
            </handler>
        </my_rule_3>
    </handlers>
</prometheus>
```
