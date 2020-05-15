---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 54
toc_title: "Query\u30D7\u30ED\u30D5\u30A1\u30A4\u30EA\u30F3\u30B0"
---

# クエリプ {#sampling-query-profiler}

ClickHouse運転サンプリングプロファイラでの分析クエリを実行します。 使用プロファイラでソースコードのルーチンを用いた最中に頻繁にクエリを実行します。 CPU時間とアイドル時間を含む壁時計の時間をトレースできます。

プロファイラを使用する:

-   この [trace\_log](../server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) サーバー設定のセクション。

    このセクションでは、 [trace\_log](../../operations/system-tables.md#system_tables-trace_log) プロファイラーの機能の結果を含むシステムテーブル。 デフォルトで設定されています。 このデータをこのテーブルのみ有効なオペレーティングシステムサーバーです。 後、サーバを再起動ClickHouseないクリーンのテーブルに格納された仮想メモリアドレスが無効になります。

-   この [query\_profiler\_cpu\_time\_period\_ns](../settings/settings.md#query_profiler_cpu_time_period_ns) または [query\_profiler\_real\_time\_period\_ns](../settings/settings.md#query_profiler_real_time_period_ns) 設定。 両方の設定を同時に使用できます。

    これらの設定を許可する設定プロファイラータイマー. これらはセッション設定であるため、サーバー全体、個々のユーザーまたはユーザープロファイル、対話型セッション、および個々のクエリごとに異なるサンプリング

デフォルトのサンプリング周波数はサンプルや、cpu、リアルタイマーが有効になっています。 この周波数により収集に関する情報を十分にclickhouse。 同時に、この頻度で作業しても、プロファイラーはclickhouseサーバーのパフォーマンスに影響しません。 が必要な場合にプロファイル毎に個別のクエリを利用するようにして高サンプリング周波数です。

分析するため `trace_log` システム表:

-   インストール `clickhouse-common-static-dbg` パッケージ。 見る [DEBパッケージからのイ](../../getting-started/install.md#install-from-deb-packages).

-   によってイントロスペクション機能を許可する [allow\_introspection\_functions](../settings/settings.md#settings-allow_introspection_functions) 設定。

    セキュ

-   を使用 `addressToLine`, `addressToSymbol` と `demangle` [イントロスペクション関数](../../sql-reference/functions/introspection.md) ClickHouseコードで関数名とその位置を取得する。 いくつかのクエリのプロファイルを取得するには、 `trace_log` テーブル。 個々の関数またはスタックトレース全体でデータを集計できます。

あなたが視覚化する必要がある場合 `trace_log` 情報、試してみる [flamegraph](../../interfaces/third-party/gui/#clickhouse-flamegraph) と [speedscope](https://github.com/laplab/clickhouse-speedscope).

## 例えば {#example}

この例では、:

-   フィルタ `trace_log` クエリ識別子と現在の日付によるデータ。

-   スタックトレースによる集計。

-   イントロスペクション関数を使用して、我々のレポートを取得します:

    -   シンボルおよび対応するソースコード関数の名前。
    -   これらの関数のソースコードの場所。

<!-- -->

``` sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = 'ebca3574-ad0a-400a-9cbc-dca382f5998c') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```

``` text
{% include "examples/sampling_query_profiler_result.txt" %}
```
