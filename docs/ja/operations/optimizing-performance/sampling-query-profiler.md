---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "Query\u30D7\u30ED\u30D5\u30A1\u30A4\u30EA\u30F3\u30B0"
---

# サンプリングクロファイラ {#sampling-query-profiler}

ClickHouse運転サンプリングプロファイラでの分析クエリを実行します。 Profilerを使用すると、クエリの実行中に最も頻繁に使用されるソースコードルーチンを検索できます。 CPU時間とアイドル時間を含む壁時計時間をトレースできます。

Profilerを使用するには:

-   セットアップ [trace\_log](../server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) サーバー構成のセクション。

    このセクションでは、 [trace\_log](../../operations/system-tables.md#system_tables-trace_log) プロファイラ機能の結果を含むシステムテーブル。 これは既定で構成されています。 この表のデータは、実行中のサーバーに対してのみ有効です。 後、サーバを再起動ClickHouseないクリーンのテーブルに格納された仮想メモリアドレスが無効になります。

-   セットアップ [query\_profiler\_cpu\_time\_period\_ns](../settings/settings.md#query_profiler_cpu_time_period_ns) または [query\_profiler\_real\_time\_period\_ns](../settings/settings.md#query_profiler_real_time_period_ns) 設定。 両方の設定を同時に使用できます。

    これらの設定を許可する設定プロファイラータイマー. これらはセッション設定であるため、サーバー全体、個々のユーザーまたはユーザープロファイル、対話式セッション、および個々のクエリごとに異なるサンプリング周波数

デフォルトのサンプリング周波数はサンプルや、CPU、リアルタイマーが有効になっています。 この頻度により、ClickHouse clusterに関する十分な情報を収集できます。 同時に、この頻度で作業すると、profilerはClickHouse serverのパフォーマンスには影響しません。 が必要な場合にプロファイル毎に個別のクエリを利用するようにして高サンプリング周波数です。

分析するには `trace_log` システム表:

-   インストール `clickhouse-common-static-dbg` パッケージ。 見る [DEBパッケージから](../../getting-started/install.md#install-from-deb-packages).

-   によるイントロスペクション関数を許可する。 [allow\_introspection\_functions](../settings/settings.md#settings-allow_introspection_functions) 設定。

    セキュリティ上の理由から、introspection関数は既定で無効になっています。

-   使用する `addressToLine`, `addressToSymbol` と `demangle` [内観関数](../../sql-reference/functions/introspection.md) ClickHouseコードで関数名とその位置を取得するには。 いくつかのクエリのプロファイルを取得するには、 `trace_log` テーブル。 個々の関数またはスタックトレース全体でデータを集計できます。

視覚化する必要がある場合 `trace_log` 情報、試して [フラメグラフ](../../interfaces/third-party/gui/#clickhouse-flamegraph) と [スピードスコープ](https://github.com/laplab/clickhouse-speedscope).

## 例 {#example}

この例では、:

-   フィルタ処理 `trace_log` クエリ識別子と現在の日付によるデータ。

-   スタックトレースによる集計。

-   イントロスペクション関数を使用して、我々はのレポートを取得します:

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
