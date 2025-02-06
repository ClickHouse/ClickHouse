---
slug: /ja/operations/optimizing-performance/sampling-query-profiler
sidebar_position: 54
sidebar_label: クエリプロファイリング
---
import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';

# サンプリングクエリプロファイラー

ClickHouseは、クエリ実行を分析するためのサンプリングプロファイラーを実行します。プロファイラーを使用すると、クエリ実行中に最も頻繁に使用されるソースコードルーチンを特定できます。CPU時間およびアイドル時間を含む壁時計時間をトレースできます。

クエリプロファイラーはClickHouse Cloudで自動的に有効化されており、以下のようにサンプルクエリを実行することができます。

:::note 以下のクエリをClickHouse Cloudで実行する場合は、クラスタの全ノードから選択するために`FROM system.trace_log`を`FROM clusterAllReplicas(default, system.trace_log)`に変更してください。
:::

``` sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = 'ebca3574-ad0a-400a-9cbc-dca382f5998c') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
SETTINGS allow_introspection_functions = 1
```

セルフマネージドのデプロイメント環境でクエリプロファイラーを使用するには：

- サーバー設定の[trace_log](../../operations/server-configuration-parameters/settings.md#trace_log)セクションをセットアップします。

  このセクションは、プロファイラーの動作結果を含む[trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log)システムテーブルを設定します。これはデフォルトで設定されています。このテーブルのデータは実行中のサーバーに対してのみ有効であり、サーバーが再起動すると、ClickHouseはテーブルをクリアせず、すべての保存された仮想メモリアドレスが無効になる可能性があります。

- [query_profiler_cpu_time_period_ns](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) または [query_profiler_real_time_period_ns](../../operations/settings/settings.md#query_profiler_real_time_period_ns)設定をセットアップします。これらの設定は同時に使用可能です。

  これらの設定により、プロファイラーのタイマーを構成できます。セッション設定なので、サーバー全体、個々のユーザーまたはユーザープロファイル、インタラクティブなセッション、および各個別のクエリに対して異なるサンプリング頻度を得ることができます。

デフォルトのサンプリング頻度は1秒ごとのサンプルで、CPUタイマーと実時間タイマーの両方が有効です。この頻度により、ClickHouseクラスタに関する十分な情報を収集できます。同時に、この頻度で作業する場合、プロファイラーはClickHouseサーバーのパフォーマンスに影響を与えません。各個別のクエリをプロファイルする必要がある場合は、より高いサンプリング頻度を試してください。

`trace_log`システムテーブルを分析するには：

- `clickhouse-common-static-dbg`パッケージをインストールします。[DEBパッケージからのインストール](../../getting-started/install.md#install-from-deb-packages)を参照してください。

- [allow_introspection_functions](../../operations/settings/settings.md#allow_introspection_functions)設定により内省関数を許可します。

  セキュリティ上の理由から、内省関数はデフォルトで無効になっています。

- `addressToLine`、`addressToLineWithInlines`、`addressToSymbol`、および`demangle` [内省関数](../../sql-reference/functions/introspection.md)を使用して、ClickHouseコード内の関数名とその位置を取得します。特定のクエリのプロファイルを取得するには、`trace_log`テーブルからデータを集約する必要があります。個別の関数やスタックトレース全体でデータを集約することができます。

`trace_log`情報を視覚化する必要がある場合は、[flamegraph](../../interfaces/third-party/gui.md#clickhouse-flamegraph-clickhouse-flamegraph)や[speedscope](https://github.com/laplab/clickhouse-speedscope)を試してください。

## 例 {#example}

この例では、以下を行います：

- クエリアイデンティファイアと現在の日付で`trace_log`データをフィルタリングします。

- スタックトレースで集約します。

- 内省関数を使用して、以下のレポートを取得します：

  - シンボルの名前と対応するソースコード関数。
  - これらの関数のソースコードの位置。

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
