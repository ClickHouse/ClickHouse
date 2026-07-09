---
description: 'ClickHouse のサンプリングクエリプロファイラに関するドキュメント'
sidebar_label: 'クエリプロファイリング'
sidebar_position: 54
slug: /operations/optimizing-performance/sampling-query-profiler
title: 'サンプリングクエリプロファイラ'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="sampling-query-profiler">
  # サンプリングクエリプロファイラ
</div>

ClickHouse には、クエリ実行を分析できるサンプリングプロファイラが備わっています。
このプロファイラを使用すると、クエリ実行中に最も頻繁に使用されるソースコードのルーチンを特定できます。
アイドル時間を含む CPU 時間とウォールクロック時間を追跡できます。

クエリプロファイラは ClickHouse Cloud で自動的に有効になります。
次のクエリ例では、関数名とソースコード上の位置を解決したうえで、プロファイル対象のクエリで最も頻出するスタックトレースを見つけます。

:::tip
`query_id` の値を、プロファイルしたいクエリの ID に置き換えてください。
:::

<Tabs groupId="deployment">
  <TabItem value="cloud" label="ClickHouse Cloud">
    ClickHouse Cloud では、クエリ結果テーブルの上にあるバーの右端 (テーブル/チャートのトグルの横) にある **&quot;...&quot;** をクリックすると、クエリ ID を取得できます。コンテキストメニューが開くので、**&quot;Copy query ID&quot;** をクリックしてください。

    クラスター内のすべてのノードから選択するには、`clusterAllReplicas(default, system.trace_log)` を使用します。

    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM clusterAllReplicas(default, system.trace_log)
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>

  <TabItem value="self-managed" label="セルフマネージド">
    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>
</Tabs>

<div id="self-managed-query-profiler">
  ## セルフマネージド環境でクエリプロファイラを使用する
</div>

セルフマネージド環境でクエリプロファイラを使用するには、以下の手順に従ってください。

<VerticalStepper headerLevel="h3">
  ### デバッグ情報付きの ClickHouse をインストールする

  `clickhouse-common-static-dbg` パッケージをインストールします。

  1. 手順 [「Debian リポジトリを設定する」](/ja/install/debian_ubuntu#setup-the-debian-repository) の説明に従います
  2. `sudo apt-get install clickhouse-server clickhouse-client clickhouse-common-static-dbg` を実行し、デバッグ情報付きでコンパイルされた ClickHouse のバイナリファイルをインストールします
  3. `sudo service clickhouse-server start` を実行してサーバーを起動します
  4. `clickhouse-client` を実行します。`clickhouse-common-static-dbg` のデバッグシンボルはサーバーによって自動的に読み込まれるため、有効にするための特別な操作は不要です

  ### サーバー設定を確認する

  [サーバー設定ファイル](/ja/operations/configuration-files)の [`trace_log`](../../operations/server-configuration-parameters/settings.md#trace_log) セクションが設定されていることを確認してください。これはデフォルトで有効になっています。

  ```xml
  <!-- トレースログ。クエリプロファイラによって収集されたスタックトレースを保存します。
       query_profiler_real_time_period_ns および query_profiler_cpu_time_period_ns 設定を参照してください。 -->
  <trace_log>
      <database>system</database>
      <table>trace_log</table>

      <partition_by>toYYYYMM(event_date)</partition_by>
      <flush_interval_milliseconds>7500</flush_interval_milliseconds>
      <max_size_rows>1048576</max_size_rows>
      <reserved_size_rows>8192</reserved_size_rows>
      <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
      <!-- クラッシュ時にログをディスクへダンプするかどうかを示します -->
      <flush_on_crash>false</flush_on_crash>
      <symbolize>true</symbolize>
  </trace_log>
  ```

  このセクションでは、プロファイラの実行結果を含む [trace&#95;log](/ja/operations/system-tables/trace_log) システムテーブル を設定します。
  このテーブル内のデータは、サーバーの稼働中に限って有効であることに注意してください。
  サーバーの再起動後も ClickHouse はこのテーブルをクリーンアップしないため、保存されている仮想メモリアドレスはすべて無効になる可能性があります。

  ### プロファイラのタイマーを設定する

  [`query_profiler_cpu_time_period_ns`](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) または [`query_profiler_real_time_period_ns`](../../operations/settings/settings.md#query_profiler_real_time_period_ns) 設定を構成します。
  これら 2 つの設定は同時に使用できます。

  これらの設定を使うと、プロファイラのタイマーを設定できます。
  これらはセッション設定であるため、サーバー全体、個々のユーザーまたはユーザープロファイル、対話セッション、および個々のクエリごとに異なるサンプリング頻度を設定できます。

  デフォルトのサンプリング頻度は 1 秒あたり 1 サンプルで、CPU タイマーと実時間タイマーの両方が有効になっています。
  この頻度であれば、サーバーのパフォーマンスに影響を与えることなく、ClickHouse クラスターに関する十分な情報を収集できます。
  個々のクエリごとにプロファイルする必要がある場合は、より高いサンプリング頻度を使用してください。

  ### `trace_log` システムテーブルを分析する

  `trace_log` システムテーブル を分析するには、[`allow_introspection_functions`](../../operations/settings/settings.md#allow_introspection_functions) 設定でイントロスペクション関数を有効にします。

  ```sql
  SET allow_introspection_functions=1
  ```

  :::note
  セキュリティ上の理由から、イントロスペクション関数はデフォルトで無効になっています
  :::

  `addressToLine`、`addressToLineWithInlines`、`addressToSymbol`、`demangle` の[イントロスペクション関数](../../sql-reference/functions/introspection.md)を使用すると、関数名と ClickHouse コード内での位置を取得できます。
  特定のクエリのプロファイルを取得するには、`trace_log` テーブルのデータを集約する必要があります。
  データは個々の関数単位でも、スタックトレース全体単位でも集約できます。

  :::tip
  `trace_log` の情報を可視化する必要がある場合は、[flamegraph](/ja/interfaces/third-party/gui#clickhouse-flamegraph) と [speedscope](https://www.speedscope.app) を試してください。
  :::
</VerticalStepper>

<div id="flamegraph">
  ## `flameGraph` 関数を使用したフレームグラフの作成
</div>

ClickHouse には、`trace_log` に保存されたスタックトレースから直接フレームグラフを生成する集約関数 [`flameGraph`](/ja/sql-reference/aggregate-functions/reference/flame_graph) があります。
出力は、[flamegraph.pl](https://github.com/brendangregg/FlameGraph) と互換性のあるフォーマットの文字列の配列です。

**構文:**

```sql
flameGraph(traces, [size = 1], [ptr = 0])
```

**引数:**

* `traces` — スタックトレース。[`Array(UInt64)`](/ja/sql-reference/data-types/array)。
* `size` — メモリプロファイリング用の割り当てサイズ。[`Int64`](/ja/sql-reference/data-types/int-uint)。
* `ptr` — 割り当てアドレス。[`UInt64`](/ja/sql-reference/data-types/int-uint)。

`ptr` が 0 以外の場合、`flameGraph` は同じサイズとポインタを持つ割り当て (`size > 0`) と解放 (`size < 0`) を対応付けます。
表示されるのは、解放されなかった割り当てのみです。
対応する割り当てがない解放は無視されます。

<div id="cpu-flame-graph">
  ### CPU フレームグラフ
</div>

:::note
以下のクエリを実行するには、[flamegraph.pl](https://github.com/brendangregg/FlameGraph) がインストールされている必要があります。

次のコマンドでインストールできます。

```bash
git clone https://github.com/brendangregg/FlameGraph
# Then use it as:
# ~/FlameGraph/flamegraph.pl
```

次のクエリ内の `flamegraph.pl` を、お使いのマシンで `flamegraph.pl` があるパスに置き換えてください
:::

```sql
SET query_profiler_cpu_time_period_ns = 10000000;
```

クエリを実行し、フレームグラフを作成します：

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(arrayReverse(trace)))
        FROM system.trace_log
        WHERE trace_type = 'CPU' AND query_id = '<query_id>'" \
    | flamegraph.pl > flame_cpu.svg
```

<div id="memory-flame-graph-all">
  ### メモリフレームグラフ — すべての割り当て
</div>

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

クエリを実行してから、フレームグラフを作成します：

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem.svg
```

<div id="memory-flame-graph-unfreed">
  ### Memory flame graph — 解放されていない割り当て
</div>

このバリアントでは、ポインタごとに割り当てと解放を対応付け、クエリ実行中に解放されなかったメモリだけを表示します。

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1,
    use_uncompressed_cache = 1,
    merge_tree_max_rows_to_use_cache = 100000000000,
    merge_tree_max_bytes_to_use_cache = 1000000000000;
```

フレームグラフを作成するには、次のクエリを実行します。

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_unfreed.svg
```

<div id="memory-flame-graph-time-point">
  ### メモリフレームグラフ — ある時点でのアクティブな割り当て
</div>

この方法では、メモリ使用量のピークを特定し、その時点で何が割り当てられていたかを可視化できます。

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

<div id="find-memory-usage-over-time">
  #### メモリ使用量の推移を確認する
</div>

```sql
SELECT
    event_time,
    formatReadableSize(max(s)) AS m
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
)
GROUP BY event_time
ORDER BY event_time;
```

<div id="find-time-point-maximum-memory-usage">
  #### メモリ使用量が最大となる時点を見つける
</div>

```sql
SELECT
    argMax(event_time, s),
    max(s)
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
);
```

<div id="build-flame-graph">
  #### その時点でアクティブな割り当てのフレームグラフを作成する
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time <= '<time_point>'
            ORDER BY event_time
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_pos.svg
```

<div id="build-flame-graph-deallocations">
  #### その時点以降のメモリ解放のフレームグラフを作成する (その後に何が解放されたかを把握するため)
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, -size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time > '<time_point>'
            ORDER BY event_time DESC
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_neg.svg
```

<div id="example">
  ## 例
</div>

以下のコードスニペットでは、次のことを行います。

* クエリ識別子と当日の日付で `trace_log` データを絞り込みます。
* スタックトレースごとに集計します。
* イントロスペクション関数を使用して、次の内容を含むレポートを取得します。
  * シンボル名と、それに対応するソースコード上の関数名。
  * それらの関数のソースコード上の位置。

```sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = '<query_id>') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```