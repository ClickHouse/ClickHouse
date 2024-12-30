---
slug: /ja/sql-reference/aggregate-functions/reference/flame_graph
sidebar_position: 138
---

# flameGraph

スタックトレースのリストを使用して[フレームグラフ](https://www.brendangregg.com/flamegraphs.html)を構築する集約関数です。フレームグラフのSVGを生成するために[flamegraph.pl ユーティリティ](https://github.com/brendangregg/FlameGraph)で使用できる文字列の配列を出力します。

## 構文

```sql
flameGraph(traces, [size], [ptr])
```

## パラメータ

- `traces` — スタックトレース。[Array](../../data-types/array.md)([UInt64](../../data-types/int-uint.md))。
- `size` — メモリプロファイリングのための割り当てサイズ。（オプション - デフォルトは`1`）[UInt64](../../data-types/int-uint.md)。
- `ptr` — 割り当てアドレス。（オプション - デフォルトは`0`）[UInt64](../../data-types/int-uint.md)。

:::note
`ptr != 0`の場合、flameGraphは同じサイズとptrでの割り当て（size > 0）および解放（size < 0）をマッピングします。解放されていない割り当てのみが表示されます。マッピングされていない解放は無視されます。
:::

## 戻り値

- [flamegraph.pl ユーティリティ](https://github.com/brendangregg/FlameGraph)で使用するための文字列の配列。[Array](../../data-types/array.md)([String](../../data-types/string.md))。

## 使用例

### CPUクエリプロファイラに基づいたフレームグラフの構築

```sql
SET query_profiler_cpu_time_period_ns=10000000;
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
```

```text
clickhouse client --allow_introspection_functions=1 -q "select arrayJoin(flameGraph(arrayReverse(trace))) from system.trace_log where trace_type = 'CPU' and query_id = 'xxx'" | ~/dev/FlameGraph/flamegraph.pl  > flame_cpu.svg
```

### メモリクエリプロファイラに基づき、すべての割り当てを表示するフレームグラフの構築

```sql
SET memory_profiler_sample_probability=1, max_untracked_memory=1;
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
```

```text
clickhouse client --allow_introspection_functions=1 -q "select arrayJoin(flameGraph(trace, size)) from system.trace_log where trace_type = 'MemorySample' and query_id = 'xxx'" | ~/dev/FlameGraph/flamegraph.pl --countname=bytes --color=mem > flame_mem.svg
```

### クエリコンテキストで解放されていない割り当てを示すメモリクエリプロファイラに基づいたフレームグラフの構築

```sql
SET memory_profiler_sample_probability=1, max_untracked_memory=1, use_uncompressed_cache=1, merge_tree_max_rows_to_use_cache=100000000000, merge_tree_max_bytes_to_use_cache=1000000000000;
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
```

```text
clickhouse client --allow_introspection_functions=1 -q "SELECT arrayJoin(flameGraph(trace, size, ptr)) FROM system.trace_log WHERE trace_type = 'MemorySample' AND query_id = 'xxx'" | ~/dev/FlameGraph/flamegraph.pl --countname=bytes --color=mem > flame_mem_untracked.svg
```

### 一定時点でのアクティブ割り当てを表示するメモリクエリプロファイラに基づいたフレームグラフの構築

```sql
SET memory_profiler_sample_probability=1, max_untracked_memory=1;
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
```

- 1 - 秒単位のメモリ使用量

```sql
SELECT event_time, m, formatReadableSize(max(s) as m) FROM (SELECT event_time, sum(size) OVER (ORDER BY event_time) AS s FROM system.trace_log WHERE query_id = 'xxx' AND trace_type = 'MemorySample') GROUP BY event_time ORDER BY event_time;
```

- 2 - 最大メモリ使用量の時点を見つける

```sql
SELECT argMax(event_time, s), max(s) FROM (SELECT event_time, sum(size) OVER (ORDER BY event_time) AS s FROM system.trace_log WHERE query_id = 'xxx' AND trace_type = 'MemorySample');
```

-  3 - 一定時点でのアクティブな割り当てを固定する

```text
clickhouse client --allow_introspection_functions=1 -q "SELECT arrayJoin(flameGraph(trace, size, ptr)) FROM (SELECT * FROM system.trace_log WHERE trace_type = 'MemorySample' AND query_id = 'xxx' AND event_time <= 'yyy' ORDER BY event_time)" | ~/dev/FlameGraph/flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_pos.svg
```

- 4 - 一定時点での解放を見つける

```text
clickhouse client --allow_introspection_functions=1 -q "SELECT arrayJoin(flameGraph(trace, -size, ptr)) FROM (SELECT * FROM system.trace_log WHERE trace_type = 'MemorySample' AND query_id = 'xxx' AND event_time > 'yyy' ORDER BY event_time desc)" | ~/dev/FlameGraph/flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_neg.svg
```
