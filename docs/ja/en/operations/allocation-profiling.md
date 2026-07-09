---
description: 'ClickHouse のアロケーションプロファイリングの詳細ページ'
sidebar_label: 'アロケーションプロファイリング'
slug: /operations/allocation-profiling
title: 'アロケーションプロファイリング'
doc_type: 'guide'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling">
  # アロケーションプロファイリング
</div>

ClickHouse はグローバル アロケータとして [jemalloc](https://github.com/jemalloc/jemalloc) を使用しています。jemalloc には、アロケーションのサンプリングとプロファイリングのためのツールが備わっています。

ClickHouse と Keeper では、config、クエリ設定、`SYSTEM` コマンド、さらに Keeper の four letter word (4LW) コマンドを使用してサンプリングを制御できます。結果を確認する方法はいくつかあります。

* クエリごとの分析のために、`JemallocSample` タイプとして `system.trace_log` にサンプルを収集します。
* 組み込みの [jemalloc web UI](#jemalloc-web-ui) で、ライブ メモリ統計を表示し、ヒーププロファイル を取得します (26.2+) 。
* [`system.jemalloc_profile_text`](#fetching-heap-profiles-from-sql) を使用して、現在の ヒーププロファイル を SQL から直接取得します (26.2+) 。
* ヒーププロファイル をディスクにフラッシュし、[`jeprof`](#analyzing-heap-profile-files-with-jeprof) で分析します。

:::note

このガイドはバージョン 25.9+ に適用されます。
それ以前のバージョンについては、[25.9 より前のバージョン向けのアロケーションプロファイリング](/ja/operations/allocation-profiling-old.md) を参照してください。

:::

<div id="sampling-allocations">
  ## 割り当てのサンプリング
</div>

割り当てのサンプリングとプロファイリングを行うには、`jemalloc_enable_global_profiler` 設定を有効にして ClickHouse/Keeper を起動します:

```xml
<clickhouse>
    <jemalloc_enable_global_profiler>1</jemalloc_enable_global_profiler>
</clickhouse>
```

`jemalloc` はメモリ割り当てをサンプリングし、その情報を内部に保存します。

`jemalloc_enable_profiler` 設定を使用すると、クエリごとにサンプリングを有効にすることもできます。

:::warning 警告
ClickHouse はメモリ割り当てが非常に多いアプリケーションであるため、jemalloc のサンプリングによりパフォーマンスのオーバーヘッドが発生する可能性があります。
:::

<div id="storing-jemalloc-samples-in-system-trace-log">
  ## `system.trace_log` に jemalloc のサンプルを保存する
</div>

jemalloc のサンプルは、`JemallocSample` タイプとして `system.trace_log` に保存できます。
これをグローバルに有効化するには、`jemalloc_collect_global_profile_samples_in_trace_log` 設定を使用します。

```xml
<clickhouse>
    <jemalloc_collect_global_profile_samples_in_trace_log>1</jemalloc_collect_global_profile_samples_in_trace_log>
</clickhouse>
```

:::warning 警告
ClickHouse はメモリ割り当てが非常に多いアプリケーションであるため、system.trace&#95;log にすべてのサンプルを収集すると、高負荷になる可能性があります。
:::

また、`jemalloc_collect_profile_samples_in_trace_log` 設定を使って、クエリ単位で有効にすることもできます。

<div id="example-analyzing-memory-usage-trace-log">
  ### 例: クエリのメモリ使用量を分析する
</div>

まず、jemallocプロファイラを有効にしてクエリを実行し、サンプルを `system.trace_log` に収集します:

```sql
SELECT *
FROM numbers(1000000)
ORDER BY number DESC
SETTINGS max_bytes_ratio_before_external_sort = 0
FORMAT `Null`
SETTINGS jemalloc_enable_profiler = 1, jemalloc_collect_profile_samples_in_trace_log = 1

Query id: 8678d8fe-62c5-48b8-b0cd-26851c62dd75

Ok.

0 rows in set. Elapsed: 0.009 sec. Processed 1.00 million rows, 8.00 MB (108.58 million rows/s., 868.61 MB/s.)
Peak memory usage: 12.65 MiB.
```

:::note
ClickHouse を `jemalloc_enable_global_profiler` を指定して起動している場合、`jemalloc_enable_profiler` を有効にする必要はありません。
`jemalloc_collect_global_profile_samples_in_trace_log` と `jemalloc_collect_profile_samples_in_trace_log` についても同様です。
:::

`system.trace_log` を flush します:

```sql
SYSTEM FLUSH LOGS trace_log
```

次に、これに対してクエリを実行し、時間の経過に伴う累積メモリ使用量を取得します。

```sql
WITH per_bucket AS
(
    SELECT
        event_time_microseconds AS bucket_time,
        sum(size) AS bucket_sum
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
    GROUP BY bucket_time
)
SELECT
    bucket_time,
    sum(bucket_sum) OVER (
        ORDER BY bucket_time ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_size,
    formatReadableSize(cumulative_size) AS cumulative_size_readable
FROM per_bucket
ORDER BY bucket_time
```

メモリ使用量が最も高かった時刻を特定します:

```sql
SELECT
    argMax(bucket_time, cumulative_size),
    max(cumulative_size)
FROM
(
    WITH per_bucket AS
    (
        SELECT
            event_time_microseconds AS bucket_time,
            sum(size) AS bucket_sum
        FROM system.trace_log
        WHERE trace_type = 'JemallocSample'
          AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
        GROUP BY bucket_time
    )
    SELECT
        bucket_time,
        sum(bucket_sum) OVER (
            ORDER BY bucket_time ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_size,
        formatReadableSize(cumulative_size) AS cumulative_size_readable
    FROM per_bucket
    ORDER BY bucket_time
)
```

その結果を使って、ピーク時に最も多く見られた割り当てスタックを確認します。

```sql
SELECT
    concat(
        '\n',
        arrayStringConcat(
            arrayMap(
                (x, y) -> concat(x, ': ', y),
                arrayMap(x -> addressToLine(x), allocation_trace),
                arrayMap(x -> demangle(addressToSymbol(x)), allocation_trace)
            ),
            '\n'
        )
    ) AS symbolized_trace,
    sum(s) AS per_trace_sum
FROM
(
    SELECT
        ptr,
        sum(size) AS s,
        argMax(trace, event_time_microseconds) AS allocation_trace
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
      AND event_time_microseconds <= '2025-09-04 11:56:21.737139'
    GROUP BY ptr
    HAVING s > 0
)
GROUP BY ALL
ORDER BY per_trace_sum ASC
```

<div id="jemalloc-web-ui">
  ## Jemalloc web UI
</div>

:::note
このセクションはバージョン 26.2 以降で利用できます。
:::

ClickHouse には、`/jemalloc` HTTP エンドポイントで jemalloc のメモリ統計を表示する組み込みの web UI があります。
この UI では、allocated、active、resident、mapped memory に加え、アリーナ単位および bin 単位の統計を含むライブメモリメトリクスをグラフで表示します。
また、UI からグローバルおよびクエリごとのヒーププロファイルを直接取得することもできます。

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```text
    http://localhost:8123/jemalloc
    ```

    サーバー UI には、Summary、Allocations、Arenas、Operations、Global Profiler、Query Profiler、Raw Output のすべてのタブが含まれます。
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```text
    http://localhost:9182/jemalloc
    ```

    Keeper UI は HTTP control ポートで利用できます。このポートは**デフォルトで無効**になっているため、Keeper の設定で `keeper_server.http_control.port` を設定して明示的に有効化する必要があります。

    ```xml
    <clickhouse>
        <keeper_server>
            <http_control>
                <port>9182</port>
            </http_control>
        </keeper_server>
    </clickhouse>
    ```

    有効にすると、この UI ではサーバーと同じ可視化 (Summary、Allocations、Arenas、Operations、Global Profiler、Raw Output) が利用できます。ただし、Query Profiler タブは SQL と `system.trace_log` を必要とするため使用できません。

    :::warning セキュリティ
    Keeper の HTTP control ポートには、アプリケーションレベルの認証がありません。すべてのデータクエリが SQL HTTP ハンドラーを経由し、ユーザー名/パスワードの認証情報が必要な ClickHouse Server の jemalloc UI とは異なり、Keeper の REST API エンドポイントには認証がありません。これは、他の Keeper HTTP control エンドポイント (commands、storage、dashboard) と同様です。

    このポートへのアクセスは、ネットワークレベルの制御で制限してください。たとえば、Keeper を localhost にバインドする、ファイアウォールルールを使用する、または認証付きのリバースプロキシの背後に配置します。`listen_host` が設定されていない場合、Keeper はデフォルトで localhost のみを listen します。
    :::

    Keeper は、プログラムから利用するための REST API エンドポイントも公開しています。

    * `GET /jemalloc/stats` — 生の `malloc_stats_print` 出力
    * `GET /jemalloc/status` — JSON 形式の profiling 状態 (`prof_enabled`、`prof_active`、`thread_active_init`、`lg_sample`)
    * `GET /jemalloc/profile?format={collapsed|raw}` — ヒーププロファイルを flush し、server-side でシンボル化を行います。フレームグラフ の描画に適したコラプスされたスタック (デフォルト) または生の jemalloc ダンプを返します
  </TabItem>
</Tabs>

<div id="fetching-heap-profiles-from-sql">
  ## SQL からヒーププロファイルを取得する
</div>

:::note
このセクションはバージョン 26.2 以降で利用できます。
:::

`system.jemalloc_profile_text` システムテーブルを使うと、外部ツールを使ったり、先にディスクへフラッシュしたりしなくても、現在の jemalloc ヒーププロファイルを SQL から直接取得して確認できます。

このテーブルには 1 つのカラムがあります。

| カラム    | 型      | 説明                             |
| ------ | ------ | ------------------------------ |
| `line` | String | シンボル化された jemalloc ヒーププロファイルの行。 |

このテーブルは直接クエリできます。事前にヒーププロファイルをフラッシュする必要はありません。

```sql
SELECT * FROM system.jemalloc_profile_text
```

<div id="output-format">
  ### 出力フォーマット
</div>

出力フォーマットは、`jemalloc_profile_text_output_format` 設定で制御します。この設定では、次の 3 つの値を使用できます。

* `raw` — jemalloc によって生成される生のヒーププロファイル。
* `symbolized` — 関数シンボルが埋め込まれた、jeprof 互換のフォーマット。シンボルはすでに埋め込まれているため、`jeprof` は ClickHouse バイナリがなくても出力を解析できます。
* `collapsed` (デフォルト) — フレームグラフ互換のコラプスされたスタックです。各行に 1 つのスタックとバイト数が含まれます。

たとえば、生のプロファイルを取得するには次のようにします。

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'raw'
```

シンボル化された出力を取得するには:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'symbolized'
```

<div id="fetching-heap-profiles-settings">
  ### 追加設定
</div>

* `jemalloc_profile_text_symbolize_with_inline` (Bool, デフォルト: `true`) — シンボル化時にインラインフレームを含めるかどうか。これを無効にするとシンボル化は大幅に高速化されますが、インライン化された関数呼び出しがスタックに現れなくなるため、精度が低下します。影響するのは `symbolized` および `collapsed` フォーマットのみです。
* `jemalloc_profile_text_collapsed_use_count` (Bool, デフォルト: `false`) — `collapsed` フォーマットを使用する場合、バイト数ではなく割り当て回数で集計します。

<div id="example-flamegraph-from-sql">
  ### 例: SQLからフレームグラフを生成する
</div>

既定の出力フォーマットは `collapsed` のため、出力をそのまま FlameGraph にパイプできます。

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text" | flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

バイト数ではなく、割り当て回数に基づく フレームグラフ を生成するには:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text SETTINGS jemalloc_profile_text_collapsed_use_count = 1" | flamegraph.pl --color=mem --title="Allocation Count Flame Graph" --width 2400 > result.svg
```

<div id="flushing-heap-profiles">
  ## ヒーププロファイルをディスクにフラッシュする
</div>

`jeprof` を使用してオフラインで解析できるよう、ヒーププロファイルをファイルとして保存する必要がある場合は、ディスクにフラッシュできます。

デフォルトでは、ヒーププロファイルファイルは `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap` に生成されます。ここで、`_pid_` は ClickHouse の PID、`_seqnum_` は現在のヒーププロファイルのグローバルな連番です。
Keeper の場合、デフォルトのファイルは `/tmp/jemalloc_keeper._pid_._seqnum_.heap` で、同じ規則に従います。

現在のプロファイルをフラッシュするには、次を実行します。

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```

    フラッシュされたプロファイルの保存先が返されます。
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

別の保存先は、`MALLOC_CONF` 環境変数に `prof_prefix` オプションを追加して指定できます。
たとえば、ファイル名のプレフィックスを `my_current_profile` として `/data` フォルダーにプロファイルを生成したい場合は、次の環境変数を指定して ClickHouse/Keeper を実行できます。

```sh
MALLOC_CONF=prof_prefix:/data/my_current_profile
```

生成されるファイル名には、プレフィックス、PID、およびシーケンス番号が付加されます。

<div id="analyzing-heap-profile-files-with-jeprof">
  ## `jeprof` を使用したヒーププロファイルファイルの分析
</div>

ヒーププロファイルをディスクにフラッシュした後、[`jemalloc`](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in) のツールである [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in) を使用して分析できます。インストール方法はいくつかあります。

* システムのパッケージマネージャーを使用する
* [jemalloc リポジトリ](https://github.com/jemalloc/jemalloc) をクローンし、ルートフォルダーで `autogen.sh` を実行する。これにより、`bin` フォルダー内の `jeprof` スクリプトを利用できます

利用可能な出力フォーマットは多数あります。オプションの一覧をすべて表示するには、`jeprof --help` を実行してください。

<div id="symbolized-heap-profiles">
  ### シンボル化されたヒーププロファイル
</div>

バージョン 26.1+ 以降、ClickHouse では `SYSTEM JEMALLOC FLUSH PROFILE` でフラッシュすると、シンボル化されたヒーププロファイルが自動的に生成されます。
シンボル化されたプロファイル (`.symbolized` 拡張子) は関数シンボルが埋め込まれているため、ClickHouse バイナリがなくても `jeprof` で解析できます。

たとえば、次を実行すると:

```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

ClickHouse は、シンボル化されたプロファイルのパス (例: `/tmp/jemalloc_clickhouse.12345.0.heap.symbolized`) を返します。

その後、`jeprof` で直接解析できます。

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --output_format [ > output_file]
```

:::note

**バイナリは不要**: シンボル化されたプロファイル (`.symbolized` ファイル) を使用する場合、`jeprof` に ClickHouse バイナリのパスを指定する必要はありません。これにより、別のマシン上でも、またはバイナリ更新後でも、プロファイルをはるかに簡単に解析できます。

:::

古い非シンボル化ヒーププロファイルがあり、かつ ClickHouse バイナリに引き続きアクセスできる場合は、従来の方法を使用できます。

```sh
jeprof path/to/clickhouse path/to/heap/profile --output_format [ > output_file]
```

:::note

シンボル化されていないプロファイルでは、`jeprof` はスタックトレースの生成に `addr2line` を使用しますが、これにはかなり時間がかかることがあります。
その場合は、このツールの[代替実装](https://github.com/gimli-rs/addr2line)をインストールすることをお勧めします。

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

あるいは、`llvm-addr2line` も同様に利用できます (ただし、`llvm-objdump` は `jeprof` と互換性がない点に注意してください)

その後、次のように使用します：`jeprof --tools addr2line:/usr/bin/llvm-addr2line,nm:/usr/bin/llvm-nm,objdump:/usr/bin/objdump,c++filt:/usr/bin/llvm-cxxfilt`

:::

2 つのprofileを比較する場合は、`--base` 引数を使用できます。

```sh
jeprof --base /path/to/first.heap.symbolized /path/to/second.heap.symbolized --output_format [ > output_file]
```

<div id="examples">
  ### 例
</div>

シンボル化されたプロファイルを使用する (推奨) :

* 各プロシージャを1行に1つずつ記述したテキストファイルを生成します:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --text > result.txt
```

* call-graph付きの PDF ファイルを生成します:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --pdf > result.pdf
```

シンボル化されていないプロファイルを使用する場合 (バイナリが必要) :

* 各関数を1行ずつ記述したテキストファイルを生成します:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --text > result.txt
```

* コールグラフ付きのPDFファイルを生成します:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### フレームグラフの生成
</div>

`jeprof` を使用すると、フレームグラフの生成に使用するコラプスされたスタックを出力できます。

`--collapsed` 引数を使用する必要があります。

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --collapsed > result.collapsed
```

または、シンボル化されていないプロファイルの場合:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --collapsed > result.collapsed
```

その後、コラプスされたスタックを可視化するために、さまざまなツールを利用できます。

最もよく使われているのは [FlameGraph](https://github.com/brendangregg/FlameGraph) で、`flamegraph.pl` というスクリプトが含まれています。

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

もう1つの興味深いツールとして、収集したスタックをよりインタラクティブに分析できる [speedscope](https://www.speedscope.app/) があります。

<div id="additional-options-for-profiler">
  ## プロファイラの追加オプション
</div>

`jemalloc` には、プロファイラに関連するさまざまなオプションがあります。これらは、`MALLOC_CONF` 環境変数を変更することで制御できます。
たとえば、アロケーションサンプルの間隔は `lg_prof_sample` で制御できます。
ヒーププロファイルを N バイトごとにダンプしたい場合は、`lg_prof_interval` を使用して有効化できます。

オプションの完全な一覧については、`jemalloc` の[リファレンスページ](https://jemalloc.net/jemalloc.3.html)を確認することをお勧めします。

<div id="other-resources">
  ## その他のリソース
</div>

ClickHouse/Keeper は、`jemalloc` 関連のメトリクスをさまざまな形で公開しています。

:::warning 警告
これらのメトリクスは相互に同期されておらず、値にずれが生じる可能性がある点に注意してください。
:::

<div id="system-table-asynchronous_metrics">
  ### システムテーブル `asynchronous_metrics`
</div>

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric LIKE '%jemalloc%'
FORMAT Vertical
```

[参照](/ja/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### システムテーブル `jemalloc_bins`
</div>

すべてのアリーナから集計した、異なるサイズクラス (ビン) における jemalloc アロケータによるメモリ割り当てに関する情報が含まれます。

[リファレンス](/ja/operations/system-tables/jemalloc_bins)

<div id="system-table-jemalloc_stats">
  ### システムテーブル `jemalloc_stats` (26.2+)
</div>

`malloc_stats_print()` の出力全体を 1 つの文字列として返します。`SYSTEM JEMALLOC STATS` コマンドと同等です。

```sql
SELECT * FROM system.jemalloc_stats
```

<div id="prometheus">
  ### Prometheus
</div>

`asynchronous_metrics` の `jemalloc` 関連メトリクスはすべて、ClickHouse と Keeper の両方で Prometheus エンドポイント経由でも公開されています。

[Reference](/ja/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Keeper の `jmst` 4LW コマンド
</div>

Keeper は `jmst` 4LW コマンドをサポートしており、[基本的なアロケータの統計情報](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics)を返します。

```sh
echo jmst | nc localhost 9181
```