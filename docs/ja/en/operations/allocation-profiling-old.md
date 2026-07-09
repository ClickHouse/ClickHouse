---
description: 'ClickHouse のアロケーションプロファイリングについて詳しく説明するページ'
sidebar_label: '25.9 より前のバージョンのアロケーションプロファイリング'
slug: /operations/allocation-profiling-old
title: '25.9 より前のバージョンのアロケーションプロファイリング'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling-for-versions-before-259">
  # 25.9 より前のバージョンにおけるアロケーションプロファイリング
</div>

ClickHouse はグローバルアロケータとして [jemalloc](https://github.com/jemalloc/jemalloc) を使用しています。jemalloc には、アロケーションのサンプリングとプロファイリングを行うためのツールがいくつか備わっています。
アロケーションプロファイリングをより簡単に行えるよう、Keeper では four letter word (4LW) コマンドに加えて `SYSTEM` コマンドも提供されています。

<div id="sampling-allocations-and-flushing-heap-profiles">
  ## 割り当てのサンプリングとヒーププロファイルの書き出し
</div>

`jemalloc` で割り当てのサンプリングとプロファイリングを行うには、環境変数 `MALLOC_CONF` を使ってプロファイリングを有効にした状態で ClickHouse/Keeper を起動する必要があります。

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:true
```

`jemalloc` は割り当てをサンプリングし、その情報を内部に保持します。

次を実行すると、現在のプロファイルをフラッシュするよう `jemalloc` に指示できます。

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

デフォルトでは、ヒーププロファイルファイルは `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap` に生成されます。ここで、`_pid_` は ClickHouse の PID、`_seqnum_` は現在のヒーププロファイルに対応するグローバルな連番です。
Keeper の場合、デフォルトのファイルは `/tmp/jemalloc_keeper._pid_._seqnum_.heap` で、同じルールに従います。

別の場所を指定するには、`MALLOC_CONF` 環境変数に `prof_prefix` オプションを追加します。
たとえば、ファイル名のプレフィックスを `my_current_profile` として `/data` フォルダーにプロファイルを生成したい場合は、次の環境変数を指定して ClickHouse/Keeper を実行できます。

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
```

生成されるファイル名には、プレフィックス、PID、および連番が付加されます。

<div id="analyzing-heap-profiles">
  ## ヒーププロファイル の分析
</div>

ヒーププロファイル が生成されたら、次はそれを分析する必要があります。
そのためには、[jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in) という `jemalloc` のツールを使用できます。インストール方法はいくつかあります。

* システムのパッケージマネージャーを使用する
* [jemalloc リポジトリ](https://github.com/jemalloc/jemalloc) をクローンし、ルートフォルダーで `autogen.sh` を実行する。これにより、`bin` フォルダー内に `jeprof` スクリプトが用意されます

:::note
`jeprof` は `addr2line` を使用してスタックトレースを生成しますが、これには非常に時間がかかることがあります。
その場合は、このツールの[代替実装](https://github.com/gimli-rs/addr2line)をインストールすることをおすすめします。

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

:::

`jeprof` を使うと、ヒーププロファイル からさまざまなフォーマットを生成できます。
使い方や、このツールで利用できる各種オプションについては、`jeprof --help` を実行して確認することをお勧めします。

一般的な `jeprof` コマンドの使用方法は次のとおりです。

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

2 つのプロファイルの間でどの割り当てが発生したかを比較したい場合は、`base` 引数を指定できます。

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

<div id="examples">
  ### 例
</div>

* 各プロシージャを1行ずつ記載したテキストファイルを生成したい場合:

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

* call-graph を含む PDF ファイルを生成する場合:

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### フレームグラフの生成
</div>

`jeprof` を使うと、フレームグラフの作成に使用するコラプスされたスタックを生成できます。

`--collapsed` 引数を使用する必要があります。

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
```

その後は、コラプスされたスタックを可視化するために、さまざまなツールを利用できます。

中でも最も広く使われているのが [FlameGraph](https://github.com/brendangregg/FlameGraph) で、`flamegraph.pl` というスクリプトが含まれています。

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

もう 1 つの便利なツールとして、収集したスタックをよりインタラクティブに分析できる [speedscope](https://www.speedscope.app/) があります。

<div id="controlling-allocation-profiler-during-runtime">
  ## 実行時に allocation profiler を制御する
</div>

ClickHouse/Keeper をプロファイラ有効の状態で起動している場合は、実行時に allocation profiling を無効化/有効化する追加コマンドを利用できます。
これらのコマンドを使うと、特定のインターバルだけをより簡単にプロファイリングできます。

プロファイラを無効にするには、次を実行します。

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC DISABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmdp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

プロファイラを有効にするには、次を実行します。

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC ENABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmep | nc localhost 9181
    ```
  </TabItem>
</Tabs>

また、デフォルトで有効な `prof_active` オプションを設定することで、プロファイラの初期状態を制御することもできます。
たとえば、起動中は割り当てをサンプリングせず、起動後だけサンプリングしたい場合は、プロファイラを有効にできます。ClickHouse/Keeper は次の環境変数を指定して起動できます。

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

プロファイラは後から有効にできます。

<div id="additional-options-for-profiler">
  ## プロファイラの追加オプション
</div>

`jemalloc` には、プロファイラ関連のさまざまなオプションがあります。これらは `MALLOC_CONF` 環境変数を変更することで制御できます。
たとえば、割り当てサンプルの間隔は `lg_prof_sample` で制御できます。
N バイトごとに ヒーププロファイル をダンプしたい場合は、`lg_prof_interval` を有効にします。

オプションの完全な一覧については、`jemalloc` の[リファレンスページ](https://jemalloc.net/jemalloc.3.html)を確認することをお勧めします。

<div id="other-resources">
  ## その他のリソース
</div>

ClickHouse/Keeper では、`jemalloc` 関連のメトリクスがさまざまな形で公開されています。

:::warning 警告
これらのメトリクスは相互に同期されていないため、値にずれが生じる可能性がある点に注意してください。
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

[リファレンス](/ja/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### システムテーブル `jemalloc_bins`
</div>

すべてのアリーナから集計された、各サイズクラス (ビン) における jemalloc アロケータによるメモリ割り当てに関する情報が含まれています。

[リファレンス](/ja/operations/system-tables/jemalloc_bins)

<div id="prometheus">
  ### Prometheus
</div>

`asynchronous_metrics` にある `jemalloc` 関連のメトリクスは、ClickHouse と Keeper の両方で Prometheus エンドポイント経由でも公開されています。

[参照](/ja/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Keeper の `jmst` 4LW コマンド
</div>

Keeper は `jmst` 4LW コマンドをサポートしており、[基本的なアロケータの統計情報](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics)を返します:

```sh
echo jmst | nc localhost 9181
```