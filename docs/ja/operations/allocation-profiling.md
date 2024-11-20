---
slug: /ja/operations/allocation-profiling
sidebar_label: "アロケーションプロファイリング"
title: "アロケーションプロファイリング"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# アロケーションプロファイリング

ClickHouseは、[jemalloc](https://github.com/jemalloc/jemalloc)をグローバルアロケーターとして使用しており、これはアロケーションのサンプリングやプロファイリング用のいくつかのツールを提供しています。  
アロケーションプロファイリングをより便利にするために、Keeperには`SYSTEM`コマンドと4LWコマンドが提供されています。

## アロケーションのサンプリングとヒーププロファイルのフラッシュ

`jemalloc`でアロケーションをサンプリングしてプロファイルするためには、環境変数`MALLOC_CONF`を使用してプロファイリングを有効にしてClickHouse/Keeperを起動する必要があります。

```sh
MALLOC_CONF=background_thread:true,prof:true
```

`jemalloc`はアロケーションをサンプリングし、情報を内部に保持します。

現在のプロファイルをフラッシュするように`jemalloc`に指示するには、次のように実行します：

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">

    SYSTEM JEMALLOC FLUSH PROFILE

</TabItem>
<TabItem value="keeper" label="Keeper">

    echo jmfp | nc localhost 9181

</TabItem>
</Tabs>

デフォルトでは、ヒーププロファイルファイルは`/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`に生成され、`_pid_`はClickHouseのPID、`_seqnum_`は現在のヒーププロファイルのグローバルシーケンス番号です。  
Keeperの場合、デフォルトのファイルは同じルールに従って`/tmp/jemalloc_keeper._pid_._seqnum_.heap`になります。

別の場所を指定するには、`MALLOC_CONF`環境変数に`prof_prefix`オプションを追加します。  
たとえば、`/data`フォルダにプロファイルを生成したい場合、ファイル名のプレフィックスを`my_current_profile`とするには、ClickHouse/Keeperを以下の環境変数で実行します：
```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
```
生成されたファイルにはプレフィックスにPIDとシーケンス番号が追加されます。

## ヒーププロファイルの分析

ヒーププロファイルを生成した後、それを分析する必要があります。  
これには、`jemalloc`のツールである[jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in)を使用する必要があります。このツールは以下の方法でインストールできます：
- システムのパッケージマネージャーを使用して`jemalloc`をインストールする
- [jemallocリポジトリ](https://github.com/jemalloc/jemalloc)をクローンし、ルートフォルダーからautogen.shを実行すると、`bin`フォルダ内に`jeprof`スクリプトが用意されます

:::note
`jeprof`は`addr2line`を使用してスタックトレースを生成しますが、これが非常に遅い場合があります。  
その場合は、[代替実装](https://github.com/gimli-rs/addr2line)のインストールをお勧めします。

```
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```
:::

`jeprof`を使用してヒーププロファイルから生成できる多くの異なる形式があります。
`jeprof --help`を実行し、利用法およびツールが提供する多くの異なるオプションを確認することをお勧めします。

一般に、`jeprof`コマンドは次のようになります：

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

2つのプロファイル間でどのアロケーションが発生したかを比較したい場合、ベース引数を設定できます：

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

例えば：

- 各手続きが1行ごとに書かれたテキストファイルを生成したい場合：

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

- コールグラフ付きのPDFファイルを生成したい場合：

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

### フレームグラフの生成

`jeprof`を使用してフレームグラフを作成するために折りたたまれたスタックを生成できます。

`--collapsed`引数を使用する必要があります：

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
```

その後、多くの異なるツールを使用して折りたたまれたスタックを視覚化できます。

最も人気があるのは、`flamegraph.pl`スクリプトを含む[FlameGraph](https://github.com/brendangregg/FlameGraph)です：

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

さらに、収集されたスタックをよりインタラクティブに分析するためのツールである[speedscope](https://www.speedscope.app/)も興味深いです。

## 実行時にアロケーションプロファイラーを制御する

ClickHouse/Keeperがプロファイラーを有効にして開始された場合、実行時にアロケーションプロファイリングを無効/有効にするための追加コマンドがサポートされています。
これらのコマンドを使用することで、特定の間隔だけをプロファイル化するのが容易になります。

プロファイラーを無効にする：

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">

    SYSTEM JEMALLOC DISABLE PROFILE

</TabItem>
<TabItem value="keeper" label="Keeper">

    echo jmdp | nc localhost 9181

</TabItem>
</Tabs>

プロファイラーを有効にする：

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">

    SYSTEM JEMALLOC ENABLE PROFILE

</TabItem>
<TabItem value="keeper" label="Keeper">

    echo jmep | nc localhost 9181

</TabItem>
</Tabs>

プロファイラーの初期状態を制御することも可能で、デフォルトで有効になっている`prof_active`オプションを設定することができます。  
たとえば、起動時にアロケーションのサンプリングを行わず、プロファイラーを有効にした後でのみ行いたい場合、以下の環境変数を使用してClickHouse/Keeperを起動することができます：
```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

その後、後の段階でプロファイラーを有効にすることができます。

## プロファイラーの追加オプション

`jemalloc`にはプロファイラーに関連するさまざまなオプションがあり、これを変更することで`MALLOC_CONF`環境変数を通じて制御できます。  
たとえば、アロケーションサンプルの間隔は`lg_prof_sample`で制御できます。  
Nバイトごとにヒーププロファイルをダンプする場合、`lg_prof_interval`を使用して有効にできます。

これらのオプションについては、`jemalloc`の[リファレンスページ](https://jemalloc.net/jemalloc.3.html)を確認することをお勧めします。

## その他のリソース

ClickHouse/Keeperは、`jemalloc`関連のメトリクスをさまざまな方法で公開しています。

:::warning 警告
これらのメトリクスのいずれも互いに同期されておらず、値がずれる可能性があることを意識してください。
:::

### システムテーブル `asynchronous_metrics`

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric ILIKE '%jemalloc%'
FORMAT Vertical
```

[参照](/ja/operations/system-tables/asynchronous_metrics)

### システムテーブル `jemalloc_bins`

異なるサイズクラス（ビン）でのjemallocアロケーターを介して行われたメモリアロケーションに関する情報を、すべてのアリーナから集約したものを含みます。

[参照](/ja/operations/system-tables/jemalloc_bins)

### Prometheus

`asynchronous_metrics`からのすべての`jemalloc`関連メトリクスは、ClickHouseとKeeperの両方でPrometheusエンドポイントを使用して公開されています。

[参照](/ja/operations/server-configuration-parameters/settings#prometheus)

### Keeperの`jmst` 4LWコマンド

Keeperは、[基本アロケーター統計](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics)を返す`jmst` 4LWコマンドをサポートしています。

例：
```sh
echo jmst | nc localhost 9181
```
