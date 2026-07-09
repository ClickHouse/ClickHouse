---
description: 'システムテーブルとは何か、そしてそれがなぜ役立つのかを概説します。'
keywords: ['システムテーブル', '概要']
sidebar_label: '概要'
sidebar_position: 52
slug: /operations/system-tables/overview
title: 'システムテーブルの概要'
doc_type: 'reference'
---

<div id="system-tables-introduction">
  ## システムテーブルの概要
</div>

システムテーブルは、次の情報を提供します。

* サーバーの状態、プロセス、環境。
* サーバーの内部プロセス。
* ClickHouseバイナリのビルド時に使用されたオプション。

システムテーブルには、次の特徴があります。

* `system` データベースに配置されています。
* データの読み取り専用です。
* drop や alter はできませんが、detach は可能です。

ほとんどのシステムテーブルは、データをRAMに格納します。ClickHouseサーバーは起動時にこのようなシステムテーブルを作成します。

ほかのシステムテーブルとは異なり、システムログテーブル [metric&#95;log](../../operations/system-tables/metric_log.md)、[query&#95;log](../../operations/system-tables/query_log.md)、[query&#95;thread&#95;log](../../operations/system-tables/query_thread_log.md)、[trace&#95;log](../../operations/system-tables/trace_log.md)、[part&#95;log](../../operations/system-tables/part_log.md)、[crash&#95;log](../../operations/system-tables/crash_log.md)、[text&#95;log](../../operations/system-tables/text_log.md)、および [backup&#95;log](../../operations/system-tables/backup_log.md) は [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルエンジンで管理され、デフォルトではファイルシステムにデータを保存します。ファイルシステムからテーブルを削除すると、ClickHouseサーバーは次回データ書き込み時に再び空のテーブルを作成します。新しいリリースでシステムテーブルのスキーマが変更された場合、ClickHouseは現在のテーブルをリネームし、新しいテーブルを作成します。

システムログテーブルは、`/etc/clickhouse-server/config.d/` 配下にテーブルと同じ名前の設定ファイルを作成するか、`/etc/clickhouse-server/config.xml` で対応する要素を設定することでカスタマイズできます。カスタマイズ可能な要素は次のとおりです。

* `database`: システムログテーブルが属するデータベース。このオプションは現在非推奨です。すべてのシステムログテーブルは `system` データベース配下にあります。
* `table`: データを insert するテーブル。
* `partition_by`: [PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) 式を指定します。
* `ttl`: テーブルの [TTL](../../sql-reference/statements/alter/ttl.md) 式を指定します。
* `flush_interval_milliseconds`: データをディスクへ flush する間隔。
* `engine`: パラメータ付きの完全な engine 式 (`ENGINE =` で開始) を指定します。このオプションは `partition_by` および `ttl` と競合します。同時に設定すると、サーバーは例外を発生させて終了します。

例:

```xml
<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <ttl>event_date + INTERVAL 30 DAY DELETE</ttl>
        <!--
        <engine>ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024</engine>
        -->
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_log>
</clickhouse>
```

デフォルトでは、テーブルサイズの増加に上限はありません。テーブルのサイズを制御するには、古いログレコードを削除するために [有効期限 (TTL)](/ja/sql-reference/statements/alter/ttl) の設定を使用できます。また、`MergeTree` エンジンのテーブルでパーティション化機能を利用することもできます。

<div id="system-tables-sources-of-system-metrics">
  ## システムメトリクスの取得元
</div>

ClickHouseサーバー は、システムメトリクスの収集に以下を使用します。

* `CAP_NET_ADMIN` ケーパビリティ。
* [procfs](https://en.wikipedia.org/wiki/Procfs) (Linux のみ) 。

**procfs**

ClickHouseサーバー に `CAP_NET_ADMIN` ケーパビリティがない場合は、`ProcfsMetricsProvider` にフォールバックしようとします。`ProcfsMetricsProvider` を使うと、クエリごとのシステムメトリクス (CPU および I/O) を収集できます。

システムで procfs がサポートされ、有効になっている場合、ClickHouseサーバー は次のメトリクスを収集します。

* `OSCPUVirtualTimeMicroseconds`
* `OSCPUWaitMicroseconds`
* `OSIOWaitMicroseconds`
* `OSReadChars`
* `OSWriteChars`
* `OSReadBytes`
* `OSWriteBytes`

:::note
`OSIOWaitMicroseconds` は、Linux カーネル 5.14.x 以降ではデフォルトで無効です。
`sudo sysctl kernel.task_delayacct=1` を使用するか、`/etc/sysctl.d/` に `kernel.task_delayacct = 1` を含む `.conf` ファイルを作成することで有効にできます。
:::

<div id="system-tables-in-clickhouse-cloud">
  ## ClickHouse Cloud のシステムテーブル
</div>

ClickHouse Cloud では、システムテーブルはセルフマネージド環境と同様に、サービスの状態やパフォーマンスに関する重要な情報を提供します。システムテーブルの一部はクラスター全体のレベルで動作し、特に分散メタデータを管理する Keeper ノードからデータを取得するものがこれに該当します。これらのテーブルはクラスター全体の状態を反映しており、個々のノードでクエリしても一貫した結果になるはずです。たとえば、[`parts`](/ja/operations/system-tables/parts) は、どのノードからクエリしても一貫している必要があります。

```sql
SELECT hostname(), count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-vccsrty-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.005 sec.

SELECT
 hostname(),
    count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-w59bfco-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.004 sec.
```

一方、他のシステムテーブルはノード固有です。たとえば、メモリ内に保持されるものや、MergeTreeテーブルエンジンを使用してデータを永続化するものがあります。これは、ログやメトリクスのようなデータで一般的です。この永続化により、履歴データを分析に利用できます。ただし、これらのノード固有のテーブルは、本質的に各ノードごとに異なります。

一般に、システムテーブルがノード固有かどうかを判断する際には、次のルールを適用できます。

* `_log` 接尾辞を持つシステムテーブル。
* メトリクスを公開するシステムテーブル。たとえば `metrics`、`asynchronous_metrics`、`events`。
* 進行中のプロセスを公開するシステムテーブル。たとえば `processes`、`merges`。

さらに、システムテーブルの新しいバージョンが、アップグレードやスキーマの変更に伴って作成されることがあります。これらのバージョンは、数値の接尾辞を使って命名されます。

たとえば、`system.query_log` テーブルを考えてみましょう。これには、そのノードで実行された各クエリについて 1 行が含まれます。

```sql
SHOW TABLES FROM system LIKE 'query_log%'

┌─name─────────┐
│ query_log    │
│ query_log_1  │
│ query_log_10 │
│ query_log_2  │
│ query_log_3  │
│ query_log_4  │
│ query_log_5  │
│ query_log_6  │
│ query_log_7  │
│ query_log_8  │
│ query_log_9  │
└──────────────┘

11 rows in set. Elapsed: 0.004 sec.
```

<div id="querying-multiple-versions">
  ### 複数のバージョンをまたいだクエリ
</div>

[`merge`](/ja/sql-reference/table-functions/merge) 関数を使うと、これらのテーブルをまたいでクエリできます。たとえば、次のクエリは各 `query_log` テーブルについて、対象ノードに対して発行された最新のクエリを特定します。

```sql
SELECT
    _table,
    max(event_time) AS most_recent
FROM merge('system', '^query_log')
GROUP BY _table
ORDER BY most_recent DESC

┌─_table───────┬─────────most_recent─┐
│ query_log    │ 2025-04-13 10:59:29 │
│ query_log_1  │ 2025-04-09 12:34:46 │
│ query_log_2  │ 2025-04-09 12:33:45 │
│ query_log_3  │ 2025-04-07 17:10:34 │
│ query_log_5  │ 2025-03-24 09:39:39 │
│ query_log_4  │ 2025-03-24 09:38:58 │
│ query_log_6  │ 2025-03-19 16:07:41 │
│ query_log_7  │ 2025-03-18 17:01:07 │
│ query_log_8  │ 2025-03-18 14:36:07 │
│ query_log_10 │ 2025-03-18 14:01:33 │
│ query_log_9  │ 2025-03-18 14:01:32 │
└──────────────┴─────────────────────┘

11 rows in set. Elapsed: 0.373 sec. Processed 6.44 million rows, 25.77 MB (17.29 million rows/s., 69.17 MB/s.)
Peak memory usage: 28.45 MiB.
```

:::note 並び順の判断を数値の接尾辞に頼らないでください
テーブル名の数値の接尾辞はデータの順序を示しているように見えることがありますが、それを当てにしてはいけません。そのため、特定の日付範囲を対象にする場合は、必ず日付フィルタと組み合わせて `merge` テーブル関数を使用してください。
:::

重要なのは、これらのテーブルは依然として**各ノードのローカル**であるという点です。

<div id="querying-across-nodes">
  ### ノードをまたいだクエリ
</div>

クラスター全体を包括的に把握するには、[`clusterAllReplicas`](/ja/sql-reference/table-functions/cluster) 関数を `merge` 関数と組み合わせて使用できます。`clusterAllReplicas` 関数を使うと、&quot;default&quot; クラスター内のすべてのレプリカにまたがってシステムテーブルをクエリし、ノードごとのデータを1つの結果に集約できます。これを `merge` 関数と組み合わせることで、クラスター内の特定のテーブルに関するすべてのシステムデータを対象にできます。

この方法は、クラスター全体に関わる操作の監視やデバッグに特に有用で、ClickHouse Cloud デプロイメントの健全性とパフォーマンスを効果的に分析するのに役立ちます。

:::note
ClickHouse Cloud は、冗長性とフェイルオーバーのために複数のレプリカで構成されたクラスターを提供します。これにより、動的オートスケーリングやダウンタイムのないアップグレードなどの機能が実現されています。ある時点では、新しいノードがクラスターに追加されている途中だったり、クラスターから削除されている途中だったりする場合があります。こうしたノードをスキップするには、以下のように `clusterAllReplicas` を使用するクエリに `SETTINGS skip_unavailable_shards = 1` を追加してください。
:::

たとえば、`query_log` テーブルをクエリした場合の違いを見てみましょう。これは分析で重要になることがよくあります。

```sql
SELECT
    hostname() AS host,
    count()
FROM system.query_log
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.010 sec. Processed 17.87 thousand rows, 71.51 KB (1.75 million rows/s., 7.01 MB/s.)

SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', system.query_log)
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
│ c-ecru-qn-34-server-6em4y4t-0 │  656029 │
│ c-ecru-qn-34-server-iejrkg0-0 │  641155 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.026 sec. Processed 1.97 million rows, 7.88 MB (75.51 million rows/s., 302.05 MB/s.)
```

<div id="querying-across-nodes-and-versions">
  ### ノードとバージョンをまたいだクエリ
</div>

システムテーブルのバージョン管理のため、これだけでは依然としてクラスター内の全データを完全には表せません。これに上記の内容と `merge` 関数を組み合わせると、対象の日付範囲に対して正確な結果が得られます。

```sql
SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', merge('system', '^query_log'))
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │ 3008000 │
│ c-ecru-qn-34-server-6em4y4t-0 │ 3659443 │
│ c-ecru-qn-34-server-iejrkg0-0 │ 1078287 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.462 sec. Processed 7.94 million rows, 31.75 MB (17.17 million rows/s., 68.67 MB/s.)
```

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [システムテーブルで見るClickHouseの内部](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
* ブログ: [監視に欠かせないクエリ - 第1部 - INSERTクエリ](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
* ブログ: [監視に欠かせないクエリ - 第2部 - SELECTクエリ](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)