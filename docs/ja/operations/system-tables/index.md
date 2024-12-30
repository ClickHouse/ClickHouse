---
slug: /ja/operations/system-tables/
sidebar_position: 52
sidebar_label: 概要
pagination_next: 'en/operations/system-tables/asynchronous_metric_log'
---

# システムテーブル

## はじめに {#system-tables-introduction}

システムテーブルは次の情報を提供します：

- サーバーの状態、プロセス、環境。
- サーバーの内部プロセス。
- ClickHouseバイナリがビルドされたときのオプション。

システムテーブル：

- `system`データベースに配置されています。
- データの読み取り専用で利用可能です。
- 削除や変更はできませんが、デタッチすることは可能です。

ほとんどのシステムテーブルはそのデータをRAMに保存します。ClickHouseサーバーは起動時にそのようなシステムテーブルを作成します。

他のシステムテーブルとは異なり、システムログテーブルである[metric_log](../../operations/system-tables/metric_log.md)、[query_log](../../operations/system-tables/query_log.md)、[query_thread_log](../../operations/system-tables/query_thread_log.md)、[trace_log](../../operations/system-tables/trace_log.md)、[part_log](../../operations/system-tables/part_log.md)、[crash_log](../../operations/system-tables/crash-log.md)、[text_log](../../operations/system-tables/text_log.md)、[backup_log](../../operations/system-tables/backup_log.md)は、[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)テーブルエンジンによって提供され、デフォルトでデータをファイルシステムに保存します。ファイルシステムからテーブルを削除すると、次回のデータ書き込み時にClickHouseサーバーが新しい空のテーブルを再作成します。新しいリリースでシステムテーブルのスキーマが変更された場合、ClickHouseは現在のテーブルの名前を変更し、新しいものを作成します。

システムログテーブルは、`/etc/clickhouse-server/config.d/`にテーブルと同じ名前の構成ファイルを作成するか、`/etc/clickhouse-server/config.xml`に対応する要素を設定することでカスタマイズできます。カスタマイズ可能な要素は次のとおりです：

- `database`: システムログテーブルが属するデータベース。このオプションは現在非推奨です。すべてのシステムログテーブルは `system` データベースにあります。
- `table`: データを挿入するテーブル。
- `partition_by`: [PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)式を指定します。
- `ttl`: テーブルの[有効期限 (TTL)](../../sql-reference/statements/alter/ttl.md)式を指定します。
- `flush_interval_milliseconds`: ディスクへのデータフラッシュの間隔を設定します。
- `engine`: パラメータを含む完全なエンジン式（`ENGINE =` から始まる）を指定します。このオプションは `partition_by` および `ttl` と衝突します。両方が設定されている場合、サーバーは例外を発生させて終了します。

例：

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

デフォルトでは、テーブルの成長は無制限です。テーブルのサイズを制御するには、古いログレコードを削除するために[TTL](../../sql-reference/statements/alter/ttl.md#manipulations-with-table-ttl)設定を使用できます。また、`MergeTree` エンジンテーブルのパーティショニング機能を使用することもできます。

## システムメトリックの情報源 {#system-tables-sources-of-system-metrics}

システムメトリックを収集するためにClickHouseサーバーは次を使用します：

- `CAP_NET_ADMIN`の権限。
- [procfs](https://en.wikipedia.org/wiki/Procfs)（Linuxのみ）。

**procfs**

ClickHouseサーバーが`CAP_NET_ADMIN`の権限を持っていない場合、`ProcfsMetricsProvider`にフォールバックしようとします。`ProcfsMetricsProvider`は、CPUおよびI/Oのクエリごとのシステムメトリックを収集します。

システムでprocfsがサポートされ、有効化されている場合、ClickHouseサーバーはこれらのメトリックを収集します：

- `OSCPUVirtualTimeMicroseconds`
- `OSCPUWaitMicroseconds`
- `OSIOWaitMicroseconds`
- `OSReadChars`
- `OSWriteChars`
- `OSReadBytes`
- `OSWriteBytes`

:::note
Linuxカーネルの5.14.x以降では、`OSIOWaitMicroseconds`はデフォルトで無効です。
`sudo sysctl kernel.task_delayacct=1`を使用するか、`/etc/sysctl.d/`内に`.conf`ファイルを作成し、`kernel.task_delayacct = 1`と設定して有効にできます。
:::

## 関連コンテンツ

- ブログ: [ClickHouseの内部を視覚化するシステムテーブル](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
- ブログ: [重要な監視クエリ - パート1 - INSERTクエリ](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
- ブログ: [重要な監視クエリ - パート2 - SELECTクエリ](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)
