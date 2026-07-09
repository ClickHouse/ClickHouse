---
description: 'テーブルエンジンに関するドキュメント'
slug: /engines/table-engines/
toc_folder_title: 'テーブルエンジン'
toc_priority: 26
toc_title: '概要'
title: 'テーブルエンジン'
doc_type: 'reference'
---

テーブルエンジン (テーブルの種類) は、次の項目を決定します。

* データをどのように、どこに格納し、どこに書き込み、どこから読み取るか。
* どのクエリをどのようにサポートするか。
* データへの同時アクセス。
* 索引がある場合は、その利用方法。
* リクエストをマルチスレッドで実行できるかどうか。
* データのレプリケーションパラメータ。

<div id="engine-families">
  ## エンジンファミリー
</div>

<div id="mergetree">
  ### MergeTree
</div>

高負荷な処理向けの、最も汎用的で多機能なテーブルエンジンです。これらのエンジンに共通する特性は、高速なデータ挿入と、その後に行われるバックグラウンドでのデータ処理です。`MergeTree` ファミリーのエンジンは、データのレプリケーション (各エンジンの [Replicated*](/ja/engines/table-engines/mergetree-family/replication) バージョン) 、パーティション化、セカンダリのデータスキッピングインデックスのほか、他のエンジンではサポートされていない機能もサポートしています。

このファミリーに属するエンジン:

| MergeTree エンジン                                                                                       |
| ---------------------------------------------------------------------------------------------------- |
| [MergeTree](/ja/engines/table-engines/mergetree-family/mergetree)                                       |
| [ReplacingMergeTree](/ja/engines/table-engines/mergetree-family/replacingmergetree)                     |
| [SummingMergeTree](/ja/engines/table-engines/mergetree-family/summingmergetree)                         |
| [AggregatingMergeTree](/ja/engines/table-engines/mergetree-family/aggregatingmergetree)                 |
| [CollapsingMergeTree](/ja/engines/table-engines/mergetree-family/collapsingmergetree)                   |
| [VersionedCollapsingMergeTree](/ja/engines/table-engines/mergetree-family/versionedcollapsingmergetree) |
| [GraphiteMergeTree](/ja/engines/table-engines/mergetree-family/graphitemergetree)                       |
| [CoalescingMergeTree](/ja/engines/table-engines/mergetree-family/coalescingmergetree)                   |

<div id="log">
  ### Log
</div>

最小限の機能を備えた軽量な[エンジン](../../engines/table-engines/log-family/index.md)です。多数の小さなテーブル (約 100 万行まで) にすばやく書き込み、後でまとめて読み取る必要がある場合に最も効果を発揮します。

このファミリーに含まれるエンジン:

| Log エンジン                                                 |
| -------------------------------------------------------- |
| [TinyLog](/ja/engines/table-engines/log-family/tinylog)     |
| [StripeLog](/ja/engines/table-engines/log-family/stripelog) |
| [Log](/ja/engines/table-engines/log-family/log)             |

<div id="integration-engines">
  ### インテグレーションエンジン
</div>

他のデータストレージおよびデータ処理システムと連携するためのエンジンです。

このファミリーに属するエンジン:

| インテグレーションエンジン                                                                   |
| ------------------------------------------------------------------------------- |
| [ODBC](../../engines/table-engines/integrations/odbc.md)                        |
| [JDBC](../../engines/table-engines/integrations/jdbc.md)                        |
| [MySQL](../../engines/table-engines/integrations/mysql.md)                      |
| [MongoDB](../../engines/table-engines/integrations/mongodb.md)                  |
| [Redis](../../engines/table-engines/integrations/redis.md)                      |
| [HDFS](../../engines/table-engines/integrations/hdfs.md)                        |
| [S3](../../engines/table-engines/integrations/s3.md)                            |
| [Kafka](../../engines/table-engines/integrations/kafka.md)                      |
| [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md) |
| [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md)                |
| [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)            |
| [S3Queue](../../engines/table-engines/integrations/s3queue.md)                  |
| [TimeSeries](../../engines/table-engines/integrations/time-series.md)           |

<div id="special-engines">
  ### 特殊エンジン
</div>

このファミリーのエンジン:

| 特殊エンジン                                                        |
| ------------------------------------------------------------- |
| [Distributed](/ja/engines/table-engines/special/distributed)     |
| [Dictionary](/ja/engines/table-engines/special/dictionary)       |
| [Merge](/ja/engines/table-engines/special/merge)                 |
| [Executable](/ja/engines/table-engines/special/executable)       |
| [File](/ja/engines/table-engines/special/file)                   |
| [Null](/ja/engines/table-engines/special/null)                   |
| [Set](/ja/engines/table-engines/special/set)                     |
| [Join](/ja/engines/table-engines/special/join)                   |
| [URL](/ja/engines/table-engines/special/url)                     |
| [View](/ja/engines/table-engines/special/view)                   |
| [Memory](/ja/engines/table-engines/special/memory)               |
| [Buffer](/ja/engines/table-engines/special/buffer)               |
| [External Data](/ja/engines/table-engines/special/external-data) |
| [GenerateRandom](/ja/engines/table-engines/special/generate)     |
| [KeeperMap](/ja/engines/table-engines/special/keeper-map)        |
| [FileLog](/ja/engines/table-engines/special/filelog)             |

<div id="table_engines-virtual_columns">
  ## 仮想カラム
</div>

仮想カラムは、エンジンのソースコードで定義される、テーブルエンジンに固有の属性です。

仮想カラムは `CREATE TABLE` クエリで指定すべきではなく、`SHOW CREATE TABLE` および `DESCRIBE TABLE` のクエリ結果にも表示されません。また、仮想カラムは読み取り専用であるため、仮想カラムにデータを挿入することもできません。

仮想カラムからデータを選択するには、`SELECT` クエリでその名前を明示的に指定する必要があります。`SELECT *` では仮想カラムの値は返されません。

テーブルの仮想カラムの 1 つと同じ名前のカラムを持つテーブルを作成すると、その仮想カラムにはアクセスできなくなります。これは推奨されません。競合を避けやすくするため、仮想カラム名には通常、先頭にアンダースコアが付きます。

* `_table` — データの読み取り元のテーブル名を含みます。Type: [String](../../sql-reference/data-types/string.md).

  使用しているテーブルエンジンに関係なく、各テーブルには `_table` という名前の universal 仮想カラムが含まれます。

  Merge テーブルエンジンを使用するテーブルに対してクエリを実行する場合、`WHERE/PREWHERE` 句で `_table` に対する定数条件を設定できます (たとえば `WHERE _table='xyz'`) 。この場合、読み取り操作は `_table` の条件を満たすテーブルに対してのみ実行されるため、`_table` カラムは索引として機能します。

  `SELECT ... FROM (... UNION ALL ...)` のような形式のクエリを使用する場合、`_table` カラムを指定することで、返された行が実際にどのテーブルに由来するかを判別できます。