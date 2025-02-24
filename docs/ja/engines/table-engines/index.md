---
slug: /ja/engines/table-engines/
toc_folder_title: テーブルエンジン
toc_priority: 26
toc_title: はじめに
---

# テーブルエンジン

テーブルエンジン（テーブルの種類）は次の点を決定します：

- データの保存方法と保存場所、書き込みおよび読み込み先。
- サポートされているクエリとその方法。
- 同時データアクセス。
- インデックスの使用（存在する場合）。
- マルチスレッドでの要求実行が可能かどうか。
- データレプリケーションのパラメータ。

## エンジンファミリー {#engine-families}

### MergeTree {#mergetree}

高負荷タスク向けの最も汎用的かつ機能的なテーブルエンジンです。これらエンジンの共通の特徴は、データの迅速な挿入とその後のバックグラウンドでのデータ処理です。`MergeTree`ファミリーエンジンはデータのレプリケーション（エンジンの[Replicated\*](../../engines/table-engines/mergetree-family/replication.md#table_engines-replication) バージョン）、パーティション、二次データスキッピングインデックスなど、他のエンジンでサポートされていない機能をサポートしています。

このファミリーに含まれるエンジン：

- [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#mergetree)
- [ReplacingMergeTree](../../engines/table-engines/mergetree-family/replacingmergetree.md#replacingmergetree)
- [SummingMergeTree](../../engines/table-engines/mergetree-family/summingmergetree.md#summingmergetree)
- [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md#aggregatingmergetree)
- [CollapsingMergeTree](../../engines/table-engines/mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree)
- [VersionedCollapsingMergeTree](../../engines/table-engines/mergetree-family/versionedcollapsingmergetree.md#versionedcollapsingmergetree)
- [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md#graphitemergetree)

### Log {#log}

最小の機能を持つ軽量な[エンジン](../../engines/table-engines/log-family/index.md)です。小規模なテーブル（最大約100万行）を迅速に書き込み、その後全体を読み取りたい場合に最も効果的です。

このファミリーに含まれるエンジン：

- [TinyLog](../../engines/table-engines/log-family/tinylog.md#tinylog)
- [StripeLog](../../engines/table-engines/log-family/stripelog.md#stripelog)
- [Log](../../engines/table-engines/log-family/log.md#log)

### インテグレーションエンジン {#integration-engines}

他のデータストレージおよび処理システムと通信するためのエンジンです。

このファミリーに含まれるエンジン：

- [ODBC](../../engines/table-engines/integrations/odbc.md)
- [JDBC](../../engines/table-engines/integrations/jdbc.md)
- [MySQL](../../engines/table-engines/integrations/mysql.md)
- [MongoDB](../../engines/table-engines/integrations/mongodb.md)
- [Redis](../../engines/table-engines/integrations/redis.md)
- [HDFS](../../engines/table-engines/integrations/hdfs.md)
- [S3](../../engines/table-engines/integrations/s3.md)
- [Kafka](../../engines/table-engines/integrations/kafka.md)
- [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md)
- [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md)
- [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)
- [S3Queue](../../engines/table-engines/integrations/s3queue.md)
- [TimeSeries](../../engines/table-engines/integrations/time-series.md)

### 特殊エンジン {#special-engines}

このファミリーに含まれるエンジン：

- [分散テーブル](../../engines/table-engines/special/distributed.md#distributed)
- [Dictionary](../../engines/table-engines/special/dictionary.md#dictionary)
- [Merge](../../engines/table-engines/special/merge.md#merge)
- [File](../../engines/table-engines/special/file.md#file)
- [Null](../../engines/table-engines/special/null.md#null)
- [Set](../../engines/table-engines/special/set.md#set)
- [Join](../../engines/table-engines/special/join.md#join)
- [URL](../../engines/table-engines/special/url.md#table_engines-url)
- [View](../../engines/table-engines/special/view.md#table_engines-view)
- [Memory](../../engines/table-engines/special/memory.md#memory)
- [Buffer](../../engines/table-engines/special/buffer.md#buffer)
- [KeeperMap](../../engines/table-engines/special/keepermap.md)

## 仮想カラム {#table_engines-virtual_columns}

仮想カラムは、エンジンのソースコード内で定義されている統合テーブルエンジンの属性です。

`CREATE TABLE` クエリで仮想カラムを指定すべきではなく、`SHOW CREATE TABLE` や `DESCRIBE TABLE` クエリの結果に仮想カラムが表示されることはありません。仮想カラムは読み取り専用のため、仮想カラムにデータを挿入することもできません。

仮想カラムからデータを選択するには、`SELECT` クエリでその名前を指定する必要があります。`SELECT *` では仮想カラムの値は返されません。

テーブルを作成する際に、仮想カラムと同じ名前のカラムを作成した場合、仮想カラムは使用できなくなります。このようなことはお勧めしません。衝突を避けるために、仮想カラムの名前には通常アンダースコアを接頭辞として付けられています。
