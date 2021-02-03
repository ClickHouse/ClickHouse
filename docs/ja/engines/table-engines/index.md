---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u8868\u30A8\u30F3\u30B8\u30F3"
toc_priority: 26
toc_title: "\u306F\u3058\u3081\u306B"
---

# 表エンジン {#table_engines}

のテーブルエンジン型式の表を行います。:

-   データの格納方法と場所、データの書き込み先、およびデータの読み取り先。
-   サポートされているクエリと方法。
-   同時データアクセス。
-   インデックスが存在する場合の使用。
-   マルチスレッド要求の実行が可能かどうか。
-   データ複製パラメーター。

## エンジン家族 {#engine-families}

### メルゲツリー {#mergetree}

高負荷仕事のための最も普遍的な、機能テーブルエンジン。 本物件の共有によるこれらのエンジンには迅速にデータを挿入とその後のバックグラウンドデータを処となります。 `MergeTree` 家族のエンジンの支援データレプリケーション（ [複製\*](mergetree-family/replication.md#table_engines-replication) バージョンのエンジン)分割、その他の機能で対応していないその他のエンジンです。

家族のエンジン:

-   [メルゲツリー](mergetree-family/mergetree.md#mergetree)
-   [置換マージツリー](mergetree-family/replacingmergetree.md#replacingmergetree)
-   [サミングマーゲツリー](mergetree-family/summingmergetree.md#summingmergetree)
-   [AggregatingMergeTree](mergetree-family/aggregatingmergetree.md#aggregatingmergetree)
-   [折りたたみマージツリー](mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree)
-   [バージョニングコラプシングマーゲットリー](mergetree-family/versionedcollapsingmergetree.md#versionedcollapsingmergetree)
-   [GraphiteMergeTree](mergetree-family/graphitemergetree.md#graphitemergetree)

### ログ {#log}

軽量 [エンジン](log-family/index.md) 最低の機能性を使って。 多くの小さなテーブル（最大約1万行）をすばやく書き込み、後で全体として読み込む必要がある場合に最も効果的です。

家族のエンジン:

-   [TinyLog](log-family/tinylog.md#tinylog)
-   [ストリップログ](log-family/stripelog.md#stripelog)
-   [ログ](log-family/log.md#log)

### 統合エンジン {#integration-engines}

エンジン用プリケーションデータストレージと処理システム。

家族のエンジン:

-   [カフカ](integrations/kafka.md#kafka)
-   [MySQL](integrations/mysql.md#mysql)
-   [ODBC](integrations/odbc.md#table-engine-odbc)
-   [JDBC](integrations/jdbc.md#table-engine-jdbc)
-   [HDFS](integrations/hdfs.md#hdfs)

### 特殊エンジン {#special-engines}

家族のエンジン:

-   [分散](special/distributed.md#distributed)
-   [マテリアライズドビュー](special/materializedview.md#materializedview)
-   [辞書](special/dictionary.md#dictionary)
-   \[Merge\](special/merge.md#merge
-   [ファイル](special/file.md#file)
-   [Null](special/null.md#null)
-   [セット](special/set.md#set)
-   [参加](special/join.md#join)
-   [URL](special/url.md#table_engines-url)
-   [表示](special/view.md#table_engines-view)
-   [メモリ](special/memory.md#memory)
-   [バッファ](special/buffer.md#buffer)

## 仮想列 {#table_engines-virtual_columns}

仮想列は、エンジンのソースコードで定義されている整数テーブルエンジン属性です。

仮想列を指定するべきではありません。 `CREATE TABLE` あなたはそれらを見ることができません `SHOW CREATE TABLE` と `DESCRIBE TABLE` クエリ結果。 仮想列も読み取り専用であるため、仮想列にデータを挿入することはできません。

仮想列からデータを選択するには、仮想列の名前を指定する必要があります。 `SELECT` クエリ。 `SELECT *` 仮想列から値を返しません。

テーブル仮想列のいずれかと同じ名前の列を持つテーブルを作成すると、仮想列にアクセスできなくなります。 これはお勧めしません。 競合を避けるために、仮想列名には通常、アンダースコアが付けられます。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
