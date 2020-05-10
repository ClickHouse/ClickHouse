---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_folder_title: Table Engines
toc_priority: 26
toc_title: "\u5C0E\u5165"
---

# 表エンジン {#table_engines}

表エンジン(表のタイプ:

-   どのようにデータが格納されている場所、それをどこに書き込むか、どこから読み込むか。
-   どのクエリがサポートされ、どのように。
-   同時データアクセス。
-   インデックスが存在する場合の使用。
-   マルチスレッドリクエストの実行が可能かどうか。
-   データ複製パラメーター。

## エンジン家族 {#engine-families}

### Mergetree {#mergetree}

高負荷仕事のための最も普遍的な、機能テーブルエンジン。 本物件の共有によるこれらのエンジンには迅速にデータを挿入とその後のバックグラウンドデータを処となります。 `MergeTree` 家族のエンジンの支援データレプリケーション（ [複製された\*](mergetree-family/replication.md#replication) バージョンのエンジン)分割、その他の機能で対応していないその他のエンジンです。

家族のエンジン:

-   [MergeTree](mergetree-family/mergetree.md#mergetree)
-   [ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂつｹ](mergetree-family/replacingmergetree.md#replacingmergetree)
-   [SummingMergeTree](mergetree-family/summingmergetree.md#summingmergetree)
-   [ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹ](mergetree-family/aggregatingmergetree.md#aggregatingmergetree)
-   [CollapsingMergeTree](mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree)
-   [VersionedCollapsingMergeTree](mergetree-family/versionedcollapsingmergetree.md#versionedcollapsingmergetree)
-   [グラフィットメールグツリー](mergetree-family/graphitemergetree.md#graphitemergetree)

### ログ {#log}

軽量 [エンジン](log-family/index.md) 最低の機能性を使って。 多くの小さなテーブル（約1万行まで）をすばやく作成し、後でそれらを全体として読み取る必要がある場合、これらは最も効果的です。

家族のエンジン:

-   [TinyLog](log-family/tinylog.md#tinylog)
-   [ストリップログ](log-family/stripelog.md#stripelog)
-   [ログ](log-family/log.md#log)

### 統合エンジン {#integration-engines}

エンジン用プリケーションデータストレージと処理システム。

家族のエンジン:

-   [カフカname](integrations/kafka.md#kafka)
-   [MySQL](integrations/mysql.md#mysql)
-   [ODBC](integrations/odbc.md#table-engine-odbc)
-   [JDBC](integrations/jdbc.md#table-engine-jdbc)
-   [HDFS](integrations/hdfs.md#hdfs)

### 特殊エンジン {#special-engines}

家族のエンジン:

-   [分散](special/distributed.md#distributed)
-   [MaterializedView](special/materializedview.md#materializedview)
-   [辞書](special/dictionary.md#dictionary)
-   [マージ](special/merge.md#merge
-   [ファイル](special/file.md#file)
-   [ヌル](special/null.md#null)
-   [セット](special/set.md#set)
-   [参加](special/join.md#join)
-   [URL](special/url.md#table_engines-url)
-   [ビュー](special/view.md#table_engines-view)
-   [メモリ](special/memory.md#memory)
-   [バッファ](special/buffer.md#buffer)

## 仮想列 {#table_engines-virtual_columns}

Virtual columnは、エンジンのソースコードで定義されているテーブルエンジンの属性です。

仮想列を指定しないでください。 `CREATE TABLE` クエリとあなたはそれらを見るこ `SHOW CREATE TABLE` と `DESCRIBE TABLE` クエリ結果。 仮想列も読み取り専用であるため、仮想列にデータを挿入することはできません。

仮想カラムからデータを選択するには、仮想カラムの名前を指定する必要があります。 `SELECT` クエリ。 `SELECT *` 仮想列から値を返しません。

テーブル仮想列のいずれかと同じ名前の列を持つテーブルを作成すると、仮想列にアクセスできなくなります。 これはお勧めしません。 競合を回避するために、通常、仮想列名にはアンダースコアが付加されます。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
