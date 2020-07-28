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

高負荷仕事のための最も普遍的な、機能テーブルエンジン。 本物件の共有によるこれらのエンジンには迅速にデータを挿入とその後のバックグラウンドデータを処となります。 `MergeTree` 家族のエンジンの支援データレプリケーション（ [複製された\*](mergetree_family/replication.md) バージョンのエンジン)分割、その他の機能で対応していないその他のエンジンです。

家族のエンジン:

-   [MergeTree](mergetree_family/mergetree.md)
-   [ﾂつｨﾂ姪"ﾂつ"ﾂ債ﾂつｹ](mergetree_family/replacingmergetree.md)
-   [SummingMergeTree](mergetree_family/summingmergetree.md)
-   [ﾂつｨﾂ姪"ﾂつ"ﾂ債ﾂづｭﾂつｹ](mergetree_family/aggregatingmergetree.md)
-   [CollapsingMergeTree](mergetree_family/collapsingmergetree.md)
-   [VersionedCollapsingMergeTree](mergetree_family/versionedcollapsingmergetree.md)
-   [グラフィットメールグツリー](mergetree_family/graphitemergetree.md)

### ログ {#log}

軽量 [エンジン](log_family/index.md) 最低の機能性を使って。 多くの小さなテーブル（約1万行まで）をすばやく作成し、後でそれらを全体として読み取る必要がある場合、これらは最も効果的です。

家族のエンジン:

-   [TinyLog](log_family/tinylog.md)
-   [ストリップログ](log_family/stripelog.md)
-   [ログ](log_family/log.md)

### 統合エンジン {#integration-engines}

エンジン用プリケーションデータストレージと処理システム。

家族のエンジン:

-   [カフカname](integrations/kafka.md)
-   [MySQL](integrations/mysql.md)
-   [ODBC](integrations/odbc.md)
-   [JDBC](integrations/jdbc.md)
-   [HDFS](integrations/hdfs.md)

### 特殊エンジン {#special-engines}

家族のエンジン:

-   [分散](special/distributed.md)
-   [MaterializedView](special/materializedview.md)
-   [辞書](special/dictionary.md)
-   [マージ](special/merge.md)
-   [ファイル](special/file.md)
-   [ヌル](special/null.md)
-   [セット](special/set.md)
-   [参加](special/join.md)
-   [URL](special/url.md)
-   [ビュー](special/view.md)
-   [メモリ](special/memory.md)
-   [バッファ](special/buffer.md)

## 仮想列 {#table_engines-virtual-columns}

Virtual columnは、エンジンのソースコードで定義されているテーブルエンジンの属性です。

仮想列を指定しないでください。 `CREATE TABLE` クエリとあなたはそれらを見るこ `SHOW CREATE TABLE` と `DESCRIBE TABLE` クエリ結果。 仮想列も読み取り専用であるため、仮想列にデータを挿入することはできません。

仮想カラムからデータを選択するには、仮想カラムの名前を指定する必要があります。 `SELECT` クエリ。 `SELECT *` 仮想列から値を返しません。

テーブル仮想列のいずれかと同じ名前の列を持つテーブルを作成すると、仮想列にアクセスできなくなります。 これはお勧めしません。 競合を回避するために、通常、仮想列名にはアンダースコアが付加されます。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
