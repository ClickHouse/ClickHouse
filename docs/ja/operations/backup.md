---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 49
toc_title: "\u30C7\u30FC\u30BF\u30D0\u30C3\u30AF"
---

# データバック {#data-backup}

しばらく [複製](../engines/table-engines/mergetree-family/replication.md) provides protection from hardware failures, it does not protect against human errors: accidental deletion of data, deletion of the wrong table or a table on the wrong cluster, and software bugs that result in incorrect data processing or data corruption. In many cases mistakes like these will affect all replicas. ClickHouse has built-in safeguards to prevent some types of mistakes — for example, by default [50Gbを超えるデータを含むMergeTreeのようなエンジンでテーブルを削除することはできません](https://github.com/ClickHouse/ClickHouse/blob/v18.14.18-stable/programs/server/config.xml#L322-L330). しかし、これらの保障措置がカバーしないすべてのケースで回避.

人為的なミスを効果的に軽減するには、データのバックアップと復元に関する戦略を慎重に準備する必要があります **事前に**.

クリックハウスのバックアップとリストアのためのユニバーサルソリューションはありません。 gigabyteのデータで動作するものは、数十ペタバイトでは動作しません。 自分の長所と短所を持つ様々な可能なアプローチがありますが、これについては以下で説明します。 それは彼らの様々な欠点を補うために一つの代わりに、いくつかのアプローチを使用することをお勧めします。

!!! note "メモ"
    あなたが何かをバックアップし、それを復元しようとしたことがない場合、チャンスはあなたが実際にそれを必要とするときに正常に動作しない だから、どのようなバックアップアプローチを選択し、同様に復元プロセスを自動化することを確認し、定期的に予備のclickhouseクラスタでそれを練習。

## ソースデータの他の場所への複製 {#duplicating-source-data-somewhere-else}

多くの場合、clickhouseに取り込まれたデータは、次のような何らかの永続キューを介して配信されます [アパッチ-カフカ](https://kafka.apache.org). この場合、ClickHouseに書き込まれている間に同じデータストリームを読み込み、それをどこかのコールドストレージに保存する追加のサブスクライバセットを構 これは、オブジェクトストアまたは次のような分散ファイルシステムです [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

## ファイル {#filesystem-snapshots}

一部地域のファイルシステムをスナップショット機能(例えば, [ZFS](https://en.wikipedia.org/wiki/ZFS)ものではないのでいただける住ます。 可能な解決策は、この種のファイルシステムを使用して追加のレプリカを作成し、それらを [分散](../engines/table-engines/special/distributed.md) 使用されるテーブル `SELECT` クエリ。 スナップショットなどを複製するものでなければならないのクエリがデータを変更する. ボーナスパーツとして、これらのレプリカが特別なハードウェア構成によりディスクに付属のサーバ、コスト効果的です。

## クリックハウスコピー機 {#clickhouse-copier}

[クリックハウスコピー機](utilities/clickhouse-copier.md) 最初にペタバイトサイズのテーブルを再シャードするために作成された汎用性の高いツールです。 でも使用できるバックアップと復元を目的なので確実にコピーしたデータのClickHouseテーブルとクラスター

データの小さい容積のため、簡単の `INSERT INTO ... SELECT ...` リモートテーブルが作業しています。

## 部品による操作 {#manipulations-with-parts}

ClickHouseは使用を可能にします `ALTER TABLE ... FREEZE PARTITION ...` クエリをコピーのテーブル割. これは、ハードリンクを使用して実装されます `/var/lib/clickhouse/shadow/` フォルダで、通常は消費するエディスクスペースのための古いデータです。 作成されたファイルのコピーはClickHouseサーバーによって処理されないので、そこに残すことができます：追加の外部システムを必要としない単純なバックア このため、リモートで別の場所にコピーしてからローカルコピーを削除する方がよいでしょう。 分散ファイルシステムとオブジェクトストアはまだこのための良いオプションですが、十分な大きさの容量を持つ通常の添付ファイルサーバは、同 [rsync](https://en.wikipedia.org/wiki/Rsync)).

パーティション操作に関連するクエリの詳細については、 [変更文書](../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

第三者ツールを自動化するこのアプローチ: [clickhouse-バックアップ](https://github.com/AlexAkulov/clickhouse-backup).

[元の記事](https://clickhouse.tech/docs/en/operations/backup/) <!--hide-->
