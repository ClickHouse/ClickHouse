---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: "\u30C7\u30FC\u30BF\u30D0\u30C3\u30AF\u30A2"
---

# データバックア {#data-backup}

ながら [複製](../engines/table-engines/mergetree-family/replication.md) provides protection from hardware failures, it does not protect against human errors: accidental deletion of data, deletion of the wrong table or a table on the wrong cluster, and software bugs that result in incorrect data processing or data corruption. In many cases mistakes like these will affect all replicas. ClickHouse has built-in safeguards to prevent some types of mistakes — for example, by default [50Gbを超えるデータを含むMergeTreeのようなエンジンでは、テーブルを削除することはできません](server-configuration-parameters/settings.md#max-table-size-to-drop). しかし、これらの保障措置がカバーしないすべてのケースで回避.

ヒューマンエラーを効果的に軽減するには、データのバックアップと復元のための戦略を慎重に準備する必要があります **事前に**.

それぞれの会社には、利用可能なリソースとビジネス要件が異なるため、あらゆる状況に合ったClickHouseバックアップと復元のための普遍的なソリューショ 何がデータのギガバイトのために働く可能性が高い数十ペタバイトのために動作しません。 自分の長所と短所を持つ様々な可能なアプローチがありますが、これは以下で説明します。 で使うようにするといいでしょうくつかの方法でそれを補うためにさまの様々な問題があった。

!!! note "注"
    あなたが何かをバックアップし、それを復元しようとしたことがない場合、チャンスは、あなたが実際にそれを必要とするときに復元が正常に動作 したがって、どのようなバックアップアプローチを選択しても、復元プロセスも自動化し、予備のClickHouseクラスターで定期的に練習してください。

## ソースデータを他の場所に複製する {#duplicating-source-data-somewhere-else}

多くの場合、ClickHouseに取り込まれたデータは、次のような何らかの永続キューを介して配信されます [アパッチ-カフカ](https://kafka.apache.org). この場合、ClickHouseに書き込まれている間に同じデータストリームを読み取り、どこかのコールドストレージに保存する追加のサブスクライバーセットを構成するこ ほとんどの企業はすでにいくつかのデフォルト推奨コールドストレージを持っています。 [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

## ファイルシステ {#filesystem-snapshots}

一部地域のファイルシステムをスナップショット機能(例えば, [ZFS](https://en.wikipedia.org/wiki/ZFS)しかし、ライブクエリを提供するための最良の選択ではないかもしれません。 可能な解決策は、この種のファイルシステムで追加のレプリカを作成し、それらを [分散](../engines/table-engines/special/distributed.md) 以下の目的で使用されるテーブル `SELECT` クエリ。 スナップショットなどを複製するものでなければならないのクエリがデータを変更する. ボーナスパーツとして、これらのレプリカが特別なハードウェア構成によりディスクに付属のサーバ、コスト効果的です。

## クリックハウス-複写機 {#clickhouse-copier}

[クリックハウス-複写機](utilities/clickhouse-copier.md) ペタバイトサイズのテーブルを再シャードするために最初に作成された多目的な用具はある。 ClickHouseテーブルとクラスター間でデータを確実にコピーするため、バックアップと復元の目的でも使用できます。

データの小さなボリュームのために、単純な `INSERT INTO ... SELECT ...` リモートテーブルが作業しています。

## 部品による操作 {#manipulations-with-parts}

ClickHouseは、 `ALTER TABLE ... FREEZE PARTITION ...` クエリをコピーのテーブル割. これはハードリンクを使って実装される。 `/var/lib/clickhouse/shadow/` それは通常、古いデータのための余分なディスク領域を消費しません。 作成されたファイルのコピーはClickHouse serverによって処理されないので、そこに残すことができます：追加の外部システムを必要としない簡単なバックアップ このため、リモートで別の場所にコピーしてからローカルコピーを削除する方が良いでしょう。 分散ファイルシステムとオブジェクトストアはまだこのための良いオプションですが、十分な容量を持つ通常の添付ファイルサーバも同様に動作 [rsync](https://en.wikipedia.org/wiki/Rsync)).

パーティション操作に関連するクエリの詳細については、 [文書の変更](../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

第三者ツールを自動化するこのアプローチ: [clickhouse-バックアップ](https://github.com/AlexAkulov/clickhouse-backup).

[元の記事](https://clickhouse.tech/docs/en/operations/backup/) <!--hide-->
