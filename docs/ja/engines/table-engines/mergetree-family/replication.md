---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 31
toc_title: "\u30C7\u30FC\u30BF\u8907\u88FD"
---

# データ複製 {#table_engines-replication}

複製がサポートされる唯一のためのテーブルのmergetree家族:

-   レプリケートされたmergetree
-   ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂ-faq
-   ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂ-faq
-   ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂ-faq
-   レプリケートされたcollapsingmergetree
-   ReplicatedVersionedCollapsingMergetree
-   ReplicatedGraphiteMergeTree

複製は、サーバー全体ではなく、個々のテーブルのレベルで機能します。 サーバーでの店舗も複製、非複製のテーブルでも同時に行います。

複製はシャーディングに依存しません。 各シャードには独自の独立した複製があります。

圧縮データのための `INSERT` と `ALTER` クエリを複製(詳細については、ドキュメンテーションに [ALTER](../../../sql-reference/statements/alter.md#query_language_queries_alter)).

`CREATE`, `DROP`, `ATTACH`, `DETACH` と `RENAME` クエリは単一サーバーで実行され、レプリケートされません:

-   その `CREATE TABLE` queryは、クエリが実行されるサーバー上に新しい複製可能テーブルを作成します。 このテーブルが既にあるその他のサーバーを加え新たなレプリカ.
-   その `DROP TABLE` クエリは、クエリが実行されているサーバー上のレプリカを削除します。
-   その `RENAME` queryは、いずれかのレプリカでテーブルの名前を変更します。 つまり、複製のテーブルでの異なる名称の異なるレプリカ.

ClickHouse用 [アパッチの飼育係](https://zookeeper.apache.org) レプリカのメタ情報を格納するため。 使用ZooKeeperバージョン3.4.5以降。

レプリケーションを使用するには、 [zookeeper](../../../operations/server-configuration-parameters/settings.md#server-settings_zookeeper) サーバー構成セクション.

!!! attention "注意"
    セキュリ クリックハウスは `digest` [ACLスキーム](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) ZooKeeperのセキュリティサブシステムの

ZooKeeperクラスタのアドレス設定例:

``` xml
<zookeeper>
    <node index="1">
        <host>example1</host>
        <port>2181</port>
    </node>
    <node index="2">
        <host>example2</host>
        <port>2181</port>
    </node>
    <node index="3">
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

既存のzookeeperクラスターを指定すると、システムは独自のデータ用のディレクトリを使用します（レプリケート可能なテーブルを作成するときにディレクトリ

ZooKeeperが設定ファイルに設定されていない場合は、複製されたテーブルを作成することはできず、既存の複製されたテーブルは読み取り専用になります。

ZooKeeperは使用されません `SELECT` レプリケーショ `SELECT` との質問を行ってい非再現します。 分散レプリケートテーブルを照会する場合、ClickHouseの動作は設定によって制御されます [max\_replica\_delay\_for\_distributed\_queries](../../../operations/settings/settings.md#settings-max_replica_delay_for_distributed_queries) と [fallback\_to\_stale\_replicas\_for\_distributed\_queries](../../../operations/settings/settings.md#settings-fallback_to_stale_replicas_for_distributed_queries).

それぞれの `INSERT` クエリー、契約時に応募を追加飼育係を務取引等 (より正確には、これはデータの挿入された各ブロックに対するものです。 `max_insert_block_size = 1048576` 行。）これは、 `INSERT` と比較して再現します。 しかし、推奨事項に従ってデータを複数のバッチで挿入する場合 `INSERT` 毎秒、それは問題を作成しない。 全体のClickHouseクラスターの使用のための調整一飼育係のクラスタでは、合計数百 `INSERTs` 秒あたり。 データ挿入のスループット(秒あたりの行数)は、レプリケートされていないデータと同じくらい高くなります。

のための非常に大きなクラスターで異なるクラスター飼育係の異なる破片. しかし、これはyandexで必要なことは証明されていません。metricaクラスター(約300台のサーバー)。

複製は非同期、マルチます。 `INSERT` クエリ（と同様 `ALTER`）利用可能な任意のサーバーに送信することができます。 クエリが実行されているサーバーにデータが挿入され、そのデータが他のサーバーにコピーされます。 非同期であるため、最近挿入されたデータが他のレプリカに何らかの遅延で表示されます。 レプリカの一部が使用できない場合、データは使用できるようになった時点で書き込まれます。 レプリカが使用可能な場合、待機時間は、圧縮されたデータのブロックをネットワーク経由で転送するのにかかる時間です。

既定では、挿入クエリは、単一のレプリカからのデータの書き込みの確認を待機します。 データが正常に単一のレプリカに書き込まれ、このレプリカを持つサーバーが存在しなくなると、格納されたデータは失われます。 複数のレプリカからデー `insert_quorum` オプション。

データの各ブロックは原子的に書き込まれます。 挿入クエリは、以下のブロックに分割されます `max_insert_block_size = 1048576` 行。 言い換えれば、 `INSERT` クエリは、それが原子的に作られ、1048576未満の行を持っています。

データブロックは重複除外されます。 同じデータブロック（同じ順序で同じ行を含む同じサイズのデータブロック）の複数書き込みの場合、ブロックは一度だけ書き込まれます。 この理由は、クライアントアプリケーションがデータがdbに書き込まれたかどうかを知らない場合のネットワーク障害の場合です。 `INSERT` クエリーするだけで簡単に繰り返します。 どのレプリカ挿入が同じデータで送信されたかは関係ありません。 `INSERTs` べき等である。 重複排除圧縮パラメータの制御 [merge\_tree](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-merge_tree) サーバー設定。

レプリケーショ さらなるデータ変換（マージ）は、すべてのレプリカで同じ方法で調整され、実行されます。 これにより、ネットワークの使用を最小限に抑えることができます。 (複製の主な目的は、異なるデータセンター内のデータを複製することです。)

同じデータの任意の数のレプリカを持つことができます。 yandexの。metricaは、本番環境で二重の複製を使用します。 各サーバーはraid-5またはraid-6を使用し、場合によってはraid-10を使用します。 これは比較的信頼性が高く便利な解決策です。

システムは、レプリカ上のデータ同期性を監視し、障害発生後に回復することができます。 フェールオーバーは、自動(データのわずかな差異の場合)または半自動(データが大きく異なる場合、構成エラーを示す可能性があります)です。

## 複製テーブルの作成 {#creating-replicated-tables}

その `Replicated` テーブルエンジン名に接頭辞が追加されます。 例えば:`ReplicatedMergeTree`.

**複製\*マージツリーパラメータ**

-   `zoo_path` — The path to the table in ZooKeeper.
-   `replica_name` — The replica name in ZooKeeper.

例えば:

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}')
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
```

<details markdown="1">

<summary>非推奨構文の例</summary>

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)
```

</details>

その例としては、これらのパラメータを含むことができ換巻きていただけるボディーです。 置換された価値はから取られます ‘macros’ 設定ファイルのセクション。 例えば:

``` xml
<macros>
    <layer>05</layer>
    <shard>02</shard>
    <replica>example05-02-1.yandex.ru</replica>
</macros>
```

の表の飼育係るべきで機能していませんが将来的には再現します。 テーブルの異なる資料は異なる。
この場合、パスは次の部分で構成されます:

`/clickhouse/tables/` は共通の接頭辞です。 の使用をお勧めしまうことです。

`{layer}-{shard}` シャード識別子です。 この例では、Yandexので、二つの部分で構成されています。Metricaクラスターの使用インターネット上のファイル転送sharding. ほとんどのタスクでは、{shard}置換だけを残すことができます。

`table_name` ZooKeeperのテーブルのノードの名前です。 テーブル名と同じにすることをお勧めします。 テーブル名とは対照的に、名前の変更クエリの後に変更されないため、明示的に定義されています。
*HINT*：データベース名を追加することができます `table_name` 同様に。 例えば `db_name.table_name`

レプリカ名は同じテーブルの別のレプリカを識別します。 この例のように、このサーバー名を使用できます。 名前は各シャード内で一意である必要があります。

置換を使用する代わりに、パラメーターを明示的に定義できます。 これは、テストや小さなクラスターの構成に便利です。 ただし、分散ddlクエリは使用できません (`ON CLUSTER`)この場合。

組み合わせによる方がクラスターの使用をお勧めいたしま換その可能性を低減するにはエラーになります。

実行する `CREATE TABLE` 各レプリカに対するクエリ。 このクエ

テーブルに他のレプリカのデータがすでに含まれている後に新しいレプリカを追加すると、データはクエリの実行後に他のレプリカから新しいレプリ つまり、新しいレプリカは他のレプリカと同期します。

レプリカを削除するには `DROP TABLE`. However, only one replica is deleted – the one that resides on the server where you run the query.

## 障害後の復旧 {#recovery-after-failures}

場合飼育係が不可の場合、サーバは、複製のテーブルスイッチ読み取り専用モードになります。 システムは定期的にzookeeperに接続しようとします。

ZooKeeperが使用中に利用できない場合 `INSERT`、またはZooKeeperとやり取りするとエラーが発生し、例外がスローされます。

ZooKeeperに接続した後、システムはローカルファイルシステムのデータセットが期待されるデータセットと一致するかどうかをチェックします（ZooKeeperはこの情報 小さな不整合がある場合、システムはデータをレプリカと同期することで解決します。

システムが壊れたデータ部分（ファイルのサイズが間違っている）または認識されない部分（ファイルシステムに書き込まれたがzookeeperに記録されていな `detached` サブディレクトリ(削除されません)。 他の部分がコピーからのレプリカ.

ClickHouseは大量のデータを自動的に削除するなどの破壊的な操作を実行しません。

サーバーが起動（またはzookeeperとの新しいセッションを確立）すると、すべてのファイルの量とサイズのみをチェックします。 ファイルサイズが一致しているが、バイトが途中で変更されている場合、これはすぐには検出されません。 `SELECT` クエリ。 クエリは、一致しないチェックサムまたは圧縮ブロックのサイズに関する例外をスローします。 この場合、データパーツは検証キューに追加され、必要に応じてレプリカからコピーされます。

データのローカルセットが予想されるセットと大きく異なる場合は、安全機構がトリガーされます。 サーバーはこれをログに入力し、起動を拒否します。 この理由は、シャード上のレプリカが別のシャード上のレプリカのように誤って構成された場合など、このケースが構成エラーを示している可能性がある しかし、しきい値をこの機構の設定かなり低く、こうした状況が起こる中で、失敗を回復しました。 この場合、データは半自動的に復元されます。 “pushing a button”.

回復を開始するには、ノードを作成します `/path_to_table/replica_name/flags/force_restore_data` で飼育係とコンテンツ、またはコマンドを実行し復元すべての複製のテーブル:

``` bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

次に、サーバーを再起動します。 開始時に、サーバーはこれらのフラグを削除し、回復を開始します。

## 完全なデータの損失後の回復 {#recovery-after-complete-data-loss}

すべてのデータやメタデータ消えたらサーバには、次の手順に従ってください復興:

1.  サーバーにclickhouseをインストール. シャード識別子とレプリカを含むコンフィグファイルで置換を正しく定義します。
2.  サーバー上で手動で複製する必要のある複雑でないテーブルがある場合は、ディレクトリ内のレプリカからデータをコピーします `/var/lib/clickhouse/data/db_name/table_name/`).
3.  にあるテーブル定義のコピー `/var/lib/clickhouse/metadata/` レプリカから。 テーブル定義でシャードまたはレプリカ識別子が明示的に定義されている場合は、このレプリカに対応するように修正します。 （あるいは、サーバーを起動してすべての `ATTACH TABLE` にあったはずのクエリ。のsqlファイル `/var/lib/clickhouse/metadata/`.)
4.  回復を開始するには、zookeeperノードを作成します `/path_to_table/replica_name/flags/force_restore_data` 他のコンテンツ、またはコマンドを実行し復元すべての複製のテーブル: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

その後、サーバーを起動します（既に実行されている場合は再起動します）。 デー

代替の回復オプションは削除に関する情報は失われたレプリカから飼育係 (`/path_to_table/replica_name`)、レプリカを再度作成します。 “[複製テーブルの作成](#creating-replicated-tables)”.

リカバリ中のネットワーク帯域幅に制限はありません。 一度に多くのレプリカを復元する場合は、この点に留意してください。

## MergetreeからReplicatedmergetreeへの変換 {#converting-from-mergetree-to-replicatedmergetree}

我々はこの用語を使用する： `MergeTree` のすべてのテーブルエンジンを参照するには `MergeTree family`、の場合と同じ `ReplicatedMergeTree`.

あなたが持っていた場合 `MergeTree` したテーブルを手動で再現でき換で再現します。 すでに大量のデータを収集している場合は、これを行う必要があります `MergeTree` これで、レプリケーションを有効にします。

さまざまなレプリカでデータが異なる場合は、最初に同期するか、レプリカ以外のすべてのデータを削除します。

既存のmergetreeテーブルの名前を変更し、 `ReplicatedMergeTree` 古い名前のテーブル。
古いテーブルからデータを移動する `detached` サブディレクトリ内のディレクトリを新しいテーブルデータ (`/var/lib/clickhouse/data/db_name/table_name/`).
その後、実行 `ALTER TABLE ATTACH PARTITION` 作業セットにこれらのデータ部分を追加するレプリカのいずれか。

## ReplicatedmergetreeからMergetreeへの変換 {#converting-from-replicatedmergetree-to-mergetree}

別の名前のmergetreeテーブルを作成します。 ディレクトリからすべてのデータを `ReplicatedMergeTree` テーブルデータを新しいテーブルのデータディレクトリです。 次に、 `ReplicatedMergeTree` テーブルとサーバーを再起動します。

あなたが取り除きたいなら `ReplicatedMergeTree` サーバーを起動せずにテーブル:

-   対応するものを削除する `.sql` メタデータディレク (`/var/lib/clickhouse/metadata/`).
-   ZooKeeperの対応するパスを削除します (`/path_to_table/replica_name`).

この後、サーバーを起動し、 `MergeTree` テーブル、そのディレクトリにデータを移動し、サーバーを再起動します。

## Zookeeperクラスター内のメタデータが失われたり破損した場合の回復 {#recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged}

ZooKeeper内のデータが失われたり破損したりした場合は、上記のように単純なテーブルに移動してデータを保存することができます。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/replication/) <!--hide-->
