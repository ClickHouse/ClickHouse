---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 31
toc_title: "\u30C7\u30FC\u30BF\u8907\u88FD"
---

# データ複製 {#table_engines-replication}

複製がサポートされる唯一のためのテーブルのMergeTree家族:

-   複製マージツリー
-   複製されたサミングマージツリー
-   レプリケートリプレースマージツリー
-   複製された集合マージツリー
-   レプリケートコラプシングマージツリー
-   ReplicatedVersionedCollapsingMergetree
-   レプリケートグラフィティマージツリー

複製の作品のレベルを個別のテーブルではなく、全体のサーバーです。 サーバーでの店舗も複製、非複製のテーブルでも同時に行います。

複製はシャーディングに依存しません。 各シャードには、独自の独立した複製があります。

圧縮されたデータ `INSERT` と `ALTER` クエリを複製(詳細については、ドキュメンテーションに [ALTER](../../../sql-reference/statements/alter.md#query_language_queries_alter)).

`CREATE`, `DROP`, `ATTACH`, `DETACH` と `RENAME` クエリは単一サーバーで実行され、レプリケートされません:

-   その `CREATE TABLE` クエリは、クエリが実行されるサーバー上に新しい複製可能テーブルを作成します。 このテーブルが既にあるその他のサーバーを加え新たなレプリカ.
-   その `DROP TABLE` クエリは、クエリが実行されるサーバー上にあるレプリカを削除します。
-   その `RENAME` queryは、レプリカのいずれかのテーブルの名前を変更します。 つまり、複製のテーブルでの異なる名称の異なるレプリカ.

ClickHouseの使用 [アパッチの飼育係](https://zookeeper.apache.org) レプリカのメタ情報を格納するため。 ZooKeeperバージョン3.4.5以降を使用します。

レプリケーションを使用するには、 [飼育係](../../../operations/server-configuration-parameters/settings.md#server-settings_zookeeper) サーバー構成セクション。

!!! attention "注意"
    セキュリティ設定を無視しないでください クリックハウスは `digest` [ACLスキーム](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) ZooKeeperセキュリティサブシステムの。

ZooKeeperクラスタのアドレスを設定する例:

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

既存のZooKeeperクラスタを指定すると、システムはそのクラスタ上のディレクトリを独自のデータとして使用します(複製可能なテーブルを作成するときに

場合飼育係な設定コンフィグファイルを創り上げられないんで再現しテーブル、および既存の複製のテーブル読み取り専用になります。

飼育係はで使用されていません `SELECT` レプリケーションのパフォーマン `SELECT` また、クエリは非レプリケートテーブルの場合と同様に高速に実行されます。 時の照会に配布再現し、テーブルClickHouse行動制御の設定 [max\_replica\_delay\_for\_distributed\_queries](../../../operations/settings/settings.md#settings-max_replica_delay_for_distributed_queries) と [フォールバック\_to\_stale\_replicas\_for\_distributed\_queries](../../../operations/settings/settings.md#settings-fallback_to_stale_replicas_for_distributed_queries).

それぞれのため `INSERT` クエリー、契約時に応募を追加飼育係を務取引等 （より正確には、これは挿入されたデータの各ブロックに対するものです。 `max_insert_block_size = 1048576` 行。)これは、 `INSERT` 非レプリケートテーブルと比較します。 しかし、推奨事項に従ってデータを複数のバッチで挿入する場合 `INSERT` 毎秒、それは問題を作成しません。 一つのZooKeeperクラスターを調整するために使用されるClickHouseクラスター全体の合計は数百です `INSERTs` 毎秒 データ挿入のスループット(秒あたりの行数)は、レプリケートされていないデータの場合と同じくらい高くなります。

非常に大きなクラスタの場合、異なるシャードに異なるZooKeeperクラスタを使用できます。 しかし、これはYandexの上で必要な証明されていません。メトリカクラスタ（約300台）。

複製は非同期でマルチマスターです。 `INSERT` クエリ(および `ALTER`）利用可能な任意のサーバに送信することができます。 データは、クエリが実行されるサーバーに挿入され、他のサーバーにコピーされます。 ですので非同期であるため、最近では挿入されデータが表示され、その他のレプリカとの待ち時間をゼロにすることに レプリカの一部が使用できない場合、データは使用可能になったときに書き込まれます。 レプリカが使用可能な場合、待機時間は、圧縮されたデータのブロックをネットワーク経由で転送するのにかかる時間です。

既定では、挿入クエリは、単一のレプリカからのデータの書き込みの確認を待機します。 データが得られない場合には成功した記述につのレプリカのサーバーのこのレプリカが消滅し、保存したデータは失われます。 複数のレプリカからのデータ書き込みの確認の取得を有効にするには、 `insert_quorum` オプション

データの各ブロックは原子的に書き込まれます。 挿入クエリは、次のブロックに分割されます `max_insert_block_size = 1048576` 行。 言い換えれば、 `INSERT` クエリには1048576行未満があり、アトミックに作成されます。

データブロックは重複排除されます。 同じデータブロック（同じ順序で同じ行を含む同じサイズのデータブロック）の複数の書き込みの場合、ブロックは一度だけ書き込まれます。 この理由は、クライアントアプリケーションがデータがDBに書き込まれたかどうかを知らないときにネットワーク障害が発生した場合です。 `INSERT` クエリーするだけで簡単に繰り返します。 どのレプリカ挿入が同一のデータで送信されたかは関係ありません。 `INSERTs` 冪等である。 重複排除圧縮パラメータの制御 [merge\_tree](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-merge_tree) サーバー設定。

複製時に、元のデータの挿入には転送されます。 さらなるデータ変換(マージ)は、すべてのレプリカで同じ方法で調整され、実行されます。 つまり、レプリカが異なるデータセンターに存在する場合、レプリケーションは適切に機能します。 レプリケーションの主な目的は、異なるデータセンターでデータを複製することです。)

同じデータの任意の数のレプリカを持つことができます。 Yandex.Metricaは本番環境で二重複製を使用します。 各サーバーはRAID-5またはRAID-6を使用し、場合によってはRAID-10を使用します。 これは比較的信頼性が高く便利な解決策です。

システムは、レプリカのデータ同期を監視し、障害発生後に回復することができます。 フェールオーバーは、自動(データのわずかな違いの場合)または半自動(データの異なりが多すぎる場合、構成エラーを示す可能性があります)です。

## 複製テーブルの作成 {#creating-replicated-tables}

その `Replicated` テーブルエンジン名に接頭辞が追加されます。 例えば:`ReplicatedMergeTree`.

**複製\*MergeTreeパラメータ**

-   `zoo_path` — The path to the table in ZooKeeper.
-   `replica_name` — The replica name in ZooKeeper.

例:

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

<summary>非推奨の構文の例</summary>

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)
```

</details>

その例としては、これらのパラメータを含むことができ換巻きていただけるボディーです。 置き換えられた値は、 ‘macros’ 設定ファイルのセクション。 例:

``` xml
<macros>
    <layer>05</layer>
    <shard>02</shard>
    <replica>example05-02-1.yandex.ru</replica>
</macros>
```

の表の飼育係るべきで機能していませんが将来的には再現します。 テーブルの異なる資料は異なる。
この場合、パスは次の部分で構成されます:

`/clickhouse/tables/` 共通の接頭辞です。 の使用をお勧めしまうことです。

`{layer}-{shard}` シャード識別子です。 この例では、Yandexので、二つの部分で構成されています。Metricaクラスターの使用インターネット上のファイル転送sharding. ほとんどのタスクでは、{shard}置換だけを残すことができます。

`table_name` ZooKeeper内のテーブルのノード名です。 テーブル名と同じにすることをお勧めします。 これは、テーブル名とは対照的に、名前変更クエリの後に変更されないため、明示的に定義されています。
*HINT*：データベース名を追加することができます `table_name` 同様に。 例えば `db_name.table_name`

のレプリカの名前を識別のレプリカと同じ。 これには、例のようにサーバー名を使用できます。 名前は、各シャード内で一意である必要があります。

置換を使用する代わりに、パラメーターを明示的に定義できます。 これは、テストや小さなクラスターの構成に便利です。 ただし、分散DDLクエリは使用できません (`ON CLUSTER`）この場合。

大規模なクラスターで作業する場合は、エラーの可能性を減らすため、置換を使用することをお勧めします。

実行 `CREATE TABLE` 各レプリカのクエリ。 このクエリを複製テーブルを追加し、新たなレプリカは、既存します。

テーブルに他のレプリカのデータがすでに含まれている後に新しいレプリカを追加すると、クエリの実行後にデータが他のレプリカから新しいレプリカ つまり、新しいレプリカは他のレプリカと同期します。

レプリカを削除するには、 `DROP TABLE`. However, only one replica is deleted – the one that resides on the server where you run the query.

## 障害後の回復 {#recovery-after-failures}

場合飼育係が不可の場合、サーバは、複製のテーブルスイッチ読み取り専用モードになります。 システムは定期的にZooKeeperへの接続を試みます。

飼育係が中に使用できない場合 `INSERT`、またはZooKeeperとの対話時にエラーが発生すると、例外がスローされます。

ZooKeeperに接続すると、ローカルファイルシステム内のデータセットが予想されるデータセットと一致するかどうかがチェックされます(ZooKeeperはこの情報を格納し がある場合は軽微な不整合の解消による同期データのレプリカ.

システムが壊れたデータ部分(ファイルのサイズが間違っている)または認識できない部分(ファイルシステムに書き込まれているが、ZooKeeperに記録されて `detached` サブディレクトリ(削除されません)。 他の部分がコピーからのレプリカ.

ClickHouseは、大量のデータを自動的に削除するなどの破壊的な操作は実行しません。

サーバが起動時に表示されます(または新たに設立し、セッションとの飼育係）でのみチェックの量やサイズのすべてのファイルです。 ファイルサイズは一致するが、途中でバイトが変更された場合、これはすぐには検出されず、データを読み取ろうとしたときにのみ検出されます。 `SELECT` クエリ。 クエリが例外をスローしつつ、非マッチングのチェックサムはサイズに圧縮されたブロックです。 この場合、データパーツは検証キューに追加され、必要に応じてレプリカからコピーされます。

場合には地元のデータセットが異なりすぎとられ、安全機構を起動します。 サーバーはこれをログに入力し、起動を拒否します。 この理由は、シャード上のレプリカが別のシャード上のレプリカのように誤って構成された場合など、このケースが構成エラーを示す可能性があるためです。 しかし、しきい値をこの機構の設定かなり低く、こうした状況が起こる中で、失敗を回復しました。 この場合、データは半自動的に復元されます。 “pushing a button”.

回復を開始するには、ノードを作成します `/path_to_table/replica_name/flags/force_restore_data` で飼育係とコンテンツ、またはコマンドを実行し復元すべての複製のテーブル:

``` bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

次に、サーバーを再起動します。 起動時に、サーバーはこれらのフラグを削除し、回復を開始します。

## 完全なデータ損失の後の回復 {#recovery-after-complete-data-loss}

すべてのデータやメタデータ消えたらサーバには、次の手順に従ってください復興:

1.  ClickHouseをサーバにインストールします。 シャード識別子とレプリカを使用する場合は、設定ファイルで置換を正しく定義します。
2.  サーバー上で手動で複製する必要がある未複製のテーブルがある場合は、レプリカからデータをコピーします(ディレクトリ内)。 `/var/lib/clickhouse/data/db_name/table_name/`).
3.  テーブル定義のコピー `/var/lib/clickhouse/metadata/` レプリカから。 シャード識別子またはレプリカ識別子がテーブル定義で明示的に定義されている場合は、このレプリカに対応するように修正します。 (または、サーバーを起動し、すべての `ATTACH TABLE` にあったはずのクエリ.sqlファイル `/var/lib/clickhouse/metadata/`.)
4.  回復を開始するには、ZooKeeperノードを作成します `/path_to_table/replica_name/flags/force_restore_data` コマンドを実行してレプリケートされたテーブルをすべて復元します: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

次に、サーバーを起動します（すでに実行されている場合は再起動します）。 データダウンロードからのレプリカ.

代替の回復オプションは削除に関する情報は失われたレプリカから飼育係 (`/path_to_table/replica_name`)で説明したように、レプリカを再度作成します “[複製テーブルの作成](#creating-replicated-tables)”.

復旧時のネットワーク帯域幅に制限はありません。 一度に多数のレプリカを復元する場合は、この点に注意してください。

## MergeTreeからReplicatedMergeTreeへの変換 {#converting-from-mergetree-to-replicatedmergetree}

我々はこの用語を使用する `MergeTree` すべてのテーブルエンジンを参照するには `MergeTree family` の場合と同じです。 `ReplicatedMergeTree`.

あなたが持っていた場合 `MergeTree` 手動で複製されたテーブルは、複製されたテーブルに変換することができます。 すでに大量のデータを収集している場合は、これを行う必要があるかもしれません。 `MergeTree` レプリケーションを有効にします。

さまざまなレプリカでデータが異なる場合は、最初に同期するか、このデータを除くすべてのレプリカで削除します。

既存のMergeTreeテーブルの名前を変更し、次に `ReplicatedMergeTree` 古い名前のテーブル。
データを古いテーブルから `detached` サブディレクトリ内のディレクトリを新しいテーブルデータ (`/var/lib/clickhouse/data/db_name/table_name/`).
その後、実行 `ALTER TABLE ATTACH PARTITION` これらのデータパーツを作業セットに追加するレプリカのいずれかで。

## ReplicatedMergeTreeからMergeTreeへの変換 {#converting-from-replicatedmergetree-to-mergetree}

別の名前のMergeTreeテーブルを作成します。 ディレクトリからすべてのデータを移動します。 `ReplicatedMergeTree` テーブルデータを新しいテーブルのデータディレクトリです。 その後、削除 `ReplicatedMergeTree` サーバーを表にして再起動します。

あなたが取り除きたい場合は、 `ReplicatedMergeTree` サーバーを起動しないテーブル:

-   対応するものを削除する `.sql` ファイルのメタデータディレクトリ (`/var/lib/clickhouse/metadata/`).
-   ZooKeeperで対応するパスを削除します (`/path_to_table/replica_name`).

この後、サーバーを起動し、 `MergeTree` データをそのディレクトリに移動し、サーバーを再起動します。

## Zookeeperクラスター内のメタデータが紛失または破損した場合の復旧 {#recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged}

ZooKeeperのデータが紛失または破損している場合は、上記のように再生されていないテーブルに移動することでデータを保存できます。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/replication/) <!--hide-->
