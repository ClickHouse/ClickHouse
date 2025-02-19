---
slug: /ja/engines/table-engines/mergetree-family/replication
sidebar_position: 20
sidebar_label: Data Replication
---

# データレプリケーション

:::note
ClickHouse Cloudでは、レプリケーションは自動で管理されます。テーブルを作成する際、引数を追加せずに行ってください。例えば、以下の例では次のように置き換えてください：

```sql
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/table_name',
    '{replica}',
    ver
)
```

次のように：

```sql
ENGINE = ReplicatedMergeTree
```
:::

レプリケーションは、MergeTreeファミリーのテーブルでのみサポートされています：

- ReplicatedMergeTree
- ReplicatedSummingMergeTree
- ReplicatedReplacingMergeTree
- ReplicatedAggregatingMergeTree
- ReplicatedCollapsingMergeTree
- ReplicatedVersionedCollapsingMergeTree
- ReplicatedGraphiteMergeTree

レプリケーションは、サーバ全体ではなく、個別のテーブルレベルで動作します。1つのサーバは、レプリケートテーブルと非レプリケートテーブルの両方を同時に保存できます。

レプリケーションはシャードには依存しません。各シャードは独自のレプリケーションを持っています。

`INSERT` や `ALTER` クエリの圧縮データはレプリケートされます（詳細は、[ALTER](/docs/ja/sql-reference/statements/alter/index.md#query_language_queries_alter)のドキュメントを参照してください）。

`CREATE`、`DROP`、`ATTACH`、`DETACH`、`RENAME` クエリは単一のサーバで実行され、レプリケートされません：

- `CREATE TABLE` クエリは、クエリを実行するサーバ上に新しいレプリケート可能なテーブルを作成します。このテーブルが他のサーバ上に既に存在する場合、新しいレプリカを追加します。
- `DROP TABLE` クエリは、クエリを実行するサーバ上にあるレプリカを削除します。
- `RENAME` クエリは、レプリカの1つ上でテーブル名を変更します。言い換えれば、レプリケートテーブルは異なるレプリカに異なる名前を持つことができます。

ClickHouseは、レプリカのメタ情報を保存するために [ClickHouse Keeper](/docs/ja/guides/sre/keeper/index.md) を使用します。ZooKeeperバージョン3.4.5以降の利用も可能ですが、ClickHouse Keeperの使用が推奨されます。

レプリケーションを使用するには、[zookeeper](/docs/ja/operations/server-configuration-parameters/settings.md/#server-settings_zookeeper)のサーバ設定セクションでパラメータを設定してください。

:::note
セキュリティ設定を軽視しないでください。ClickHouseはZooKeeperセキュリティサブシステムの `digest` [ACLスキーム](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl)をサポートしています。
:::

ClickHouse Keeperクラスタのアドレス設定例：

``` xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <node>
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

ClickHouseはまた、補助ZooKeeperクラスタにレプリカのメタ情報を保存することをサポートしています。エンジンの引数としてZooKeeperクラスタ名とパスを提供することでこれを行います。言い換えれば、異なるテーブルのメタデータを異なるZooKeeperクラスタに保存することをサポートしています。

補助ZooKeeperクラスタのアドレス設定例：

``` xml
<auxiliary_zookeepers>
    <zookeeper2>
        <node>
            <host>example_2_1</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_2</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_3</host>
            <port>2181</port>
        </node>
    </zookeeper2>
    <zookeeper3>
        <node>
            <host>example_3_1</host>
            <port>2181</port>
        </node>
    </zookeeper3>
</auxiliary_zookeepers>
```

デフォルトのZooKeeperクラスタの代わりに補助ZooKeeperクラスタにテーブルメタデータを保存するには、次のように SQL を使って ReplicatedMergeTree エンジンでテーブルを作成することができます：

```sql
CREATE TABLE table_name ( ... ) ENGINE = ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'replica_name') ...
```
既存のZooKeeperクラスタを指定することで、システムはそこで自分のデータ用のディレクトリを使います（レプリケート可能なテーブルを作成する際にディレクトリが指定されます）。

ZooKeeperが設定ファイルに設定されていない場合、レプリケートテーブルを作成することはできず、既存のレプリケートテーブルは読み取り専用になります。

`SELECT` クエリではZooKeeperは使用されません。レプリケーションは `SELECT` のパフォーマンスに影響を与えず、非レプリケーションテーブルと同じ速さでクエリが実行されます。分散レプリケートテーブルをクエリする際、ClickHouseの動作は[max_replica_delay_for_distributed_queries](/docs/ja/operations/settings/settings.md/#max_replica_delay_for_distributed_queries)および[fallback_to_stale_replicas_for_distributed_queries](/docs/ja/operations/settings/settings.md/#fallback_to_stale_replicas_for_distributed_queries)の設定で制御されます。

各 `INSERT` クエリごとに、およそ10件のエントリが複数のトランザクションを介してZooKeeperに追加されます。（より正確にはこれは、データの挿入された各ブロックごとです。`INSERT` クエリは一つのブロック、または `max_insert_block_size = 1048576` 行ごとに一つのブロックを含みます。）これは、非レプリケートテーブルと比較して `INSERT` の遅延をわずかに長くします。ただし、1秒あたり1つの `INSERT` を超えないバッチでデータを挿入するという推奨事項に従うと問題はありません。1つのZooKeeperクラスタで調整されるClickHouseクラスタ全体で、1秒あたり数百の `INSERT` が行われます。データ挿入のスループット（1秒あたりの行数）は、非レプリケートデータの場合と同じくらい高いです。

非常に大きなクラスタの場合、異なるシャードで異なるZooKeeperクラスタを使用することができます。しかし、これまでの経験上、これは約300サーバの本番クラスタで必要であるとは証明されていません。

レプリケーションは非同期でマルチマスターです。`INSERT` クエリ（および `ALTER`）は利用可能な任意のサーバに送信できます。データはクエリが実行されたサーバに挿入され、次に他のサーバにコピーされます。非同期であるため、新たに挿入されたデータが他のレプリカに表示されるにはいくらかの遅延があります。一部のレプリカが利用不可の場合、データは利用可能になった際に書き込まれます。レプリカが利用可能な場合、遅延は圧縮されたデータブロックをネットワーク越しに転送するのにかかる時間です。レプリケートテーブルにおいてバックグラウンドタスクを実行するためのスレッド数は、[background_schedule_pool_size](/docs/ja/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size)設定で設定できます。

`ReplicatedMergeTree` エンジンは、レプリケートフェッチ用に別のスレッドプールを使用します。プールのサイズはサーバを再起動して調整可能な[background_fetches_pool_size](/docs/ja/operations/settings/settings.md/#background_fetches_pool_size)設定によって制限されます。

デフォルトでは、`INSERT` クエリは1つのレプリカからのデータ書き込み確認を待ちます。もしデータが1つのレプリカにのみ正常に書き込まれ、そのレプリカのあるサーバが消失した場合、保存されたデータは失われます。複数のレプリカからのデータ書き込み確認を得るには、`insert_quorum` オプションを使用します。

各データブロックは原子的に書き込まれます。`INSERT` クエリは`max_insert_block_size = 1048576` 行までのブロックに分けられます。言い換えれば、`INSERT` クエリが1048576行未満の場合、それは原子的に行われます。

データブロックは重複削除されます。同じデータブロックの複数回の書き込み（同じ行が同じ順序で含まれる同じサイズのデータブロック）では、ブロックは1回だけ書き込まれます。これは、ネットワーク障害が発生した場合にクライアントアプリケーションがデータがDBに書き込まれたかどうか不明なときに、`INSERT` クエリを単に再実行できるようにするためです。同じデータの `INSERT` はどのレプリカに送信されても問題ありません。`INSERT` は冪等です。重複削除のパラメータは[merge_tree](/docs/ja/operations/server-configuration-parameters/settings.md/#merge_tree)サーバ設定で制御されます。

レプリケーション中には挿入する元データのみがネットワーク越しに転送されます。データのさらなる変換（マージ）は、すべてのレプリカで同様に調整され、実行されます。これによりネットワーク使用が最小限に抑えられ、レプリカが異なるデータセンターに存在する場合でもレプリケーションがうまく機能します。（異なるデータセンターでのデータの複製がレプリケーションの主な目的であることに注意してください。）

同じデータのレプリカを任意の数だけ持つことができます。我々の経験上では、比較的信頼性があり便利な解決策は、各サーバにRAID-5またはRAID-6（場合によってはRAID-10）を使用して、本番環境で二重レプリケーションを使用することです。

システムはレプリカ上のデータの同期性を監視し、障害後のリカバリが可能です。フェイルオーバーは自動（データの小さな差の場合）または半自動（データが非常に異なる場合、これは設定エラーを示す可能性があります）です。

## レプリケートテーブルの作成 {#creating-replicated-tables}

:::note
ClickHouse Cloudでは、レプリケーションは自動で管理されます。テーブルを作成する際、引数を追加せずに作成してください。例えば、以下の例では次のように置き換えてください：
```
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', ver)
```
次のように：
```
ENGINE = ReplicatedMergeTree
```
:::

テーブルエンジン名のプレフィックスに `Replicated` を追加します。例えば：`ReplicatedMergeTree`。

:::tip
ClickHouse Cloudでは、すべてのテーブルがレプリケートされているため、`Replicated` を追加する必要はありません。
:::

### Replicated\*MergeTree パラメータ

#### zoo_path

`zoo_path` — ClickHouse Keeper内のテーブルへのパス。

#### replica_name

`replica_name` — ClickHouse Keeper内のレプリカ名。

#### other_parameters

`other_parameters` — レプリケートバージョンを作成するために使用されるエンジンのパラメータ。例えば、`ReplacingMergeTree` 内のバージョンなど。

例：

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32,
    ver UInt16
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', ver)
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID);
```

<details markdown="1">

<summary>旧構文の例</summary>

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192);
```

</details>

例に示したように、これらのパラメータは波括弧で囲まれた置換を含めることができます。置換された値は、設定ファイルの[macros](/docs/ja/operations/server-configuration-parameters/settings.md/#macros)セクションから取得されます。

例：

``` xml
<macros>
    <shard>02</shard>
    <replica>example05-02-1</replica>
</macros>
```

ClickHouse Keeper内でのテーブルへのパスは、各レプリケートテーブルごとにユニークである必要があります。異なるシャードのテーブルは異なるパスを持つ必要があります。この場合、パスは次の部分で構成されています：

`/clickhouse/tables/` は共通のプレフィックスです。これとまったく同じものを使用することをお勧めします。

`{shard}` はシャード識別子に展開されます。

`table_name` は、ClickHouse Keeper内のテーブル用ノードの名前です。これはテーブル名と同じにするのが良い考えです。これは明示的に定義されており、RENAMEクエリ後にテーブル名が変わらないためです。
*ヒント*: `table_name` の前にデータベース名を追加することもできます。例：`db_name.table_name`

組み込みの置換 `{database}` および `{table}` を使用することができ、これらはそれぞれテーブル名とデータベース名に展開されます（これらのマクロが `macros` セクションで定義されている場合を除く）。したがって、zookeeperのパスは `'/clickhouse/tables/{shard}/{database}/{table}'` と指定できます。
これらの組み込みの置換を使用する際には、テーブルの名前変更に注意してください。ClickHouse Keeper内のパスは変更できず、テーブルが名前を変更すると、マクロは異なるパスに展開され、テーブルはClickHouse Keeperに存在しないパスを示し、読み取り専用モードになります。

レプリカ名は同じテーブルの異なるレプリカを識別します。この例ではサーバ名を使用しています。この名前は各シャード内でユニークである必要があります。

小規模なクラスタの構成やテストには、置換を使用せずにパラメータを明示的に定義することもできます。ですが、この場合、分散DDLクエリ（`ON CLUSTER`）を使用することはできません。

大規模クラスタで作業する場合、誤りの可能性を減らすために置換の使用をお勧めします。

サーバ設定ファイルで `Replicated` テーブルエンジンのデフォルト引数を指定できます。例えば：

```xml
<default_replica_path>/clickhouse/tables/{shard}/{database}/{table}</default_replica_path>
<default_replica_name>{replica}</default_replica_name>
```

この場合、テーブルの作成時に引数を省略できます：

``` sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree
ORDER BY x;
```

これは次と同等です：

``` sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/table_name', '{replica}')
ORDER BY x;
```

`CREATE TABLE` クエリを各レプリカで実行します。このクエリは新しいレプリケートテーブルを作成するか、既存のテーブルに新しいレプリカを追加します。

他のレプリカ上に既にデータが含まれているテーブルに新しいレプリカを追加する場合は、クエリを実行した後、このテーブルからデータが新しいレプリカにコピーされます。言い換えれば、新しいレプリカは他のレプリカと同期されます。

レプリカを削除するには、`DROP TABLE` を実行します。ただし、削除されるのはクエリを実行したサーバ上のレプリカだけです。

## 障害後のリカバリ {#recovery-after-failures}

ClickHouse Keeperが利用できない場合、レプリケートテーブルは読み取り専用モードに切り替わります。システムは定期的にClickHouse Keeperへの接続を試みます。

`INSERT` 中にClickHouse Keeperが利用できない場合、またはClickHouse Keeperとのインタラクション中にエラーが発生した場合、例外がスローされます。

ClickHouse Keeperとの接続後、システムはローカルファイルシステム内のデータセットが期待されるデータセット（ClickHouse Keeperがこの情報を保存しています）と一致するかどうかを確認します。小さな不一致がある場合、システムはそれらをレプリカと同期させて解決します。

システムが破損したデータ部分（ファイルのサイズが間違っているもの）や未確認の部分（ファイルシステムに書き込まれたがClickHouse Keeperには記録されていない部分）を検出した場合、それらを `detached` サブディレクトリに移動します（削除はされません）。不明な部分はレプリカからコピーされます。

ClickHouseは大量のデータ削除を自動で行うような破壊的な操作を行いません。

サーバが起動する際（またはClickHouse Keeperとの新しいセッションを確立する際）、ファイルの量とサイズのみがチェックされます。ファイルサイズが一致していても中間のバイトが変更されている場合、これはすぐには検出されず、`SELECT` クエリでデータを読む際に非一致のチェックサムや圧縮ブロックのサイズについての例外が発生します。この場合、データ部分は検証キューに追加され、必要であればレプリカからコピーされます。

ローカルのデータセットが期待値と大きく異なる場合、安全機構が作動します。サーバはこれをログに記録し、起動を拒否します。この理由は、この場合が設定エラーを示す可能性があるためです。たとえば、あるシャードのレプリカが誤って異なるシャードとして設定された場合です。しかし、このメカニズムのしきい値はかなり低く設定されており、通常の障害復旧中にこの状況が発生することがあります。この場合、データは半自動的に、すなわち「ボタンを押す」という手段で復旧されます。

復旧を開始するには、ClickHouse Keeperに以下のノードを任意の内容で作成するか、すべてのレプリケートテーブルを復元するコマンドを実行してください：

``` bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

その後、サーバを再起動します。起動後、サーバはこれらのフラグを削除し、復旧を開始します。

## データの完全消失後の復旧 {#recovery-after-complete-data-loss}

あるサーバからすべてのデータとメタデータが消失した場合、次のステップで復旧を行います：

1. ClickHouseをサーバにインストールします。シャード識別子とレプリカが使用されている場合、それらが置換されるように設定ファイルを正しく定義します。
2. サーバに手動で複製された非レプリケートテーブルがあった場合、それらのデータをコピーします（ここで、レプリカのディレクトリは `/var/lib/clickhouse/data/db_name/table_name/` です）。
3. メタデータディレクトリ内の `/var/lib/clickhouse/metadata/` ファイルからテーブル定義をコピーします。シャードまたはレプリカ識別子がテーブル定義内で明示的に定義されている場合は、これをこのレプリカに対応するように修正します。（あるいは、サーバを開始して、/var/lib/clickhouse/metadata/ 内の.sqlファイルに含まれるべき`ATTACH TABLE` クエリをすべて実行します。）
4. 復旧を開始するには、ClickHouse Keeperに以下のノードを任意の内容で作成するか、すべてのレプリケートテーブルを復元するコマンドを実行してください： `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

その後、サーバを開始（既に実行中の場合は再起動）します。データはレプリカからダウンロードされます。

別の復旧オプションとして、失われたレプリカについての情報をClickHouse Keeperから削除（`/path_to_table/replica_name`）、次に "[レプリケートテーブルの作成](#creating-replicated-tables)"で記述されているようにレプリカを再作成することができます。

復旧中にはネットワーク帯域幅の制限はありません。多くのレプリカを一度に復旧している場合にはこれに注意してください。

## MergeTreeからReplicatedMergeTreeへの変換 {#converting-from-mergetree-to-replicatedmergetree}

`MergeTree` は、すべての `MergeTree ファミリー` テーブルエンジンを指すために使用されます。ReplicatedMergeTreeについても同様です。

手動でレプリケートされた`MergeTree` テーブルがあった場合、レプリケートテーブルに変換できます。これは大量のデータが既に `MergeTree` テーブルに集められていてレプリケーションを有効にしたい場合に必要なことかもしれません。

`MergeTree` テーブルは、テーブルデータディレクトリの `convert_to_replicated` フラグが設定されている場合、サーバの再起動時に自動的に変換されます（`Atomic` データベースの場合は `/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/`）。
空の `convert_to_replicated` ファイルを作成すると、次回サーバの再起動時にテーブルはレプリケートされたものとして読み込まれます。

このクエリはテーブルのデータパスを取得するために使用できます。テーブルに多くのデータパスがある場合、最初のものを使用する必要があります。

```sql
SELECT data_paths FROM system.tables WHERE table = 'table_name' AND database = 'database_name';
```

`ReplicatedMergeTree` テーブルは `default_replica_path` と `default_replica_name` の設定値で作成されます。他のレプリカで変換テーブルを作成するには、そのテーブルのパスを `ReplicatedMergeTree` エンジンの最初の引数に明示的に指定する必要があります。次のクエリを使ってそのパスを取得できます。

```sql
SELECT zookeeper_path FROM system.replicas WHERE table = 'table_name';
```

サーバ再起動なしに手動で行う方法もあります。

データが様々なレプリカで異なる場合、まずそれを同期するか、あるレプリカのデータを削除します。

既存の MergeTree テーブルの名前を変更し、その古い名前で `ReplicatedMergeTree` テーブルを作成します。
古いテーブルから新しいテーブルのデータディレクトリの `detached` サブディレクトリにデータを移動します（ここで、パスは `/var/lib/clickhouse/data/db_name/table_name/` です）。
その後、1つのレプリカで `ALTER TABLE ATTACH PARTITION` を実行して、これらのデータ部分を作業セットに追加します。

## ReplicatedMergeTreeからMergeTreeへの変換 {#converting-from-replicatedmergetree-to-mergetree}

異なる名前でMergeTreeテーブルを作成します。`ReplicatedMergeTree` テーブルデータのディレクトリからすべてのデータを新しいテーブルのデータディレクトリに移動します。その後、`ReplicatedMergeTree` テーブルを削除し、サーバーを再起動します。

`ReplicatedMergeTree` テーブルをサーバーの起動なしに削除したい場合：

- メタデータディレクトリの対応する `.sql` ファイルを削除します（ `/var/lib/clickhouse/metadata/`）。
- ClickHouse Keeperで対応するパスを削除します（ `/path_to_table/replica_name`）。

その後、サーバーを起動し、`MergeTree` テーブルを作成し、データをそのディレクトリに移動し、サーバーを再起動します。

## ClickHouse Keeper クラスタ内のメタデータが破損・消失した場合のリカバリ {#recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged}

ClickHouse Keeper内のデータが失われたか破損した場合、上述の方法に従い、データを非レプリケートテーブルに移動することでデータを保管することができます。

**関連項目**

- [background_schedule_pool_size](/docs/ja/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size)
- [background_fetches_pool_size](/docs/ja/operations/server-configuration-parameters/settings.md/#background_fetches_pool_size)
- [execute_merges_on_single_replica_time_threshold](/docs/ja/operations/settings/settings.md/#execute-merges-on-single-replica-time-threshold)
- [max_replicated_fetches_network_bandwidth](/docs/ja/operations/settings/merge-tree-settings.md/#max_replicated_fetches_network_bandwidth)
- [max_replicated_sends_network_bandwidth](/docs/ja/operations/settings/merge-tree-settings.md/#max_replicated_sends_network_bandwidth)

