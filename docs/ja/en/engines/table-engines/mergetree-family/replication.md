---
description: 'ClickHouse における Replicated* ファミリーのテーブルエンジンによるデータレプリケーションの概要'
sidebar_label: 'Replicated*'
sidebar_position: 20
slug: /engines/table-engines/mergetree-family/replication
title: 'Replicated* テーブルエンジン'
doc_type: 'reference'
---

:::note
ClickHouse Cloud ではレプリケーションは自動的に管理されるため、引数を追加せずにテーブルを作成してください。たとえば、以下のテキストでは次のように置き換えます。

```sql
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/table_name',
    '{replica}'
)
```

次を使用します:

```sql
ENGINE = ReplicatedMergeTree
```

:::

レプリケーションは、MergeTree family に属するテーブルでのみサポートされています

* ReplicatedSummingMergeTree
* ReplicatedCoalescingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedGraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree

レプリケーションはサーバー全体ではなく、個々のテーブル単位で機能します。1 台のサーバーに、レプリケートテーブルと非レプリケートテーブルの両方を同時に格納できます。

レプリケーションはシャーディングに依存しません。各分片はそれぞれ独立してレプリケーションされます。

`INSERT` および `ALTER` クエリの圧縮データはレプリケートされます (詳細については、[ALTER](/ja/sql-reference/statements/alter) のドキュメントを参照してください) 。

`CREATE`、`DROP`、`ATTACH`、`DETACH`、`RENAME` クエリは 1 台のサーバー上で実行され、レプリケートされません:

* `CREATE TABLE` クエリは、クエリを実行したサーバー上に新しいレプリケーション可能なテーブルを作成します。このテーブルが他のサーバーにすでに存在する場合は、新しいレプリカを追加します。
* `DROP TABLE` クエリは、クエリを実行したサーバー上にあるレプリカを削除します。
* `RENAME` クエリは、いずれかのレプリカ上でテーブル名を変更します。つまり、レプリケートテーブルはレプリカごとに異なる名前を持つことができます。

ClickHouse は、レプリカのメタ情報を保存するために [ClickHouse Keeper](/ja/guides/sre/keeper/index.md) を使用します。ZooKeeper バージョン 3.4.5 以降を使用することもできますが、ClickHouse Keeper を推奨します。

レプリケーションを使用するには、サーバー設定の [zookeeper](/ja/operations/server-configuration-parameters/settings#zookeeper) セクションでパラメータを設定します。

:::note
セキュリティ設定をおろそかにしないでください。ClickHouse は、ZooKeeper セキュリティサブシステムの `digest` [ACL scheme](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) をサポートしています。
:::

ClickHouse Keeper クラスターのアドレス設定例:

```xml
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

ClickHouse では、レプリカのメタ情報を補助的な ZooKeeper クラスターに保存することもできます。これを行うには、engine の引数として ZooKeeper クラスター名とパスを指定します。
つまり、異なるテーブルのメタデータをそれぞれ別の ZooKeeper クラスターに保存できます。

補助的な ZooKeeper クラスターのアドレスを設定する例:

```xml
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

テーブルのメタデータをデフォルトの ZooKeeper クラスターではなく補助的な ZooKeeper クラスターに保存するには、次のように SQL を使って
ReplicatedMergeTree エンジンのテーブルを作成できます。

```sql
CREATE TABLE table_name ( ... ) ENGINE = ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'replica_name') ...
```

既存の ZooKeeper クラスターを任意に指定でき、システムはその中のディレクトリを自身のデータ用に使用します (このディレクトリは、レプリケーション可能なテーブルの作成時に指定します) 。

設定ファイルで ZooKeeper が設定されていない場合、レプリケートテーブルを作成できず、既存のレプリケートテーブルはすべて読み取り専用になります。

ZooKeeper は `SELECT` クエリでは使用されません。これは、レプリケーションが `SELECT` のパフォーマンスに影響せず、クエリは非レプリケートテーブルの場合と同じ速度で実行されるためです。分散レプリケートテーブルに対してクエリを実行する際の ClickHouse の動作は、設定 [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](/ja/operations/settings/settings.md/#max_replica_delay_for_distributed_queries) および [fallback&#95;to&#95;stale&#95;replicas&#95;for&#95;distributed&#95;queries](/ja/operations/settings/settings.md/#fallback_to_stale_replicas_for_distributed_queries) によって制御されます。

各 `INSERT` クエリごとに、複数のトランザクションを通じておよそ 10 個のエントリが ZooKeeper に追加されます。 (より正確には、これは挿入される各データブロックごとです。1 つの INSERT クエリには 1 つのブロック、または `max_insert_block_size = 1048576` 行ごとに 1 つのブロックが含まれます。) このため、`INSERT` のレイテンシは非レプリケートテーブルと比べてわずかに長くなります。しかし、データは 1 秒あたり `INSERT` 1 回以下のバッチで挿入するという推奨事項に従えば、問題は発生しません。1 つの ZooKeeper クラスターを使って連携する ClickHouse クラスター全体では、合計で 1 秒あたり数百件の `INSERT` を処理できます。データ挿入のスループット (1 秒あたりの行数) は、非レプリケートデータの場合と同じく高いままです。

非常に大規模なクラスターでは、異なる分片ごとに別々の ZooKeeper クラスターを使用できます。しかし、私たちの経験では、約 300 台のサーバーを持つ production クラスターの実績から見ても、これは必要であることが証明されていません。

レプリケーションは非同期かつマルチマスターです。`INSERT` クエリ (および `ALTER`) は、利用可能な任意のサーバーに送信できます。データはクエリが実行されたサーバーに挿入され、その後ほかのサーバーにコピーされます。非同期であるため、直近に挿入されたデータが他のレプリカに反映されるまでには多少のレイテンシがあります。一部のレプリカが利用できない場合、そのレプリカが利用可能になった時点でデータが書き込まれます。レプリカが利用可能であれば、レイテンシは圧縮済みデータのブロックをネットワーク越しに転送するのにかかる時間です。レプリケートテーブルのバックグラウンドタスクを実行するスレッド数は、設定 [background&#95;schedule&#95;pool&#95;size](/ja/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size) で指定できます。

`ReplicatedMergeTree` engine は、レプリケーションフェッチ用に別個のスレッドプールを使用します。プールのサイズは設定 [background&#95;fetches&#95;pool&#95;size](/ja/operations/server-configuration-parameters/settings#background_fetches_pool_size) によって制限され、サーバーの再起動によって調整できます。

デフォルトでは、INSERT クエリは 1 つのレプリカからのデータ書き込み確認のみを待機します。データが 1 つのレプリカにしか正常に書き込まれず、そのレプリカを持つサーバーが消失した場合、保存されていたデータは失われます。複数のレプリカからデータ書き込み確認を得るには、`insert_quorum` オプションを使用してください。

各データブロックはアトミックに書き込まれます。INSERT クエリは最大 `max_insert_block_size = 1048576` 行までのブロックに分割されます。言い換えると、`INSERT` クエリの行数が 1048576 未満であれば、アトミックに実行されます。

データブロックは重複排除されます。同じデータブロック (同じサイズで、同じ行を同じ順序で含むデータブロック) が複数回書き込まれても、そのブロックが書き込まれるのは 1 回だけです。これは、ネットワーク障害時にクライアントアプリケーションがデータが DB に書き込まれたかどうかを把握できない場合でも、`INSERT` クエリをそのまま再実行できるようにするためです。同一データの `INSERT` がどのレプリカに送られたかは問題になりません。`INSERT` は冪等です。重複排除のパラメータは、[merge&#95;tree](/ja/operations/server-configuration-parameters/settings.md/#merge_tree) サーバー設定によって制御されます。

レプリケーション中、ネットワーク越しに転送されるのは挿入対象の元データのみです。その後のデータ変換 (マージ) は、すべてのレプリカ上で同じ方法で調整および実行されます。これによりネットワーク使用量が最小限に抑えられるため、レプリカが異なるデータセンターに配置されている場合でもレプリケーションはうまく機能します。 (異なるデータセンター間でデータを複製することが、レプリケーションの主目的である点に注意してください。)

同じデータに対して、レプリカはいくつでも持つことができます。私たちの経験では、比較的信頼性が高く扱いやすい solution として、production では各サーバーが RAID-5 または RAID-6 (一部の case では RAID-10) を使用し、二重レプリケーションを構成する方法が考えられます。

システムはレプリカ上のデータの同期状態を監視しており、障害後の復旧が可能です。フェイルオーバーは、自動 (データ差分が小さい場合) または半自動 (データ差分が大きく、設定ミスを示している可能性がある場合) で行われます。

<div id="creating-replicated-tables">
  ## レプリケートテーブルの作成
</div>

:::note
ClickHouse Cloud では、レプリケーションは自動的に処理されます。

テーブルは、レプリケーション引数を指定せずに [`MergeTree`](/ja/engines/table-engines/mergetree-family/mergetree) を使って作成してください。システムは内部で、レプリケーションとデータ分散のために [`MergeTree`](/ja/engines/table-engines/mergetree-family/mergetree) を [`SharedMergeTree`](/ja/cloud/reference/shared-merge-tree) に書き換えます。

レプリケーションはプラットフォーム側で管理されるため、`ReplicatedMergeTree` は使用せず、レプリケーションパラメーターも指定しないでください。

:::

<div id="replicatedmergetree-parameters">
  ### Replicated*MergeTree パラメータ
</div>

| パラメータ              | 説明                                                            |
| ------------------ | ------------------------------------------------------------- |
| `zoo_path`         | ClickHouse Keeper 内のテーブルのパス。                                  |
| `replica_name`     | ClickHouse Keeper 内のレプリカ名。                                    |
| `other_parameters` | レプリケート版の作成に使用するエンジンのパラメータ。たとえば、`ReplacingMergeTree` のバージョンです。 |

例:

```sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32,
    ver UInt16
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', ver)
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID);
```

<details markdown="1">
  <summary>非推奨構文での例</summary>

  ```sql
  CREATE TABLE table_name
  (
      EventDate DateTime,
      CounterID UInt32,
      UserID UInt32
  ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192);
  ```
</details>

この例のとおり、これらのパラメーターには `{}` 形式の置換を含めることができます。置換後の値は、設定ファイルの [マクロ](/ja/operations/server-configuration-parameters/settings.md/#macros) セクションから取得されます。

例:

```xml
<macros>
    <shard>02</shard>
    <replica>example05-02-1</replica>
</macros>
```

ClickHouse Keeper 内のテーブルへのパスは、各レプリケートテーブルごとに一意である必要があります。異なる分片上のテーブルには、それぞれ異なるパスを設定する必要があります。
この場合、パスは次の要素で構成されます。

`/clickhouse/tables/` は共通のプレフィックスです。これをそのまま使うことを推奨します。

`{shard}` は分片識別子に展開されます。

`table_name` は ClickHouse Keeper 内でそのテーブルに対応するノード名です。テーブル名と同じにしておくのがよいでしょう。これは明示的に定義します。というのも、テーブル名とは異なり、`RENAME` クエリの後も変わらないためです。
*HINT*: `table_name` の前にデータベース名を付けることもできます。例: `db_name.table_name`

組み込みの置換 `{database}` と `{table}` も使用できます。これらはそれぞれテーブル名とデータベース名に展開されます (これらのマクロが `macros` セクションで定義されていない場合) 。そのため、ZooKeeper パスは `'/clickhouse/tables/{shard}/{database}/{table}'` のように指定できます。
これらの組み込み置換を使う場合は、テーブルのリネームに注意してください。ClickHouse Keeper 内のパスは変更できず、テーブルをリネームすると、マクロは別のパスに展開されます。その結果、テーブルは ClickHouse Keeper 内に存在しないパスを参照することになり、読み取り専用モードに入ります。

レプリカ名は、同じテーブルの異なるレプリカを識別します。例のように、これにはサーバー名を使用できます。名前は各分片内で一意であれば十分です。

置換を使わずに、パラメータを明示的に定義することもできます。これは、テストや小規模なクラスターの設定では便利な場合があります。ただし、この場合は分散 DDL クエリ (`ON CLUSTER`) を使用できません。

大規模なクラスターで運用する場合は、ミスの可能性を減らせるため、置換を使用することを推奨します。

`Replicated` テーブルエンジンのデフォルト引数は、サーバー設定ファイルで指定できます。たとえば次のようになります。

```xml
<default_replica_path>/clickhouse/tables/{shard}/{database}/{table}</default_replica_path>
<default_replica_name>{replica}</default_replica_name>
```

この場合、テーブル作成時には引数を省略できます。

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree
ORDER BY x;
```

以下と同等です。

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/table_name', '{replica}')
ORDER BY x;
```

各レプリカで`CREATE TABLE`クエリを実行します。このクエリにより、新しいレプリケートテーブルを作成することも、既存のテーブルに新しいレプリカを追加することもできます。

すでに他のレプリカにデータが存在する状態で新しいレプリカを追加した場合、クエリの実行後、そのデータは他のレプリカから新しいレプリカにコピーされます。つまり、新しいレプリカは他のレプリカと自動的に同期されます。

レプリカを削除するには、`DROP TABLE`を実行します。ただし、削除されるのは1つのレプリカだけで、クエリを実行したサーバー上のレプリカのみです。

<div id="recovery-after-failures">
  ## 障害後の復旧
</div>

サーバーの起動時に ClickHouse Keeper が利用できない場合、レプリケートテーブルは読み取り専用モードに切り替わります。システムは定期的に ClickHouse Keeper への接続を試みます。

`INSERT` 中に ClickHouse Keeper が利用できない場合、または ClickHouse Keeper とのやり取り中にエラーが発生した場合は、例外がスローされます。

ClickHouse Keeper に接続すると、システムはローカルファイルシステム上のデータ一式が期待されるデータ一式と一致しているかどうかを確認します (この情報は ClickHouse Keeper に保存されています) 。軽微な不整合がある場合、システムはレプリカとデータを同期してそれらを解消します。

システムが破損したデータパーツ (ファイルサイズが正しくないもの) や未認識のパーツ (ファイルシステムに書き込まれているが ClickHouse Keeper に記録されていないパーツ) を検出すると、それらを `detached` サブディレクトリに移動します (削除はされません) 。不足しているパーツはレプリカからコピーされます。

ClickHouse は、大量のデータを自動的に削除するような破壊的操作は行わないことに注意してください。

サーバーの起動時 (または ClickHouse Keeper との新しいセッションを確立したとき) 、システムはすべてのファイルの数とサイズだけを確認します。ファイルサイズが一致していても、途中のどこかでバイトが変更されていた場合、これはすぐには検出されず、`SELECT` クエリでデータを読み取ろうとしたときに初めて検出されます。このクエリは、チェックサムまたは圧縮ブロックのサイズが一致しないことに関する例外をスローします。この場合、データパーツは検証キューに追加され、必要に応じてレプリカからコピーされます。

ローカルのデータ一式が期待されるものと大きく異なる場合、安全機構がトリガーされます。サーバーはこれをログに記録し、起動を拒否します。その理由は、このケースが設定ミスを示している可能性があるためです。たとえば、ある分片上のレプリカが、誤って別の分片上のレプリカとして設定されている場合です。ただし、この機構のしきい値はかなり低く設定されているため、この状況は通常の障害復旧中にも発生することがあります。この場合、データは「ボタンを押す」ことで半自動的に復旧されます。

復旧を開始するには、ClickHouse Keeper に任意の内容でノード `/path_to_table/replica_name/flags/force_restore_data` を作成するか、すべてのレプリケートテーブルを復旧するコマンドを実行します。

```bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

その後、サーバーを再起動します。起動すると、サーバーはこれらのフラグを削除し、復旧を開始します。

<div id="recovery-after-complete-data-loss">
  ## 完全なデータ損失後の復旧
</div>

いずれかのサーバーで、すべてのデータとメタデータが失われた場合は、復旧のために次の手順に従ってください。

1. サーバーに ClickHouse をインストールします。必要に応じて、分片識別子とレプリカを含む設定ファイルで置換を正しく定義します。
2. サーバー間で手動コピーが必要な、レプリケートされていないテーブルがある場合は、そのデータをレプリカからコピーします (ディレクトリ `/var/lib/clickhouse/data/db_name/table_name/`) 。
3. `/var/lib/clickhouse/metadata/` にあるテーブル定義をレプリカからコピーします。テーブル定義内で分片またはレプリカの識別子が明示的に定義されている場合は、このレプリカに対応するよう修正します。 (または、サーバーを起動し、`/var/lib/clickhouse/metadata/` 内の .sql ファイルに含まれているはずの `ATTACH TABLE` クエリをすべて実行します。)
4. 復旧を開始するには、ClickHouse Keeper ノード `/path_to_table/replica_name/flags/force_restore_data` を任意の内容で作成するか、すべてのレプリケートテーブルを復元するための次のコマンドを実行します: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

その後、サーバーを起動します (すでに稼働中の場合は再起動します) 。データはレプリカからダウンロードされます。

別の復旧方法としては、ClickHouse Keeper から失われたレプリカの情報 (`/path_to_table/replica_name`) を削除し、その後 &quot;[レプリケートテーブルの作成](#creating-replicated-tables)&quot; で説明しているとおりにレプリカを再作成することもできます。

復旧中のネットワーク帯域幅には制限がありません。多数のレプリカを同時に復元する場合は、この点に注意してください。

<div id="converting-from-mergetree-to-replicatedmergetree">
  ## MergeTree から ReplicatedMergeTree への変換
</div>

ここでは `MergeTree` という用語を、`ReplicatedMergeTree` と同様に、`MergeTree family` に属するすべてのテーブルエンジンを指すものとして使用します。

手動でレプリケーションしていた `MergeTree` テーブルがある場合は、それをレプリケートテーブルに変換できます。これは、すでに `MergeTree` テーブルに大量のデータを収集しており、そこからレプリケーションを有効にしたい場合に必要になることがあります。

[ATTACH TABLE ... AS REPLICATED](/ja/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) ステートメントを使用すると、デタッチされた `MergeTree` テーブルを `ReplicatedMergeTree` としてアタッチできます。

テーブルのデータディレクトリ (`Atomic` データベースの場合は `/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/`) に `convert_to_replicated` フラグが設定されていると、`MergeTree` テーブルはサーバーの再起動時に自動的に変換されます。
空の `convert_to_replicated` ファイルを作成すると、次回のサーバー再起動時にそのテーブルはレプリケートされたテーブルとして読み込まれます。

このクエリを使用すると、テーブルのデータパスを取得できます。テーブルに複数のデータパスがある場合は、最初のものを使用する必要があります。

```sql
SELECT data_paths FROM system.tables WHERE table = 'table_name' AND database = 'database_name';
```

ReplicatedMergeTree テーブルは、`default_replica_path` および `default_replica_name` 設定の値を使用して作成されることに注意してください。
他のレプリカ上に変換後のテーブルを作成するには、`ReplicatedMergeTree` エンジンの第1引数にそのパスを明示的に指定する必要があります。次のクエリを使用してそのパスを取得できます。

```sql
SELECT zookeeper_path FROM system.replicas WHERE table = 'table_name';
```

これを行うには、手動の方法もあります。

各レプリカでデータに差異がある場合は、先に同期するか、1つを残して他のすべてのレプリカからこのデータを削除します。

既存の MergeTree テーブルをリネームしてから、元の名前で `ReplicatedMergeTree` テーブルを作成します。
古いテーブルのデータを、新しいテーブルのデータがあるディレクトリ (`/var/lib/clickhouse/data/db_name/table_name/`) 内の `detached` サブディレクトリに移動します。
その後、いずれかのレプリカで `ALTER TABLE ATTACH PARTITION` を実行して、これらのデータパーツをアクティブなセットに追加します。

<div id="converting-from-replicatedmergetree-to-mergetree">
  ## ReplicatedMergeTree から MergeTree への変換
</div>

単一サーバー上で、デタッチされた `ReplicatedMergeTree` テーブルを `MergeTree` としてアタッチするには、[ATTACH TABLE ... AS NOT REPLICATED](/ja/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) ステートメントを使用します。

これを行う別の方法として、サーバーの再起動を伴う手順があります。別の名前で MergeTree テーブルを作成します。`ReplicatedMergeTree` テーブルのデータが格納されているディレクトリから、すべてのデータを新しいテーブルのデータディレクトリに移動します。次に `ReplicatedMergeTree` テーブルを削除し、サーバーを再起動します。

サーバーを起動せずに `ReplicatedMergeTree` テーブルを削除したい場合は、次のようにします。

* メタデータディレクトリ (`/var/lib/clickhouse/metadata/`) 内の対応する `.sql` ファイルを削除します。
* ClickHouse Keeper 内の対応するパス (`/path_to_table/replica_name`) を削除します。

この後、サーバーを起動し、`MergeTree` テーブルを作成して、そのディレクトリにデータを移動し、その後サーバーを再起動できます。

<div id="recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged">
  ## ClickHouse Keeper クラスター内のメタデータが失われた、または破損した場合の復旧
</div>

ClickHouse Keeper 内のデータが失われた、または破損した場合は、前述のとおり、データを非レプリケートテーブルに移動することで保全できます。

**関連項目**

* [background&#95;schedule&#95;pool&#95;size](/ja/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size)
* [background&#95;fetches&#95;pool&#95;size](/ja/operations/server-configuration-parameters/settings.md/#background_fetches_pool_size)
* [execute&#95;merges&#95;on&#95;single&#95;replica&#95;time&#95;threshold](/ja/operations/settings/merge-tree-settings#execute_merges_on_single_replica_time_threshold)
* [max&#95;replicated&#95;fetches&#95;network&#95;bandwidth](/ja/operations/settings/merge-tree-settings.md/#max_replicated_fetches_network_bandwidth)
* [max&#95;replicated&#95;sends&#95;network&#95;bandwidth](/ja/operations/settings/merge-tree-settings.md/#max_replicated_sends_network_bandwidth)