---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: "\u5206\u6563"
---

# 分散 {#distributed}

**分散エンジンを備えたテーブルは、データ自体を格納しません** しかし、複数のサーバーで分散クエリ処理を許可します。
読み取りは自動的に並列化されます。 読み取り中に、リモートサーバー上のテーブルインデックスが存在する場合に使用されます。

分散エンジ:

-   サーバーの設定ファイル内のクラスタ名

-   リモートデータベースの名前

-   リモートテーブルの名前

-   （オプション)shardingキー

-   (必要に応じて)ポリシー名は、非同期送信の一時ファイルを格納するために使用されます

    も参照。:

    -   `insert_distributed_sync` 設定
    -   [メルゲツリー](../mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) 例については

例:

``` sql
Distributed(logs, default, hits[, sharding_key[, policy_name]])
```

すべてのサーバからデータが読み取られます。 ‘logs’ デフォルトからのクラスター。ヒットテーブルに位置毎にサーバのクラスター
データは読み取りだけでなく、リモートサーバーで部分的に処理されます（可能な限り）。
たとえば、GROUP BYを使用したクエリの場合、リモートサーバーでデータが集計され、集計関数の中間状態が要求元サーバーに送信されます。 その後、データはさらに集計されます。

データベース名の代わりに、文字列を返す定数式を使用できます。 たとえば、currentDatabase()です。

logs – The cluster name in the server's config file.

クラスターがセットのようなこ:

``` xml
<remote_servers>
    <logs>
        <shard>
            <!-- Optional. Shard weight when writing data. Default: 1. -->
            <weight>1</weight>
            <!-- Optional. Whether to write data to just one of the replicas. Default: false (write data to all replicas). -->
            <internal_replication>false</internal_replication>
            <replica>
                <host>example01-01-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-01-2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <weight>2</weight>
            <internal_replication>false</internal_replication>
            <replica>
                <host>example01-02-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-02-2</host>
                <secure>1</secure>
                <port>9440</port>
            </replica>
        </shard>
    </logs>
</remote_servers>
```

ここで、クラスターは名前で定義されます ‘logs’ これは二つの破片で構成され、それぞれに二つの複製が含まれています。
シャードは、データの異なる部分を含むサーバーを指します(すべてのデータを読み取るには、すべてのシャードにアクセスする必要があります)。
レプリカはサーバーを複製しています（すべてのデータを読み取るために、レプリカのいずれかのデータにアクセスできます）。

クラスターの名前などを含めないでくださいませんでした。

パラメータ `host`, `port`、および必要に応じて `user`, `password`, `secure`, `compression` サーバーごとに指定されます:
- `host` – The address of the remote server. You can use either the domain or the IPv4 or IPv6 address. If you specify the domain, the server makes a DNS request when it starts, and the result is stored as long as the server is running. If the DNS request fails, the server doesn't start. If you change the DNS record, restart the server.
- `port` – The TCP port for messenger activity (‘tcp_port’ 設定では、通常9000に設定されています）。 Http_portと混同しないでください。
- `user` – Name of the user for connecting to a remote server. Default value: default. This user must have access to connect to the specified server. Access is configured in the users.xml file. For more information, see the section [アクセス権](../../../operations/access-rights.md).
- `password` – The password for connecting to a remote server (not masked). Default value: empty string.
- `secure` -接続にsslを使用します。 `port` = 9440. サーバーがリッスンする `<tcp_port_secure>9440</tcp_port_secure>` 正しい証明書を持っています。
- `compression` -データ圧縮を使用します。 デフォルト値:true。

When specifying replicas, one of the available replicas will be selected for each of the shards when reading. You can configure the algorithm for load balancing (the preference for which replica to access) – see the [load_balancing](../../../operations/settings/settings.md#settings-load_balancing) 設定。
サーバーとの接続が確立されていない場合は、短いタイムアウトで接続しようとします。 接続に失敗した場合は、すべてのレプリカに対して次のレプリカが選択されます。 すべてのレプリカで接続試行が失敗した場合、その試行は同じ方法で何度か繰り返されます。
リモートサーバーは接続を受け入れるかもしれませんが、動作しないか、または不十分に動作する可能性があります。

いずれかのシャードのみを指定することができます(この場合、クエリ処理は分散ではなくリモートと呼ばれる必要があります)。 各シャードでは、いずれかから任意の数のレプリカを指定できます。 シャードごとに異なる数のレプリカを指定できます。

設定では、必要な数のクラスターを指定できます。

クラスターを表示するには、 ‘system.clusters’ テーブル。

の分散型エンジン能にすることで、社会とクラスターのように現地サーバーです。 サーバー設定ファイルにその設定を書き込む必要があります(すべてのクラスターのサーバーでさらに優れています)。

The Distributed engine requires writing clusters to the config file. Clusters from the config file are updated on the fly, without restarting the server. If you need to send a query to an unknown set of shards and replicas each time, you don't need to create a Distributed table – use the ‘remote’ 代わりにテーブル関数。 セクションを参照 [テーブル関数](../../../sql-reference/table-functions/index.md).

クラスターにデータを書き込む方法は二つあります:

まず、どのサーバーにどのデータを書き込むかを定義し、各シャードで直接書き込みを実行できます。 つまり、分散テーブルのテーブルに挿入を実行します “looks at”. これは、対象領域の要件のために自明ではない可能性のあるシャーディングスキームを使用できるので、最も柔軟なソリューションです。 データは完全に独立して異なるシャードに書き込むことができるので、これも最適な解決策です。

次に、分散テーブルでINSERTを実行できます。 この場合、テーブルは挿入されたデータをサーバー自体に分散します。 分散テーブルに書き込むには、シャーディングキーセット(最後のパラメーター)が必要です。 さらに、シャードがひとつしかない場合、この場合は何も意味しないため、シャーディングキーを指定せずに書き込み操作が機能します。

各シャードには、設定ファイルで定義された重みを設定できます。 デフォルトでは、重みは重みと等しくなります。 データは、シャードの重みに比例する量でシャードに分散されます。 たとえば、シャードが二つあり、最初のシャードが9の重みを持ち、次のシャードが10の重みを持つ場合、最初のシャードは行の9/19部分に送信され、次のシャー

各シャードは、 ‘internal_replication’ 設定ファイルで定義されたパラメータ。

このパラメータが ‘true’ 書き込み操作は、最初の正常なレプリカを選択し、それにデータを書き込みます。 分散テーブルの場合は、この代替を使用します “looks at” 複製されたテーブル。 言い換えれば、データが書き込まれるテーブルがそれ自体を複製する場合。

に設定されている場合 ‘false’ データはすべてのレプリカに書き込まれます。 本質的に、これは分散テーブルがデータ自体を複製することを意味します。 これは、レプリケートされたテーブルを使用するよりも悪いことです。

データの行が送信されるシャードを選択するために、シャーディング式が分析され、残りの部分がシャードの総重量で除算されます。 この行は、残りの半分の間隔に対応するシャードに送信されます。 ‘prev_weight’ に ‘prev_weights + weight’,ここで ‘prev_weights’ は、最小の数を持つ破片の総重量です。 ‘weight’ この破片の重量です。 たとえば、シャードが二つあり、最初のシャードの重みが9で、次のシャードの重みが10である場合、行は範囲\[0,9)の残りのシャードの最初のシャードに送信され、

シャーディング式には、整数を返す定数およびテーブル列の任意の式を使用できます。 たとえば、次の式を使用できます ‘rand()’ データのランダム分布の場合、または ‘UserID’ ユーザーのIDを分割することからの残りの部分による分配のために（単一のユーザーのデータは単一のシャード上に存在し、ユーザーによる実行と結合を簡素化する）。 いずれかの列が十分に均等に分散されていない場合は、ハッシュ関数intHash64(UserID)でラップできます。

簡単なリマインダからの限定シshardingんを常に適しています。 これは、データの中規模および大規模なボリューム（サーバーの数十）ではなく、データの非常に大規模なボリューム（サーバーの数百以上）のために動作します。 後者の場合、分散テーブルのエントリを使用するのではなく、サブジェクト領域で必要なシャーディングスキームを使用します。

SELECT queries are sent to all the shards and work regardless of how data is distributed across the shards (they can be distributed completely randomly). When you add a new shard, you don't have to transfer the old data to it. You can write new data with a heavier weight – the data will be distributed slightly unevenly, but queries will work correctly and efficiently.

次の場合は、シャーディングスキームについて心配する必要があります:

-   特定のキーによるデータの結合(INまたはJOIN)を必要とするクエリが使用されます。 このキーによってデータがシャードされる場合は、GLOBAL INまたはGLOBAL JOINの代わりにlocal INまたはJOINを使用できます。
-   多数のサーバーが、多数の小さなクエリ（個々のクライアント-ウェブサイト、広告主、またはパートナーのクエリ）で使用されます（数百以上）。 小さなクエリがクラスタ全体に影響を与えないようにするには、単一のシャード上の単一のクライアントのデータを検索することが理にかなってい また、我々はYandexのでやったように。Metricaでは、biレベルのシャーディングを設定できます：クラスタ全体を次のように分割します “layers” ここで、レイヤーは複数のシャードで構成されます。 単一のクライアントのデータは単一のレイヤー上にありますが、必要に応じてシャードをレイヤーに追加することができ、データはランダムに分散されます。 分散テーブルはレイヤごとに作成され、グローバルクエリ用に単一の共有分散テーブルが作成されます。

データは非同期に書き込まれます。 テーブルに挿入すると、データブロックはローカルファイルシステムに書き込まれます。 データはできるだけ早くバックグラウンドでリモートサーバーに送信されます。 データを送信するための期間は、 [distributed_directory_monitor_sleep_time_ms](../../../operations/settings/settings.md#distributed_directory_monitor_sleep_time_ms) と [distributed_directory_monitor_max_sleep_time_ms](../../../operations/settings/settings.md#distributed_directory_monitor_max_sleep_time_ms) 設定。 その `Distributed` エンジンは、挿入されたデータを含む各ファイルを別々に送信しますが、 [distributed_directory_monitor_batch_inserts](../../../operations/settings/settings.md#distributed_directory_monitor_batch_inserts) 設定。 この設定の改善にクラスターの性能をより一層の活用地域のサーバやネットワーク資源です。 を確認しておきましょうか否かのデータが正常に送信されるチェックリストファイル(データまたは間に-をはさんだ)はテーブルディレクトリ: `/var/lib/clickhouse/data/database/table/`.

分散テーブルへの挿入後にサーバーが存在しなくなった場合、またはデバイスの障害後などに大まかな再起動が行われた場合は、挿入されたデータが失われ テーブルディレクトリで破損したデータ部分が検出されると、その部分は ‘broken’ 使用されなくなりました。

Max_parallel_replicasオプションを有効にすると、単一のシャード内のすべてのレプリカでクエリ処理が並列化されます。 詳細については [max_parallel_replicas](../../../operations/settings/settings.md#settings-max_parallel_replicas).

## 仮想列 {#virtual-columns}

-   `_shard_num` — Contains the `shard_num` （から `system.clusters`). タイプ: [UInt32](../../../sql-reference/data-types/int-uint.md).

!!! note "注"
    以来 [`remote`](../../../sql-reference/table-functions/remote.md)/`cluster` 表関数は内部的に同じ分散エンジンの一時インスタンスを作成します, `_shard_num` あまりにもそこに利用可能です。

**も参照。**

-   [仮想列](index.md#table_engines-virtual_columns)

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/distributed/) <!--hide-->
