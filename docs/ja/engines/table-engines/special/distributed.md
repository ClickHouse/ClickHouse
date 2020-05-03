---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 33
toc_title: "\u5206\u6563"
---

# 分散 {#distributed}

**分散エンジンを持つテーブルは、自身がデータを保存しません** しかし、複数のサーバー上の分散クエリ処理を許可します。
読書は自動的に平行である。 読み取り中に、リモートサーバー上のテーブルインデックスが使用されます。

の分散型エンジンを受け付けパラメータ:

-   サーバーの設定ファイル内のクラスター名

-   リモートデータベースの名前

-   リモートテーブルの名前

-   シャーディングキー

-   （オプションで）ポリシー名は、非同期送信のための一時ファイルを格納するために使用される

    また見なさい:

    -   `insert_distributed_sync` 設定
    -   [MergeTree](../mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) 例のため

例えば:

``` sql
Distributed(logs, default, hits[, sharding_key[, policy_name]])
```

データはすべてのサーバから読み込まれます。 ‘logs’ デフォルトのクラスター。ヒットテーブルに位置毎にサーバのクラスター
データは読み取られるだけでなく、リモートサーバーで部分的に処理されます（これが可能な限り）。
たとえば、group byを使用するクエリの場合、データはリモートサーバー上で集計され、集計関数の中間状態がリクエスターサーバーに送信されます。 その後、データはさらに集約されます。

データベース名の代わりに、文字列を返す定数式を使用できます。 たとえば、次のようになります。

logs – The cluster name in the server’s config file.

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

ここでは、クラスターは名前で定義されます ‘logs’ それぞれ二つのレプリカを含む二つのシャードで構成されています。
シャードは、データの異なる部分を含むサーバーを参照します（すべてのデータを読み取るには、すべてのシャードにアクセスする必要があります）。
レプリカはサーバーを複製しています（すべてのデータを読み取るために、レプリカのいずれかのデータにアクセスできます）。

クラ

パラメータ `host`, `port`、およびオプション `user`, `password`, `secure`, `compression` サーバーごとに指定します:
- `host` – The address of the remote server. You can use either the domain or the IPv4 or IPv6 address. If you specify the domain, the server makes a DNS request when it starts, and the result is stored as long as the server is running. If the DNS request fails, the server doesn’t start. If you change the DNS record, restart the server.
- `port` – The TCP port for messenger activity (‘tcp\_port’ 設定では、通常9000）に設定します。 それをhttp\_portと混同しないでください。
- `user` – Name of the user for connecting to a remote server. Default value: default. This user must have access to connect to the specified server. Access is configured in the users.xml file. For more information, see the section [アクセス権](../../../operations/access-rights.md).
- `password` – The password for connecting to a remote server (not masked). Default value: empty string.
- `secure` -接続にsslを使用します。 `port` = 9440. サーバーがリッスンする <tcp_port_secure>9440</tcp_port_secure> と正しい証明書。
- `compression` -データ圧縮を使用します。 デフォルト値:true。

When specifying replicas, one of the available replicas will be selected for each of the shards when reading. You can configure the algorithm for load balancing (the preference for which replica to access) – see the [load\_balancing](../../../operations/settings/settings.md#settings-load_balancing) 設定。
サーバーとの接続が確立されていない場合は、短いタイムアウトで接続しようとします。 接続に失敗すると、すべてのレプリカに対して次のレプリカが選択されます。 すべてのレプリカに対して接続の試行が失敗した場合、その試行は同じ方法で何度も繰り返されます。
リモートサーバーは接続を受け入れる可能性がありますが、動作しない可能性があります。

シャードのいずれかを指定できます(この場合、クエリ処理は分散ではなくリモートと呼ばれる必要があります)、または任意の数のシャードまで指定でき 各シャードでは、レプリカのいずれかから任意の数に指定することができます。 シャードごとに異なる数のレプリカを指定できます。

構成では、任意の数のクラスターを指定できます。

クラスタを表示するには、以下を使用します ‘system.clusters’ テーブル。

の分散型エンジン能にすることで、社会とクラスターのように現地サーバーです。 ただし、クラスターの構成はサーバー設定ファイルに書き込む必要があります(クラスターのすべてのサーバーではさらに優れています)。

The Distributed engine requires writing clusters to the config file. Clusters from the config file are updated on the fly, without restarting the server. If you need to send a query to an unknown set of shards and replicas each time, you don’t need to create a Distributed table – use the ‘remote’ 代わりにテーブル関数。 セクションを見る [テーブル関数](../../../sql-reference/table-functions/index.md).

クラスターにデータを書き込む方法は二つあります:

まず、どのサーバーにどのデータを書き込むかを定義し、各シャードで直接書き込みを実行できます。 つまり、分散テーブルのテーブルにinsertを実行します “looks at”. これは、主題領域の要件のために自明ではないシャーディングスキームを使用できるため、最も柔軟なソリューションです。 これも最適なソリューションからデータを書き込むことができるの異なる資料が完全に独立。

次に、分散テーブルでinsertを実行できます。 この場合、テーブルは挿入されたデータをサーバー自体に分散します。 分散テーブルに書き込むには、シャーディングキーセット（最後のパラメータ）が必要です。 さらに、単一のシャードしかない場合、書き込み操作はシャーディングキーを指定せずに動作します。

各シャードは設定ファイルで定義された重みを持つことができます。 デフォルトでは、重みは一つに等しいです。 データは、シャードウェイトに比例した量でシャード全体に分散されます。 たとえば、二つのシャードがあり、最初のものが9の重みを持ち、第二のものが10の重みを持つ場合、最初のシャードは9/19の行に送られ、第二のものは10/19

各破片は持つことができます ‘internal\_replication’ 設定ファイルで定義されたパラメータ。

このパラメータが設定されている場合 ‘true’ 書き込み操作は、最初の正常なレプリカを選択し、それにデータを書き込みます。 分散テーブルの場合は、この代替を使用します “looks at” 複製されたテーブル。 言い換えれば、データが書き込まれるテーブルがそれ自体を複製する場合です。

に設定されている場合 ‘false’ データはすべてのレプリカに書き込まれます。 本質的に、これは、分散テーブルがデータ自体を複製することを意味します。 レプリカの整合性はチェックされず、時間の経過とともにわずかに異なるデータが含まれるためです。

データの行が送信されるシャードを選択するには、シャーディング式が分析され、残りの部分がシャードの合計ウェイトで除算されます。 行は、残りの半分の間隔に対応するシャードに送られます ‘prev\_weight’ に ‘prev\_weights + weight’、どこ ‘prev\_weights’ 最小の数を持つシャードの合計重量です。 ‘weight’ このシャードの重さです。 たとえば、二つのシャードがあり、最初のシャードの重みが9で、二番目のシャードの重みが10である場合、行は\[0,9)の範囲から残りのシャードの最初のシャー

シャーディング式には、整数を返す定数とテーブル列からの任意の式を指定できます。 たとえば、次の式を使用できます ‘rand()’ データのランダムな分布の場合、または ‘UserID’ ユーザーのIDを分割する残りの部分で配布する場合（単一のユーザーのデータは単一のシャードに存在し、ユーザーの実行と参加が簡単になります）。 列のいずれかが十分に均等に分散されていない場合は、ハッシュ関数でラップすることができます：intHash64（UserID）。

簡単なリマインダからの限定シshardingんを常に適しています。 中規模および大量のデータ（数十のサーバー）では機能しますが、非常に大量のデータ（数百のサーバー以上）では機能しません。 後者の場合はshardingスキームに必要なのではなく、エントリに配布します。

SELECT queries are sent to all the shards and work regardless of how data is distributed across the shards (they can be distributed completely randomly). When you add a new shard, you don’t have to transfer the old data to it. You can write new data with a heavier weight – the data will be distributed slightly unevenly, but queries will work correctly and efficiently.

次の場合、シャーディングスキームについて心配する必要があります:

-   特定のキーによるデータの結合(inまたはjoin)が必要なクエリが使用されます。 このキーによってデータがシャードされている場合は、グローバルinまたはグローバル結合の代わりにローカルinまたはjoinを使用できます。
-   多数のサーバー（数百またはそれ以上）が使用され、多数の小さなクエリ（個々のクライアントのクエリ-ウェブサイト、広告主、またはパートナー）が使用されます。 小さなクエリがクラスタ全体に影響を与えないようにするには、単一のクライアントのデータを単一のシャードに配置することが理にかなっていま また、我々はyandexの中でやったように。metricaでは、biレベルのシャーディングを設定できます。 “layers” レイヤーが複数のシャードで構成されている場合。 単一のクライアントのデータは単一のレイヤーに配置されますが、必要に応じてシャードをレイヤーに追加することができ、データはその中にランダムに配 分散テーブルはレイヤごとに作成され、グローバルクエリ用に単一の共有分散テーブルが作成されます。

データは非同期に書き込まれます。 テーブルに挿入すると、データブロックはローカルファイルシステムに書き込まれます。 データはできるだけ早くバックグラウンドでリモートサーバーに送信されます。 データを送信する期間は、以下によって管理されます。 [distributed\_directory\_monitor\_sleep\_time\_ms](../../../operations/settings/settings.md#distributed_directory_monitor_sleep_time_ms) と [distributed\_directory\_monitor\_max\_sleep\_time\_ms](../../../operations/settings/settings.md#distributed_directory_monitor_max_sleep_time_ms) 設定。 その `Distributed` エンジンを送信し、各ファイルを挿入したデータが別々にまでを一括送信ファイルの [distributed\_directory\_monitor\_batch\_inserts](../../../operations/settings/settings.md#distributed_directory_monitor_batch_inserts) 設定。 この設定の改善にクラスターの性能をより一層の活用地域のサーバやネットワーク資源です。 を確認しておきましょうか否かのデータが正常に送信されるチェックリストファイル(データまたは間に-をはさんだ)はテーブルディレクトリ: `/var/lib/clickhouse/data/database/table/`.

分散テーブルへの挿入後にサーバーが存在しなくなった場合、または大まかな再起動(デバイス障害など)が発生した場合は、挿入されたデータが失われる可 破損したデータ部分がテーブルディレクトリで検出された場合、そのデータ部分は、 ‘broken’ サブディレクトリと、もはや使用。

Max\_parallel\_replicasオプションを有効にすると、単一のシャード内のすべてのレプリカでクエリ処理が並列化されます。 詳細については、以下を参照してください [max\_parallel\_replicas](../../../operations/settings/settings.md#settings-max_parallel_replicas).

## 仮想列 {#virtual-columns}

-   `_shard_num` — Contains the `shard_num` （から `system.clusters`). タイプ: [UInt32](../../../sql-reference/data-types/int-uint.md).

!!! note "メモ"
    それ以来 [`remote`](../../../sql-reference/table-functions/remote.md)/`cluster` テーブル機能の内部を一時のインスタンスと同じ分散型エンジン, `_shard_num` あまりにもそこに利用可能です。

**また見なさい**

-   [仮想列](index.md#table_engines-virtual_columns)

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/distributed/) <!--hide-->
