---
sidebar_label: "Distributed"
sidebar_position: 10
slug: /ja/engines/table-engines/special/distributed
---

# Distributed テーブルエンジン

:::warning
クラウドでDistributedテーブルエンジンを作成するには、[remoteとremoteSecure](../../../sql-reference/table-functions/remote)テーブル関数を使用できます。ClickHouse Cloudでは、`Distributed(...)`の構文は使用できません。
:::

Distributedエンジンを使用したテーブルは独自のデータを保存せず、複数のサーバーでの分散型クエリ処理を可能にします。読み込みは自動的に並列化されます。読み込み時には、リモートサーバー上のテーブルインデックスが利用されます（存在する場合）。

## テーブルの作成 {#distributed-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]])
[SETTINGS name=value, ...]
```

### テーブルから {#distributed-from-a-table}

`Distributed`テーブルが現在のサーバー上のテーブルを指している場合、そのテーブルのスキーマを採用できます:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster] AS [db2.]name2 ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]]) [SETTINGS name=value, ...]
```

### Distributedパラメータ

#### cluster

`cluster` - サーバーの設定ファイル内のクラスタ名

#### database

`database` - リモートデータベースの名前

#### table

`table` - リモートテーブルの名前

#### sharding_key

`sharding_key` - （省略可能）シャーディングキー

`sharding_key`を指定することが必要な場合:

- Distributed テーブルへの`INSERT`（テーブルエンジンがデータをどのように分割するかを決定するために`sharding_key`が必要）。ただし、`insert_distributed_one_random_shard`設定が有効になっている場合、`INSERT`にはシャーディングキーは必要ありません。
- `optimize_skip_unused_shards`を使用するために、クエリすべきシャードを決定するために`sharding_key`が必要です

#### policy_name

`policy_name` - （省略可能）ポリシー名、一時ファイルをバックグラウンド送信用に保存するために使用されます

**関連リンク**

 - [distributed_foreground_insert](../../../operations/settings/settings.md#distributed_foreground_insert) 設定
 - [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) の例

### Distributed設定

#### fsync_after_insert

`fsync_after_insert` - バックグラウンドでのDistributedへの挿入後にファイルデータに対して`fsync`を実行します。**イニシエーターノード**のディスクに挿入されたデータ全体がOSによりフラッシュされることを保証します。

#### fsync_directories

`fsync_directories` - ディレクトリに対して`fsync`を実行します。Distributedテーブルに関連するバックグラウンド挿入操作後（挿入後、シャードへのデータ送信後など）にディレクトリメタデータがOSにより更新されることを保証します。

#### skip_unavailable_shards

`skip_unavailable_shards` - trueの場合、ClickHouseは使用不可のシャードを黙ってスキップします。シャードは次のいずれかの理由で使用不可とマークされます: 1) 接続失敗によりシャードに到達できません。2) シャードがDNS経由で解決不可能です。3) シャードにテーブルが存在しません。デフォルトはfalseです。

#### bytes_to_throw_insert

`bytes_to_throw_insert` - バックグラウンドINSERTのために保留される圧縮バイト数がこの数を超えると例外がスローされます。0 - 例外をスローしません。デフォルトは0です。

#### bytes_to_delay_insert

`bytes_to_delay_insert` - バックグラウンドINSERTのために保留される圧縮バイト数がこの数を超えるとクエリが遅延します。0 - 遅延しません。デフォルトは0です。

#### max_delay_to_insert

`max_delay_to_insert` - バックグラウンド送信のために多くの保留バイトがある場合、Distributedテーブルにデータを挿入する最大遅延時間（秒単位）。デフォルトは60です。

#### background_insert_batch

`background_insert_batch` - [distributed_background_insert_batch](../../../operations/settings/settings.md#distributed_background_insert_batch) と同じ

#### background_insert_split_batch_on_failure

`background_insert_split_batch_on_failure` - [distributed_background_insert_split_batch_on_failure](../../../operations/settings/settings.md#distributed_background_insert_split_batch_on_failure) と同じ

#### background_insert_sleep_time_ms

`background_insert_sleep_time_ms` - [distributed_background_insert_sleep_time_ms](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms) と同じ

#### background_insert_max_sleep_time_ms

`background_insert_max_sleep_time_ms` - [distributed_background_insert_max_sleep_time_ms](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms) と同じ

#### flush_on_detach

`flush_on_detach` - DETACH/DROP/サーバーシャットダウン時にリモートノードにデータをフラッシュします。デフォルトはtrueです。

:::note
**耐久性の設定** (`fsync_...`):

- バックグラウンドINSERT（つまり、`distributed_foreground_insert=false`）にのみ影響を与え、データが最初にイニシエーターノードのディスクに保存され、後でバックグラウンドでシャードに送信されます。
- 挿入操作のパフォーマンスを大幅に低下させる可能性がある
- Distributedテーブルフォルダ内に保存されたデータを、挿入を受け入れた**ノード**で書き出すことに影響を与えます。基盤となるMergeTreeテーブルへのデータ書き込みの保証が必要な場合 – `system.merge_tree_settings`内の耐久性の設定（`...fsync...`）を参照してください。

**挿入制限の設定** (`..._insert`) については、次も参照してください:

- [distributed_foreground_insert](../../../operations/settings/settings.md#distributed_foreground_insert) 設定
- [prefer_localhost_replica](../../../operations/settings/settings.md#prefer-localhost-replica) 設定
- `bytes_to_throw_insert`は`bytes_to_delay_insert`よりも前に処理されるため、`bytes_to_delay_insert`未満の値に設定しないようにしてください。
:::

**例**

``` sql
CREATE TABLE hits_all AS hits
ENGINE = Distributed(logs, default, hits[, sharding_key[, policy_name]])
SETTINGS
    fsync_after_insert=0,
    fsync_directories=0;
```

データは`logs`クラスタ内にあるすべてのサーバーからリモートにある`default.hits`テーブルから読み取られます。データは読むだけでなく可能な限りリモートサーバーで部分的に処理されます。たとえば、`GROUP BY`を使ったクエリでは、データがリモートサーバーで集約され、集計関数の中間状態がリクエスト側のサーバーに送信されます。その後データはさらに集約されます。

データベース名の代わりに、文字列を返す定数式を使用できます。例えば：`currentDatabase()`。

## クラスター {#distributed-clusters}

クラスターは[サーバー構成ファイル](../../../operations/configuration-files.md) で設定されます:

``` xml
<remote_servers>
    <logs>
        <!-- 分散クエリ用のサーバー間クラスタ秘匿
             デフォルト: 秘匿なし（認証は行われません）

             設定されている場合、分散クエリはシャード上で検証されるため少なくとも:
             - シャードにこのクラスターが存在する必要があります
             - このクラスターに同じ秘匿が必要です。

             また（そしてこれはより重要ですが）、initial_userがクエリの現在のユーザーとして使用されます。
        -->
        <!-- <secret></secret> -->
        
        <!-- オプション: このクラスタに対する分散DDLクエリ（ON CLUSTER句）が許可されるかどうか。デフォルト: true（許可される）。 -->
        <!-- <allow_distributed_ddl_queries>true</allow_distributed_ddl_queries> -->
        
        <shard>
            <!-- オプション: データ書き込み時のシャードの重み。デフォルト: 1。 -->
            <weight>1</weight>
            <!-- オプション: データを各レプリカのみに書き込むかどうか。デフォルト: false(全レプリカにデータを書き込む)。 -->
            <internal_replication>false</internal_replication>
            <replica>
                <!-- オプション: 負荷分散（load_balancing設定も参照）中のレプリカの優先順位。デフォルト: 1（小さいほど優先度が高い）。 -->
                <priority>1</priority>
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

ここでは、`logs`という名前のクラスターが定義されており、2つのシャードで構成され、それぞれが2つのレプリカを持っています。シャードはデータの異なる部分を含むサーバーを指します（すべてのデータを読むにはすべてのシャードにアクセスする必要があります）。レプリカはサーバーを複製しています（すべてのデータを読むには、任意のレプリカのデータにアクセスすることができます）。

クラスター名にはドットを含めることはできません。

各サーバーには、`host`、`port`、およびオプションで`user`、`password`、`secure`、`compression`のパラメーターを指定します:

- `host` – リモートサーバーのアドレス。ドメインかIPv4またはIPv6のアドレスを使用できます。ドメインを指定した場合、サーバーは起動時にDNSリクエストを行い、その結果をサーバーが稼働している間は保持します。DNSリクエストが失敗した場合、サーバーは起動しません。DNSレコードを変更した場合は、サーバーを再起動してください。
- `port` – メッセンジャー活動用のTCPポート（設定の`tcp_port`、通常は9000に設定）。`http_port`と混同しないでください。
- `user` – リモートサーバーに接続するためのユーザー名。デフォルト値は`default`ユーザー。このユーザーは指定されたサーバーに接続するためのアクセス権を持っている必要があります。アクセスは`users.xml`ファイルで設定されます。詳細は[アクセス権](../../../guides/sre/user-management/index.md)セクションを参照してください。
- `password` – リモートサーバーへの接続のためのパスワード（マスクされていない）。デフォルト値: 空文字列。
- `secure` - セキュアなSSL/TLS接続を使用するかどうか。通常、ポートを指定する必要もあります（デフォルトのセキュアポートは`9440`）。サーバーは`<tcp_port_secure>9440</tcp_port_secure>`をリッスンし、正しい証明書で構成される必要があります。
- `compression` - データ圧縮を使用するかどうか。デフォルト値: `true`。

レプリカを指定する場合、読み込み時には使用可能なレプリカの中から各シャードに対して1つのレプリカが選択されます。負荷分散（どのレプリカにアクセスするかの優先順位）のアルゴリズムを設定できます – [load_balancing](../../../operations/settings/settings.md#load_balancing)設定を参照してください。サーバーとの接続が確立されなかった場合、短いタイムアウトで接続の試行が行われます。接続が失敗した場合、次のレプリカが選択され、それをすべてのレプリカに対して繰り返します。すべてのレプリカで接続試行が失敗した場合、同じ方法で複数回試行します。これは回復性に寄与しますが、完全なフォールトトレランスを提供するわけではありません：リモートサーバーは接続を受け入れるが、機能しないか、または正常に機能しません。

1つのシャードのみを指定することも（この場合、クエリ処理はリモートと呼ばれるべきで、分散ではない）や任意数のシャードまで指定することができます。各シャードには1つから任意数のレプリカを指定できます。各シャードに異なる数のレプリカを指定することもできます。

任意の数のクラスターを構成に指定することができます。

あなたのクラスターを表示するには、`system.clusters`テーブルを使用してください。

`Distributed`エンジンはクラスターをローカルサーバーのように操作することを可能にします。ただし、クラスターの構成はサーバー構成ファイルで動的に指定することはできません。通常、クラスター内のすべてのサーバーは同じクラスター構成を持っているが（必須ではない）、構成ファイルからクラスターはサーバーを再起動せずに動的に更新されます。

未知のシャードやレプリカのセットに毎回クエリを送信する必要がある場合、`Distributed`テーブルを作成する必要はありません - `remote`テーブル関数を使用します。セクション[テーブル関数](../../../sql-reference/table-functions/index.md)を参照してください。

## データの書き込み {#distributed-writing-data}

クラスターにデータを記録する方法には二つあります:

まず、どのサーバーにどのデータを書き込むかを定義し、各シャードに直接書き込みを行うことができます。言い換えれば、Distributedテーブルが指すクラスター内のリモートテーブルに直接`INSERT`ステートメントを実行します。これは最も柔軟な解決策であり、非自明なシャーディングスキームでも使用可能です。また、この解決策は最も最適であり、データは完全に独立して異なるシャードに書き込むことができます。

第二に、`Distributed`テーブルに`INSERT`ステートメントを実行することができます。この場合、テーブルは挿入されたデータをサーバーに自分で分配します。`Distributed`テーブルに書き込むためには、`sharding_key`パラメータが設定されている必要があります（シャードが一つしかない場合は除く）。

各シャードには構成ファイルで`<weight>`を定義できます。デフォルトでは重さは`1`です。データはシャードの重さに比例して分配されます。すべてのシャードの重みを足し、その後各シャードの重みを合計で割り、それぞれのシャードの割合を決めます。例えば、二つのシャードがあり、最初のシャードの重みが1で、二番目のシャードの重みが2の場合、最初のシャードには挿入行の三分の一（1 / 3）が送信され、二番目のシャードには三分の二（2 / 3）が送信されます。

各シャードには構成ファイルで`internal_replication`パラメータが定義できます。このパラメータが`true`に設定されている場合、書き込み操作は最初の健全なレプリカを選択し、データを書き込みます。`Distributed`テーブルを基にしているテーブルがレプリケートされたテーブル（例:`Replicated*MergeTree`テーブルエンジン）である場合に使用します。テーブルレプリカの一つが書き込みを受け取り、自動的に他のレプリカにレプリケートされます。

`internal_replication`が`false`に設定されている場合（デフォルト）、データはすべてのレプリカに書き込まれます。この場合、`Distributed`テーブル自身がデータをレプリケートします。これはレプリケートされたテーブルを使用することよりも劣ります。なぜなら、レプリカの整合性が確認されず、時間と共にわずかに異なるデータを含むことになるからです。

データの行を送信するシャードを選択するために、シャーディング式が分析され、その余りはシャードの総重みで割ります。行は、`prev_weights`から`prev_weights + weight`までの余りの半開区間に対応するシャードに送られます。ここで、`prev_weights`は最小数のシャードの総重みで、`weight`はこのシャードの重みです。例えば、二つのシャードがあり、最初のシャードの重みが9で、二番目のシャードの重みが10の場合、行は余りが範囲\[0, 9)のとき最初のシャードに送信され、\[9, 19)のとき二番目のシャードに送信されます。

シャーディング式は、整数を返す定数およびテーブル列から構成される任意の式であり得ます。例えば、データのランダムな分配には`rand()`を、ユーザーのIDでの分配には`UserID`を使うことができます（この場合、単一のユーザーのデータが単一のシャードに存在するので、`IN`や`JOIN`をユーザーで実行するのが簡単です）。いずれかの列が十分に均等に分散されていない場合、ハッシュ関数でラップすることもできます（例: `intHash64(UserID)`）。

単純な除算の余りはシャーディングにとって制限された解決策であり、常に適切であるわけではありません。中規模および大規模のデータ（数十台のサーバー）には機能しますが、非常に大きなデータ量（数百台以上のサーバー）には機能しません。後者の場合、`Distributed`テーブル内のエントリを使用するのではなく、対象領域が求めるシャーディングスキームを使用します。

次の場合にはシャーディングスキームを検討すべきです:

- 特定のキーでのデータの結合（`IN`または`JOIN`）を要求するクエリが使用される場合。このキーでデータがシャーディングされている場合、`GLOBAL IN`または`GLOBAL JOIN`を使用することなく、ローカル`IN`または`JOIN`を使用することができ、はるかに効率的です。
- 大量のサーバーが使用され（数百台以上）、少量のクエリが存在する場合（例えば個々のクライアントのデータのクエリ）。小さなクエリがクラスター全体に影響を与えないようにするため、一つのクライアントのデータを一つのシャードに配置することに意味があります。または、階層シャーディングを設定します。クラスター全体を「レイヤー」に分割し、レイヤーは複数のシャードで構成され、単一のクライアントのデータが一つのレイヤーに配置されますが、必要に応じてシャードをレイヤーに追加し、データはランダムに分配されます。各レイヤーのために`Distributed`テーブルを作成し、グローバルクエリ用の単一の共有Distributed テーブルを作成します。

データはバックグラウンドで書き込まれます。テーブルに挿入されると、データブロックはローカルファイルシステムにただ書き込まれます。データは可能な限りすぐにリモートサーバーにバックグラウンドで送信されます。データを送信する周期性は、[distributed_background_insert_sleep_time_ms](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms) および [distributed_background_insert_max_sleep_time_ms](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms) 設定によって管理されます。`Distributed`エンジンは、挿入されたデータを分離して各ファイルを送信しますが、[distributed_background_insert_batch](../../../operations/settings/settings.md#distributed_background_insert_batch)設定を有効にしてファイルをバッチで送信することができます。この設定により、ローカルサーバーやネットワークリソースの利用を最適化し、クラスタのパフォーマンスを向上させます。データが正常に送信されたかどうかを確認するには、テーブルディレクトリにあるファイル（送信待ちのデータ）をチェックする必要があります：`/var/lib/clickhouse/data/database/table/`。バックグラウンドタスクを実行するスレッドの数は、[background_distributed_schedule_pool_size](../../../operations/settings/settings.md#background_distributed_schedule_pool_size)設定で設定できます。

サーバーが消失したり、`Distributed`テーブルへの`INSERT`の後でラフな再起動をした場合（例えばハードウェアの損傷による）、挿入されたデータは失われる可能性があります。テーブルディレクトリで損傷したデータ部分が検出された場合、そのデータは`broken`サブディレクトリに転送され、もう使用されません。

## データの読み取り {#distributed-reading-data}

`Distributed`テーブルにクエリを投げると、`SELECT`クエリがすべてのシャードに送信され、データがシャード間で完全にランダムに分散している場合も機能します。新しいシャードを追加する際には、古いデータをそこに移動する必要はありません。代わりに、より高い重みで新しいデータを書き込むことができます。データがわずかに不均等に分布されますが、クエリは正しく効率的に動作します。

`max_parallel_replicas`オプションが有効になっている場合、クエリ処理は同一シャード内のすべてのレプリカにまたがって並行化されます。詳細は[こちら](../../../operations/settings/settings.md#max_parallel_replicas)のセクションを参照してください。

Distributed `in`クエリおよび`global in`クエリの処理方法について学ぶには、[こちら](../../../sql-reference/operators/in.md#select-distributed-subqueries)のドキュメントを参照してください。

## 仮想カラム {#virtual-columns}

#### _shard_num

`_shard_num` — テーブル`system.clusters`の`shard_num`値が格納される。タイプ: [UInt32](../../../sql-reference/data-types/int-uint.md)。

:::note
[remote](../../../sql-reference/table-functions/remote.md)および[cluster](../../../sql-reference/table-functions/cluster.md)テーブル関数は内部的に一時的にDistributedテーブルを作成するため、`_shard_num`はそこでも利用可能です。
:::

**関連リンク**

- [仮想カラム](../../../engines/table-engines/index.md#table_engines-virtual_columns) の説明
- [background_distributed_schedule_pool_size](../../../operations/settings/settings.md#background_distributed_schedule_pool_size) 設定
- [shardNum()](../../../sql-reference/functions/other-functions.md#shardnum)および[shardCount()](../../../sql-reference/functions/other-functions.md#shardcount)関数
