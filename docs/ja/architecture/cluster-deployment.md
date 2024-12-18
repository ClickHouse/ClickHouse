---
slug: /ja/architecture/cluster-deployment
sidebar_label: クラスターデプロイ
sidebar_position: 100
title: クラスターデプロイ
---

このチュートリアルでは、すでに[ローカルの ClickHouse サーバー](../getting-started/install.md)をセットアップしていることを前提としています。

このチュートリアルを通じて、シンプルな ClickHouse クラスターのセットアップ方法を学びます。小規模ですが、フォールトトレラントでスケーラブルです。その後、例のデータセットの1つを使用してデータを充填し、デモクエリを実行します。

## クラスターデプロイ {#cluster-deployment}

この ClickHouse クラスターは同質クラスターになります。以下の手順に従ってください:

1. クラスターのすべてのマシンに ClickHouse サーバーをインストールする
2. 設定ファイルにクラスターのコンフィグを設定する
3. 各インスタンスにローカルテーブルを作成する
4. [分散テーブル](../engines/table-engines/special/distributed.md)を作成する

[分散テーブル](../engines/table-engines/special/distributed.md)は、ClickHouse クラスター内のローカルテーブルへの「ビュー」の一種です。分散テーブルからの SELECT クエリは、クラスターのすべてのシャードのリソースを使用して実行されます。複数のクラスターに対して設定を指定し、それに応じて異なるクラスターにビューを提供するために複数の分散テーブルを作成できます。

以下は、3つのシャードとそれぞれ1つのレプリカを持つクラスターのコンフィグ例です:

```xml
<remote_servers>
    <perftest_3shards_1replicas>
        <shard>
            <replica>
                <host>example-perftest01j.clickhouse.com</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest02j.clickhouse.com</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest03j.clickhouse.com</host>
                <port>9000</port>
            </replica>
        </shard>
    </perftest_3shards_1replicas>
</remote_servers>
```

さらなるデモンストレーションのために、単一ノードデプロイメントチュートリアルで使用した `hits_v1` の `CREATE TABLE` クエリと同じクエリを使用して、新しいローカルテーブルを作成しますが、異なるテーブル名にします:

```sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

分散テーブルを作成すると、クラスターのローカルテーブルに対するビューを提供します:

```sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

一般的な方法として、クラスターのすべてのマシンに同様の分散テーブルを作成します。これにより、クラスターの任意のマシンで分散クエリを実行できます。また、[remote](../sql-reference/table-functions/remote.md) テーブル関数を使用して、特定の SELECT クエリに対する一時的な分散テーブルを作成するオプションもあります。

分散テーブルに対して [INSERT SELECT](../sql-reference/statements/insert-into.md) を実行し、テーブルを複数のサーバーに分散させてみましょう。

```sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

予想通り、計算量の多いクエリは1台のサーバーを使用するよりも3台のサーバーを使用することでN倍速く実行されます。

この場合、3つのシャードを持つクラスターを使用し、各シャードには1つのレプリカがあります。

本番環境での回復力を提供するために、各シャードには複数の可用性ゾーンまたはデータセンター（または少なくともラック）に広がる2〜3のレプリカを含めることをお勧めします。 ClickHouse は無制限の数のレプリカをサポートしていることに注意してください。

以下は、1つのシャードに3つのレプリカを含むクラスターのコンフィグ例です:

```xml
<remote_servers>
    ...
    <perftest_1shards_3replicas>
        <shard>
            <replica>
                <host>example-perftest01j.clickhouse.com</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest02j.clickhouse.com</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest03j.clickhouse.com</host>
                <port>9000</port>
             </replica>
        </shard>
    </perftest_1shards_3replicas>
</remote_servers>
```

ネイティブなレプリケーションを有効にするには、[ZooKeeper](http://zookeeper.apache.org/) が必要です。ClickHouse はすべてのレプリカのデータ整合性を保証し、障害後自動的に復元手続きを実行します。ZooKeeper クラスターを他のプロセス（ClickHouse を含む）が実行されていない別のサーバーにデプロイすることをお勧めします。

:::note Note
ZooKeeper は厳密には必須ではありません: 一部の簡単なケースでは、アプリケーションコードからすべてのレプリカにデータを書き込むことでデータを複製できます。このアプローチは**推奨されません**。なぜなら、この場合、ClickHouse はすべてのレプリカでのデータ整合性を保証できません。このため、データ整合性の責任はアプリケーションにあります。
:::

ZooKeeper の場所は設定ファイルで指定されます:

```xml
<zookeeper>
    <node>
        <host>zoo01.clickhouse.com</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo02.clickhouse.com</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo03.clickhouse.com</host>
        <port>2181</port>
    </node>
</zookeeper>
```

また、テーブル作成時に使用されるシャードとレプリカを識別するためのマクロを設定する必要があります:

```xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

レプリケートされたテーブルの作成時にレプリカが存在しない場合、新しい最初のレプリカがインスタンス化されます。すでにライブレプリカがある場合、新しいレプリカは既存のものからデータをクローンします。すべてのレプリケートされたテーブルを先に作成してからデータを挿入するか、あるいはデータ挿入中または後に他のレプリカを追加するかのいずれかのオプションがあります。

```sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplicatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

ここでは、[ReplicatedMergeTree](../engines/table-engines/mergetree-family/replication.md) テーブルエンジンを使用しています。パラメーターでは、シャードとレプリカ識別子を含む ZooKeeper パスを指定します。

```sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

レプリケーションはマルチマスターモードで動作します。データは任意のレプリカにロードでき、システムは他のインスタンスと自動的に同期します。レプリケーションは非同期であるため、ある時点で、すべてのレプリカが最近挿入されたデータを含むとは限りません。ただし、データのインゲストを許可するには、少なくとも1つのレプリカが稼働している必要があります。その他のレプリカは再アクティブ化するとデータを同期し、一貫性を修復します。この方法により、最近挿入されたデータが失われる可能性が低いという利点があります。
