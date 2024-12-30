---
slug: /ja/operations/ssl-zookeeper
sidebar_position: 45
sidebar_label: Zookeeperとの安全な通信
---

# ClickHouseとZookeeper間のオプションの安全な通信
import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_automated.md';

<SelfManaged />

ClickHouse クライアントとのSSL通信用に `ssl.keyStore.location`、`ssl.keyStore.password`、`ssl.trustStore.location`、`ssl.trustStore.password` を指定する必要があります。これらのオプションはZookeeperバージョン3.5.2から利用可能です。

`zookeeper.crt`を信頼された証明書に追加できます。

``` bash
sudo cp zookeeper.crt /usr/local/share/ca-certificates/zookeeper.crt
sudo update-ca-certificates
```

`config.xml`のクライアントセクションは以下のようになります:

``` xml
<client>
    <certificateFile>/etc/clickhouse-server/client.crt</certificateFile>
    <privateKeyFile>/etc/clickhouse-server/client.key</privateKeyFile>
    <loadDefaultCAFile>true</loadDefaultCAFile>
    <cacheSessions>true</cacheSessions>
    <disableProtocols>sslv2,sslv3</disableProtocols>
    <preferServerCiphers>true</preferServerCiphers>
    <invalidCertificateHandler>
        <name>RejectCertificateHandler</name>
    </invalidCertificateHandler>
</client>
```

ClickHouseの設定にZookeeperを、いくつかのクラスタとマクロと共に追加します:

``` xml
<clickhouse>
    <zookeeper>
        <node>
            <host>localhost</host>
            <port>2281</port>
            <secure>1</secure>
        </node>
    </zookeeper>
</clickhouse>
```

`clickhouse-server`を起動します。ログには以下のように出力されるはずです:

```text
<Trace> ZooKeeper: initialized, hosts: secure://localhost:2281
```

プレフィックス `secure://` は接続がSSLで保護されていることを示しています。

トラフィックが暗号化されていることを確認するために、セキュアポートで`tcpdump`を実行します:

```bash
tcpdump -i any dst port 2281 -nnXS
```

`clickhouse-client`でクエリを実行します:

```sql
SELECT * FROM system.zookeeper WHERE path = '/';
```

暗号化されていない接続では、`tcpdump`の出力に次のようなものが表示されます:

```text
..../zookeeper/quota.
```

暗号化された接続ではこれが表示されません。
