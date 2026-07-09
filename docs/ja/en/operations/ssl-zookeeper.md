---
description: 'ClickHouse
  と ZooKeeper 間の安全な SSL/TLS 通信を設定するためのガイド'
sidebar_label: 'ZooKeeper との安全な通信'
sidebar_position: 45
slug: /operations/ssl-zookeeper
title: 'ClickHouse と ZooKeeper 間のオプションの安全な通信'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

SSL 経由で ClickHouse client と通信するには、`ssl.keyStore.location`、`ssl.keyStore.password`、`ssl.trustStore.location`、`ssl.trustStore.password` を指定する必要があります。これらのオプションは ZooKeeper バージョン 3.5.2 以降で利用できます。

信頼済み証明書に `zookeeper.crt` を追加できます。

```bash
sudo cp zookeeper.crt /usr/local/share/ca-certificates/zookeeper.crt
sudo update-ca-certificates
```

`config.xml` の Client セクションは次のようになります。

```xml
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

いくつかのクラスター設定とマクロを含めて、ClickHouse の設定に Zookeeper を追加します：

```xml
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

`clickhouse-server` を起動します。ログには次のように表示されるはずです。

```text
<Trace> ZooKeeper: initialized, hosts: secure://localhost:2281
```

プレフィックス `secure://` は、接続が SSL で保護されていることを示します。

トラフィックが暗号化されていることを確認するには、SSL で保護されたポートで `tcpdump` を実行します：

```bash
tcpdump -i any dst port 2281 -nnXS
```

次に、`clickhouse-client` でクエリを実行します:

```sql
SELECT * FROM system.zookeeper WHERE path = '/';
```

暗号化されていない接続では、`tcpdump` の出力に次のように表示されます:

```text
..../zookeeper/quota.
```

暗号化接続では、これは表示されないはずです。