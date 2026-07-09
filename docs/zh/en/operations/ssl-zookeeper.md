---
description: '配置 ClickHouse
  与 ZooKeeper 之间安全 SSL/TLS 通信的指南'
sidebar_label: '与 ZooKeeper 的安全通信'
sidebar_position: 45
slug: /operations/ssl-zookeeper
title: 'ClickHouse 与 ZooKeeper 之间的可选安全通信'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

你需要为通过 SSL 与 ClickHouse 客户端通信指定 `ssl.keyStore.location`、`ssl.keyStore.password` 以及 `ssl.trustStore.location`、`ssl.trustStore.password`。这些选项从 Zookeeper 3.5.2 版本开始提供。

你可以将 `zookeeper.crt` 添加到受信任证书列表中。

```bash
sudo cp zookeeper.crt /usr/local/share/ca-certificates/zookeeper.crt
sudo update-ca-certificates
```

`config.xml` 中的 Client 部分如下：

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

在 ClickHouse 配置中添加 Zookeeper，并设置一些集群和宏：

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

启动 `clickhouse-server`。你应在日志中看到：

```text
<Trace> ZooKeeper: initialized, hosts: secure://localhost:2281
```

前缀 `secure://` 表示该连接受 SSL 保护。

为确认流量已加密，请在安全端口上运行 `tcpdump`：

```bash
tcpdump -i any dst port 2281 -nnXS
```

然后在 `clickhouse-client` 中执行查询：

```sql
SELECT * FROM system.zookeeper WHERE path = '/';
```

在未加密连接中，你会在 `tcpdump` 输出中看到类似下面的内容：

```text
..../zookeeper/quota.
```

使用加密连接时，你不应看到这个。