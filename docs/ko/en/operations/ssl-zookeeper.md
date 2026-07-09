---
description: 'ClickHouse와 ZooKeeper 간의 보안 SSL/TLS 통신 구성 가이드'
sidebar_label: 'ZooKeeper와의 보안 통신'
sidebar_position: 45
slug: /operations/ssl-zookeeper
title: 'ClickHouse와 ZooKeeper 간의 선택적 보안 통신'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

SSL을 통해 ClickHouse client와 통신하려면 `ssl.keyStore.location`, `ssl.keyStore.password` 및 `ssl.trustStore.location`, `ssl.trustStore.password`를 지정해야 합니다. 이러한 옵션은 ZooKeeper 버전 3.5.2부터 사용할 수 있습니다.

신뢰할 수 있는 인증서에 `zookeeper.crt`를 추가할 수 있습니다.

```bash
sudo cp zookeeper.crt /usr/local/share/ca-certificates/zookeeper.crt
sudo update-ca-certificates
```

`config.xml`의 Client 섹션은 다음과 같습니다:

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

일부 cluster 및 macros를 사용해 ClickHouse 구성에 ZooKeeper를 추가합니다:

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

`clickhouse-server`를 실행하십시오. 로그에 다음 내용이 표시됩니다:

```text
<Trace> ZooKeeper: initialized, hosts: secure://localhost:2281
```

접두사 `secure://`는 연결에 SSL 보안이 적용되었음을 나타냅니다.

트래픽이 암호화되는지 확인하려면 보안이 적용된 포트에서 `tcpdump`를 실행하십시오:

```bash
tcpdump -i any dst port 2281 -nnXS
```

그리고 `clickhouse-client`에서 쿼리를 실행합니다:

```sql
SELECT * FROM system.zookeeper WHERE path = '/';
```

암호화되지 않은 연결에서는 `tcpdump` 출력에 다음과 같은 내용이 표시됩니다:

```text
..../zookeeper/quota.
```

암호화된 연결에서는 이 내용이 보이지 않아야 합니다.