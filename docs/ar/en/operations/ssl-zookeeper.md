---
description: 'دليل لإعداد اتصال SSL/TLS آمن بين ClickHouse
  وZooKeeper'
sidebar_label: 'اتصال آمن مع ZooKeeper'
sidebar_position: 45
slug: /operations/ssl-zookeeper
title: 'اتصال آمن اختياري بين ClickHouse وZooKeeper'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

يجب تحديد `ssl.keyStore.location` و`ssl.keyStore.password` و`ssl.trustStore.location` و`ssl.trustStore.password` للاتصال مع clickhouse client عبر SSL. هذه الخيارات متاحة بدءًا من الإصدار 3.5.2 من Zookeeper.

يمكنك إضافة `zookeeper.crt` إلى الشهادات الموثوق بها.

```bash
sudo cp zookeeper.crt /usr/local/share/ca-certificates/zookeeper.crt
sudo update-ca-certificates
```

سيكون قسم Client في `config.xml` على النحو التالي:

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

أضف Zookeeper إلى إعدادات ClickHouse مع بعض إعدادات الـ cluster والـ macros:

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

شغّل `clickhouse-server`. في السجلات، ينبغي أن ترى:

```text
<Trace> ZooKeeper: initialized, hosts: secure://localhost:2281
```

تشير البادئة `secure://` إلى أن الاتصال مؤمَّن عبر SSL.

للتأكد من أن حركة البيانات مشفَّرة، شغّل `tcpdump` على المنفذ المؤمَّن:

```bash
tcpdump -i any dst port 2281 -nnXS
```

ثم نفِّذ استعلامًا في `clickhouse-client`:

```sql
SELECT * FROM system.zookeeper WHERE path = '/';
```

عند استخدام اتصال غير مُشفَّر، سترى في مخرجات `tcpdump` شيئًا مثل الآتي:

```text
..../zookeeper/quota.
```

عند استخدام اتصالٍ مشفّر، يجب ألّا ترى هذا.