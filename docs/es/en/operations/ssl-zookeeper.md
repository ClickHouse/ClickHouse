---
description: 'Guía para configurar la comunicación segura mediante SSL/TLS entre ClickHouse
  y ZooKeeper'
sidebar_label: 'Comunicación segura con ZooKeeper'
sidebar_position: 45
slug: /operations/ssl-zookeeper
title: 'Comunicación segura opcional entre ClickHouse y ZooKeeper'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

Debe especificar `ssl.keyStore.location`, `ssl.keyStore.password`, `ssl.trustStore.location` y `ssl.trustStore.password` para la comunicación mediante SSL con clickhouse client. Estas opciones están disponibles a partir de la versión 3.5.2 de ZooKeeper.

Puede añadir `zookeeper.crt` a los certificados de confianza.

```bash
sudo cp zookeeper.crt /usr/local/share/ca-certificates/zookeeper.crt
sudo update-ca-certificates
```

La sección Client de `config.xml` tendrá este aspecto:

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

Añada Zookeeper a la configuración de ClickHouse con un clúster y algunas macros:

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

Inicie `clickhouse-server`. En los logs debería ver lo siguiente:

```text
<Trace> ZooKeeper: initialized, hosts: secure://localhost:2281
```

El prefijo `secure://` indica que la conexión está protegida con SSL.

Para asegurarse de que el tráfico esté cifrado, ejecute `tcpdump` en el puerto protegido:

```bash
tcpdump -i any dst port 2281 -nnXS
```

Y ejecuta la consulta en `clickhouse-client`:

```sql
SELECT * FROM system.zookeeper WHERE path = '/';
```

En una conexión no cifrada, verás en la salida de `tcpdump` algo como esto:

```text
..../zookeeper/quota.
```

Con una conexión cifrada, no deberías ver esto.