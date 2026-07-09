---
description: 'Guia para configurar comunicação SSL/TLS segura entre ClickHouse
  e ZooKeeper'
sidebar_label: 'Comunicação segura com ZooKeeper'
sidebar_position: 45
slug: /operations/ssl-zookeeper
title: 'Comunicação segura opcional entre ClickHouse e ZooKeeper'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

Você deve especificar `ssl.keyStore.location`, `ssl.keyStore.password`, `ssl.trustStore.location` e `ssl.trustStore.password` para a comunicação com o cliente do ClickHouse via SSL. Essas opções estão disponíveis a partir da versão 3.5.2 do ZooKeeper.

Você pode adicionar `zookeeper.crt` aos certificados confiáveis.

```bash
sudo cp zookeeper.crt /usr/local/share/ca-certificates/zookeeper.crt
sudo update-ca-certificates
```

A seção Client no `config.xml` ficará assim:

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

Adicione o ZooKeeper à configuração do ClickHouse com algumas definições de cluster e macros:

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

Inicie o `clickhouse-server`. Nos logs, você deverá ver:

```text
<Trace> ZooKeeper: initialized, hosts: secure://localhost:2281
```

O prefixo `secure://` indica que a conexão está protegida por SSL.

Para garantir que o tráfego esteja criptografado, execute `tcpdump` na porta segura:

```bash
tcpdump -i any dst port 2281 -nnXS
```

E faça a consulta no `clickhouse-client`:

```sql
SELECT * FROM system.zookeeper WHERE path = '/';
```

Em uma conexão sem criptografia, você verá na saída do `tcpdump` algo assim:

```text
..../zookeeper/quota.
```

Em uma conexão criptografada, isso não deve aparecer.