---
description: 'Guide de configuration d''une communication SSL/TLS sécurisée entre ClickHouse
  et ZooKeeper'
sidebar_label: 'Communication sécurisée avec Zookeeper'
sidebar_position: 45
slug: /operations/ssl-zookeeper
title: 'Communication sécurisée optionnelle entre ClickHouse et Zookeeper'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_automated.md';

<SelfManaged />

Vous devez spécifier `ssl.keyStore.location`, `ssl.keyStore.password`, ainsi que `ssl.trustStore.location` et `ssl.trustStore.password` pour la communication avec le client ClickHouse via SSL. Ces options sont disponibles à partir de la version 3.5.2 de ZooKeeper.

Vous pouvez ajouter `zookeeper.crt` aux certificats approuvés.

```bash
sudo cp zookeeper.crt /usr/local/share/ca-certificates/zookeeper.crt
sudo update-ca-certificates
```

La section Client de `config.xml` se présentera ainsi :

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

Ajoutez Zookeeper à la configuration de ClickHouse avec un cluster et des macros :

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

Démarrez `clickhouse-server`. Dans les logs, vous devriez voir :

```text
<Trace> ZooKeeper: initialized, hosts: secure://localhost:2281
```

Le préfixe `secure://` indique que la connexion est sécurisée via SSL.

Pour vérifier que le trafic est chiffré, exécutez `tcpdump` sur le port sécurisé :

```bash
tcpdump -i any dst port 2281 -nnXS
```

Puis exécutez une requête dans `clickhouse-client` :

```sql
SELECT * FROM system.zookeeper WHERE path = '/';
```

Sur une connexion non chiffrée, vous verrez dans la sortie de `tcpdump` quelque chose comme ceci :

```text
..../zookeeper/quota.
```

Avec une connexion chiffrée, ceci ne devrait pas apparaître.