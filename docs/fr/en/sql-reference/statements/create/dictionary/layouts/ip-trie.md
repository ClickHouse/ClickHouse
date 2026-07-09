---
slug: /sql-reference/statements/create/dictionary/layouts/ip-trie
title: 'layout de dictionnaire ip_trie'
sidebar_label: 'ip_trie'
sidebar_position: 10
description: 'Stocker un dictionnaire sous forme de trie pour une recherche rapide de préfixes d’adresses IP.'
doc_type: 'référence'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Le dictionnaire `ip_trie` est conçu pour effectuer des recherches d&#39;adresses IP par préfixe réseau.
Il stocke des plages d&#39;adresses IP au format CIDR et permet de déterminer rapidement dans quel préfixe (par exemple, un sous-réseau ou une plage d&#39;ASN) se trouve une adresse IP donnée, ce qui en fait un choix idéal pour les recherches basées sur les IP, comme la géolocalisation ou la classification des réseaux.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/4dxMAqltygk?si=rrQrneBReK6lLfza" title="Recherche basée sur les IP avec le dictionnaire ip_trie" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

**Exemple**

Supposons que nous ayons une Table dans ClickHouse contenant nos préfixes IP et leurs correspondances :

```sql
CREATE TABLE my_ip_addresses (
    prefix String,
    asn UInt32,
    cca2 String
)
ENGINE = MergeTree
PRIMARY KEY prefix;
```

```sql
INSERT INTO my_ip_addresses VALUES
    ('202.79.32.0/20', 17501, 'NP'),
    ('2620:0:870::/48', 3856, 'US'),
    ('2a02:6b8:1::/48', 13238, 'RU'),
    ('2001:db8::/32', 65536, 'ZZ')
;
```

Définissons un dictionnaire `ip_trie` pour cette table. La disposition `ip_trie` nécessite une clé composée :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY my_ip_trie_dictionary (
        prefix String,
        asn UInt32,
        cca2 String DEFAULT '??'
    )
    PRIMARY KEY prefix
    SOURCE(CLICKHOUSE(TABLE 'my_ip_addresses'))
    LAYOUT(IP_TRIE)
    LIFETIME(3600);
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <structure>
        <key>
            <attribute>
                <name>prefix</name>
                <type>String</type>
            </attribute>
        </key>
        <attribute>
                <name>asn</name>
                <type>UInt32</type>
                <null_value />
        </attribute>
        <attribute>
                <name>cca2</name>
                <type>String</type>
                <null_value>??</null_value>
        </attribute>
        ...
    </structure>
    <layout>
        <ip_trie>
            <!-- L'attribut de clé `prefix` peut être récupéré via dictGetString. -->
            <!-- Cette option augmente la consommation de mémoire. -->
            <access_to_key_from_attributes>true</access_to_key_from_attributes>
        </ip_trie>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

La clé ne doit comporter qu’un seul attribut de type `String` contenant un préfixe IP valide. Les autres types ne sont pas encore pris en charge.

La syntaxe est :

```sql
dictGetT('dict_name', 'attr_name', ip)
```

La fonction accepte soit `UInt32` pour IPv4, soit `FixedString(16)` pour IPv6. Par exemple :

```sql
SELECT dictGet('my_ip_trie_dictionary', 'cca2', toIPv4('202.79.32.10')) AS result;

┌─result─┐
│ NP     │
└────────┘


SELECT dictGet('my_ip_trie_dictionary', 'asn', IPv6StringToNum('2001:db8::1')) AS result;

┌─result─┐
│  65536 │
└────────┘


SELECT dictGet('my_ip_trie_dictionary', ('asn', 'cca2'), IPv6StringToNum('2001:db8::1')) AS result;

┌─result───────┐
│ (65536,'ZZ') │
└──────────────┘
```

Les autres types ne sont pas encore pris en charge. La fonction renvoie l’attribut du préfixe correspondant à cette adresse IP. En cas de chevauchement entre plusieurs préfixes, le plus spécifique est renvoyé.

Les données doivent tenir entièrement en RAM.