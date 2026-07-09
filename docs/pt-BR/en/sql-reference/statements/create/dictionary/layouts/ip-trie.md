---
slug: /sql-reference/statements/create/dictionary/layouts/ip-trie
title: 'layout de dicionário ip_trie'
sidebar_label: 'ip_trie'
sidebar_position: 10
description: 'Armazena um dicionário como uma trie para buscas rápidas por prefixo de endereço IP.'
doc_type: 'referência'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

O dicionário `ip_trie` foi projetado para buscas de endereços IP por prefixo de rede.
Ele armazena intervalos de IP na notação CIDR e permite determinar rapidamente em qual prefixo (por exemplo, sub-rede ou intervalo de ASN) um determinado IP se enquadra, tornando-o ideal para buscas baseadas em IP, como geolocalização ou classificação de rede.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/4dxMAqltygk?si=rrQrneBReK6lLfza" title="Busca baseada em IP com o dicionário ip_trie" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

**Exemplo**

Suponha que tenhamos uma tabela no ClickHouse que contenha nossos prefixos de IP e mapeamentos:

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

Vamos definir um Dicionário `ip_trie` para esta tabela. O layout `ip_trie` requer uma chave composta:

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

  <TabItem value="xml" label="Arquivo de configuração">
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
            <!-- O atributo de chave `prefix` pode ser recuperado com dictGetString. -->
            <!-- Esta opção aumenta o uso de memória. -->
            <access_to_key_from_attributes>true</access_to_key_from_attributes>
        </ip_trie>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

A chave deve ter apenas um atributo do tipo `String` que contenha um prefixo de IP válido. Outros tipos ainda não são suportados.

A sintaxe é:

```sql
dictGetT('dict_name', 'attr_name', ip)
```

A função aceita `UInt32` para IPv4 ou `FixedString(16)` para IPv6. Por exemplo:

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

Outros tipos ainda não têm suporte. A função retorna o atributo do prefixo correspondente a este endereço IP. Se houver prefixos sobrepostos, o mais específico será retornado.

Os dados devem caber inteiramente na RAM.