---
slug: /sql-reference/statements/create/dictionary/layouts/ip-trie
title: 'структура словаря ip_trie'
sidebar_label: 'ip_trie'
sidebar_position: 10
description: 'Хранение словаря в виде префиксного дерева для быстрого поиска по префиксам IP-адресов.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Словарь `ip_trie` предназначен для поиска IP-адресов по сетевому префиксу.
Он хранит IP-диапазоны в нотации CIDR и позволяет быстро определить, к какому префиксу (например, подсети или диапазону ASN) относится заданный IP-адрес, что делает его удобным для IP-поиска, например геолокации или классификации сетей.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/4dxMAqltygk?si=rrQrneBReK6lLfza" title="Поиск на основе IP с помощью словаря ip_trie" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

**Пример**

Предположим, у нас есть таблица в ClickHouse, содержащая IP-префиксы и сопоставления:

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

Определим для этой таблицы словарь `ip_trie`. Для структуры `ip_trie` требуется составной ключ:

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

  <TabItem value="xml" label="Файл конфигурации">
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
            <!-- Атрибут ключа `prefix` можно получить с помощью dictGetString. -->
            <!-- Эта опция увеличивает использование памяти. -->
            <access_to_key_from_attributes>true</access_to_key_from_attributes>
        </ip_trie>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Ключ должен содержать только один атрибут типа `String` с допустимым IP-префиксом. Другие типы пока не поддерживаются.

Синтаксис:

```sql
dictGetT('dict_name', 'attr_name', ip)
```

Функция принимает либо тип `UInt32` для IPv4, либо `FixedString(16)` для IPv6. Например:

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

Другие типы пока не поддерживаются. Функция возвращает атрибут для префикса, соответствующего этому IP-адресу. Если есть перекрывающиеся префиксы, возвращается наиболее специфичный префикс.

Данные должны полностью помещаться в оперативную память.