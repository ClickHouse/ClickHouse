---
slug: /sql-reference/statements/create/dictionary/layouts/ip-trie
title: 'ip_trie dictionary layout'
sidebar_label: 'ip_trie'
sidebar_position: 10
description: 'Store a dictionary as a trie for fast IP address prefix lookups.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The `ip_trie` dictionary is designed for IP address lookups by network prefix.
It stores IP ranges in CIDR notation and allows fast determination of which prefix (e.g. subnet or ASN range) a given IP falls into, making it ideal for IP-based searches like geolocation or network classification.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/4dxMAqltygk?si=rrQrneBReK6lLfza" title="IP based search with the ip_trie dictionary" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

**Example**

Suppose we have a table in ClickHouse that contains our IP prefixes and mappings:

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

Let's define an `ip_trie` dictionary for this table. The `ip_trie` layout requires a composite key:

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
<TabItem value="xml" label="Configuration file">

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
        <!-- Key attribute `prefix` can be retrieved via dictGetString. -->
        <!-- This option increases memory usage. -->
        <access_to_key_from_attributes>true</access_to_key_from_attributes>
    </ip_trie>
</layout>
```

</TabItem>
</Tabs>
<br/>

The key must have only one `String` type attribute that contains an allowed IP prefix. Other types are not supported yet.

The syntax is:

```sql
dictGetT('dict_name', 'attr_name', ip)
```

The function takes either `UInt32` for IPv4, or `FixedString(16)` for IPv6. For example:

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

Other types are not supported yet. The function returns the attribute for the prefix that corresponds to this IP address. If there are overlapping prefixes, the most specific one is returned.

Data must completely fit into RAM.
