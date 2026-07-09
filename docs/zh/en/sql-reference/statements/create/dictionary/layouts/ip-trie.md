---
slug: /sql-reference/statements/create/dictionary/layouts/ip-trie
title: 'ip_trie 字典布局'
sidebar_label: 'ip_trie'
sidebar_position: 10
description: '将字典以 trie 形式存储，以便快速进行 IP 地址前缀查找。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

`ip_trie` 字典专为按网络前缀查找 IP 地址而设计。
它以 CIDR 表示法存储 IP 范围，并可快速判断给定 IP 属于哪个前缀 (例如子网或 ASN 范围) ，因此非常适合用于基于 IP 的搜索，例如地理定位或网络分类。

<iframe width="1024" height="576" src="https://www.youtube.com/embed/4dxMAqltygk?si=rrQrneBReK6lLfza" title="使用 ip_trie 字典进行基于 IP 的搜索" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

**示例**

假设我们在 ClickHouse 中有一张表，包含 IP 前缀及其映射：

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

我们来为这个表定义一个 `ip_trie` 字典。`ip_trie` 布局需要使用复合键：

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

  <TabItem value="xml" label="配置文件">
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
            <!-- 键属性 `prefix` 可以通过 dictGetString 获取。 -->
            <!-- 此选项会增加内存用量。 -->
            <access_to_key_from_attributes>true</access_to_key_from_attributes>
        </ip_trie>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

该键只能包含一个 `String` 类型的属性，且该属性必须包含合法的 IP 前缀。其他类型暂不支持。

语法如下：

```sql
dictGetT('dict_name', 'attr_name', ip)
```

该函数接受 `UInt32` (用于 IPv4) 或 `FixedString(16)` (用于 IPv6) 。例如：

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

其他类型暂不支持。该函数会返回与此 IP 地址对应前缀的属性。如果有多个前缀重叠，则返回其中最具体的前缀。

Data 必须能够完全装入 RAM。