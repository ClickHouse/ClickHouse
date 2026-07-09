---
slug: /sql-reference/statements/create/dictionary/layouts/ip-trie
title: 'ip_trie Dictionary レイアウト'
sidebar_label: 'ip_trie'
sidebar_position: 10
description: '高速な IP アドレスのプレフィックスルックアップ向けに、Dictionary をトライ木として格納します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

`ip_trie` Dictionary は、ネットワークプレフィックスによる IP アドレスのルックアップ向けに設計されています。
IP 範囲を CIDR 表記で格納し、指定した IP がどのプレフィックス (たとえばサブネットや ASN の範囲) に該当するかを高速に判定できます。そのため、地理位置情報やネットワーク分類など、IP ベースの検索に最適です。

<iframe width="1024" height="576" src="https://www.youtube.com/embed/4dxMAqltygk?si=rrQrneBReK6lLfza" title="ip_trie Dictionary を使った IP ベースの検索" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

**例**

IP プレフィックスとマッピングを含む ClickHouse のテーブルがあるとします。

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

このテーブル用に `ip_trie` Dictionary を定義しましょう。`ip_trie` レイアウトでは複合キーが必要です。

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

  <TabItem value="xml" label="設定ファイル">
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
            <!-- キー属性 `prefix` は dictGetString を使って取得できます。 -->
            <!-- このオプションを有効にすると、メモリ使用量が増加します。 -->
            <access_to_key_from_attributes>true</access_to_key_from_attributes>
        </ip_trie>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

キーには、有効な IP プレフィックスを含む `String` 型の属性を 1 つだけ指定する必要があります。他の型はまだサポートされていません。

構文は次のとおりです。

```sql
dictGetT('dict_name', 'attr_name', ip)
```

この関数は、IPv4 の場合は `UInt32`、IPv6 の場合は `FixedString(16)` を受け取ります。たとえば次のとおりです。

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

その他の型は、まだサポートされていません。この関数は、この IP アドレスに対応するプレフィックスの属性を返します。プレフィックスが重複している場合は、最も具体的なものが返されます。

データは RAM に完全に収まる必要があります。