---
slug: /sql-reference/statements/create/dictionary/layouts/ip-trie
title: 'تخطيط قاموس ip_trie'
sidebar_label: 'ip_trie'
sidebar_position: 10
description: 'خزّن القاموس كبنية trie لإجراء عمليات بحث سريعة عن بادئات عناوين IP.'
doc_type: 'مرجع'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

صُمِّم القاموس `ip_trie` لإجراء lookup لعناوين IP استنادًا إلى بادئة الشبكة.
ويخزّن نطاقات IP بترميز CIDR، ويتيح التحديد السريع للبادئة التي يقع ضمنها عنوان IP معيّن (مثل شبكة فرعية أو نطاق ASN)، مما يجعله مثاليًا لعمليات البحث المعتمدة على IP، مثل تحديد الموقع الجغرافي أو تصنيف الشبكات.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/4dxMAqltygk?si=rrQrneBReK6lLfza" title="بحث قائم على IP باستخدام قاموس ip_trie" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

**مثال**

لنفترض أن لدينا جدولًا في ClickHouse يحتوي على بادئات IP والتعيينات المقابلة لها:

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

لنعرّف قاموس `ip_trie` لهذا الجدول. يتطلب تخطيط `ip_trie` مفتاحًا مركبًا:

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

  <TabItem value="xml" label="ملف الإعدادات">
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
            <!-- يمكن استرجاع سمة المفتاح `prefix` باستخدام dictGetString. -->
            <!-- يزيد هذا الخيار من استهلاك الذاكرة. -->
            <access_to_key_from_attributes>true</access_to_key_from_attributes>
        </ip_trie>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

يجب أن يحتوي المفتاح على سمة واحدة فقط من النوع `String` تتضمن بادئة IP صالحة. أما الأنواع الأخرى فغير مدعومة بعد.

الصيغة هي:

```sql
dictGetT('dict_name', 'attr_name', ip)
```

تقبل الدالة إما `UInt32` لـ IPv4 أو `FixedString(16)` لـ IPv6. على سبيل المثال:

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

الأنواع الأخرى غير مدعومة بعد. تُرجِع الدالة السمة الخاصة بالبادئة المطابقة لعنوان IP هذا. وإذا وُجدت بادئات متداخلة، فستُرجَع البادئة الأكثر تحديدًا.

يجب أن تتسع RAM للبيانات بالكامل.