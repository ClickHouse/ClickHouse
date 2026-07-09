---
description: 'توثيق QUOTA'
sidebar_label: 'QUOTA'
sidebar_position: 46
slug: /sql-reference/statements/alter/quota
title: 'ALTER QUOTA'
doc_type: 'reference'
---

يُغيّر QUOTA.

الصيغة:

```sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time | queries_per_normalized_hash} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

تتوافق المفاتيح `user_name` و`ip_address` و`forwarded_ip_address` و`client_key` و`client_key, user_name` و`client_key, ip_address` و`normalized_query_hash` مع الحقول في جدول [system.quotas](../../../operations/system-tables/quotas.md).

لا يمكن استخدام الخيارين `IPV4_PREFIX_BITS` و`IPV6_PREFIX_BITS` إلا عندما تكون `KEYED BY` هي `ip_address` أو `forwarded_ip_address`. وهما يتوافقان مع الحقل في جدول [system.quotas](../../../operations/system-tables/quotas.md).

تتوافق المعلمات `queries` و`query_selects` و`query_inserts` و`errors` و`result_rows` و`result_bytes` و`read_rows` و`read_bytes` و`execution_time` و`queries_per_normalized_hash` مع الحقول في جدول [system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md).

تتيح عبارة `ON CLUSTER` إنشاء حصص على عنقود؛ راجع [Distributed DDL](../../../sql-reference/distributed-ddl.md).

**أمثلة**

حدِّد الحد الأقصى لعدد الاستعلامات للمستخدم الحالي بقيد 123 استعلامًا خلال 15 شهرًا:

```sql
ALTER QUOTA IF EXISTS qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

بالنسبة إلى المستخدم default، اجعل الحد الأقصى لوقت التنفيذ نصف ثانية خلال 30 دقيقة، وحدِّد الحد الأقصى لعدد الاستعلامات بـ321 والحد الأقصى لعدد الأخطاء بـ10 خلال 5 أرباع:

```sql
ALTER QUOTA IF EXISTS qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```