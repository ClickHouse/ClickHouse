---
slug: /sql-reference/statements/create/dictionary/sources/null
title: 'مصدر القاموس Null'
sidebar_position: 14
sidebar_label: 'Null'
description: 'قم بتكوين مصدر قاموس Null (فارغ) في ClickHouse لأغراض الاختبار.'
doc_type: 'reference'
---

مصدر خاص يمكن استخدامه لإنشاء قواميس تجريبية (فارغة).
يمكن أن تكون القواميس التجريبية مفيدة لأغراض الاختبار أو في البيئات التي تحتوي على عُقد بيانات وعُقد استعلام منفصلة مع Distributed tables.

```sql
CREATE DICTIONARY null_dict (
    id              UInt64,
    val             UInt8,
    default_val     UInt8 DEFAULT 123,
    nullable_val    Nullable(UInt8)
)
PRIMARY KEY id
SOURCE(NULL())
LAYOUT(FLAT())
LIFETIME(0);
```