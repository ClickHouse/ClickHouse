---
description: 'يتيح قراءة البيانات المتاحة عبر خادم Apache Arrow Flight والكتابة إليها.'
sidebar_label: 'arrowFlight'
sidebar_position: 186
slug: /sql-reference/table-functions/arrowflight
title: 'arrowFlight'
doc_type: 'reference'
---

يتيح قراءة البيانات المتاحة عبر خادم [Apache Arrow Flight](/ar/interfaces/arrowflight) والكتابة إليها.

**بناء الجملة**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**الوسائط**

* `host:port` — عنوان خادم Arrow Flight. إذا لم يُحدَّد المنفذ، فسيُستخدم المنفذ الافتراضي `8815`. [String](../../sql-reference/data-types/string.md).
* `dataset_name` — اسم مجموعة البيانات أو الواصف المتاح على خادم Arrow Flight. [String](../../sql-reference/data-types/string.md).
* `username` — اسم المستخدم لمصادقة HTTP الأساسية. [String](../../sql-reference/data-types/string.md).
* `password` — كلمة المرور لمصادقة HTTP الأساسية. [String](../../sql-reference/data-types/string.md).

إذا لم يتم تحديد `username` و`password`، فلن تُستخدم المصادقة (وهذا لا يعمل إلا إذا كان خادم Arrow Flight يسمح بالوصول دون مصادقة).

تدعم الدالة أيضًا [المجموعات المسماة](/ar/operations/named-collections) — راجع [محرك الجدول ArrowFlight](/ar/engines/table-engines/integrations/arrowflight#named-collections) للاطلاع على قائمة المعلمات المدعومة.

**القيمة المُعادة**

كائن جدول يمثّل مجموعة البيانات البعيدة. يُستدل على المخطط تلقائيًا من خادم Arrow Flight.

**الإعدادات**

* `arrow_flight_request_descriptor_type` — يتحكم في كيفية إرسال اسم مجموعة البيانات إلى خادم Flight. القيم: `path` (افتراضي) أو `command`. راجع [محرك الجدول ArrowFlight](/ar/engines/table-engines/integrations/arrowflight#settings) لمزيد من التفاصيل.

**أمثلة**

القراءة من خادم Arrow Flight بعيد:

```sql title="Query"
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

```text title="Response"
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

إدراج البيانات في خادم Arrow Flight عن بُعد:

```sql
INSERT INTO FUNCTION arrowFlight('127.0.0.1:9005', 'sample_dataset') VALUES (4, 'qux', 99.9);
```

باستخدام مجموعة مُسمّاة:

```sql
SELECT * FROM arrowFlight(named_collection_name);
```

**راجع أيضًا**

* [محرك الجدول ArrowFlight](/ar/engines/table-engines/integrations/arrowflight)
* [واجهة Arrow Flight](/ar/interfaces/arrowflight)
* [مواصفة Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)