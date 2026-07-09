---
description: 'ينشئ جدولًا مؤقتًا بالبنية المحددة باستخدام محرك الجدول Null. تُستخدم الدالة
  لتسهيل كتابة الاختبارات والعروض التوضيحية.'
sidebar_label: 'دالة null'
sidebar_position: 140
slug: /sql-reference/table-functions/null
title: 'null'
doc_type: 'reference'
---

ينشئ جدولًا مؤقتًا بالبنية المحددة باستخدام محرك الجدول [Null](../../engines/table-engines/special/null.md). ووفقًا لخصائص محرك `Null`، يتم تجاهل بيانات الجدول، ويُحذف الجدول نفسه فور تنفيذ الاستعلام. تُستخدم الدالة لتسهيل كتابة الاختبارات والعروض التوضيحية.

<div id="syntax">
  ## الصيغة
</div>

```sql
null('structure')
```

<div id="argument">
  ## الوسيطة
</div>

* `structure` — قائمة بالأعمدة وأنواعها. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## القيمة المُعادة
</div>

جدول مؤقت يستخدم محرك `Null` بالبنية المحددة.

<div id="example">
  ## مثال
</div>

استعلام باستخدام الدالة `null`:

```sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```

يمكن أن يحل محل ثلاثة استعلامات:

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

<div id="related">
  ## ذات صلة
</div>

* [محرك الجدول Null](../../engines/table-engines/special/null.md)