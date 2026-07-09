---
description: 'توثيق لسياسة الإخفاء'
sidebar_label: 'سياسة الإخفاء'
sidebar_position: 42
slug: /sql-reference/statements/create/masking-policy
title: 'CREATE MASKING POLICY'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

ينشئ سياسة إخفاء تتيح تحويل قيم الأعمدة أو إخفاءها ديناميكيًا لمستخدمين أو أدوار محددة عند الاستعلام من جدول.

:::tip
توفر سياسات الإخفاء أمانًا للبيانات على مستوى العمود من خلال تحويل البيانات الحساسة وقت الاستعلام دون تعديل البيانات المخزنة.
:::

الصياغة:

```sql
CREATE MASKING POLICY [IF NOT EXISTS | OR REPLACE] policy_name ON [database.]table
    UPDATE column1 = expression1 [, column2 = expression2 ...]
    [WHERE condition]
    TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}
    [PRIORITY priority_number]
```

<div id="update-clause">
  ## بند UPDATE
</div>

يحدّد بند `UPDATE` الأعمدة المطلوب إخفاؤها وكيفية تحويلها. يمكنك إخفاء عدة أعمدة ضمن سياسة واحدة.

أمثلة:

* إخفاء بسيط: `UPDATE email = '***masked***'`
* إخفاء جزئي: `UPDATE email = concat(substring(email, 1, 3), '***@***.***')`
* إخفاء قائم على التجزئة: `UPDATE email = concat('masked_', substring(hex(cityHash64(email)), 1, 8))`
* عدة أعمدة: `UPDATE email = '***@***.***', phone = '***-***-****'`

<div id="where-clause">
  ## عبارة `WHERE`
</div>

تتيح عبارة `WHERE` الاختيارية تطبيق الإخفاء المشروط استنادًا إلى قيم الصفوف. ولا يُطبَّق الإخفاء إلا على الصفوف التي تستوفي الشرط.

مثال:

```sql
CREATE MASKING POLICY mask_high_salaries ON employees
UPDATE salary = 0
WHERE salary > 100000
TO analyst;
```

<div id="to-clause">
  ## بند TO
</div>

في قسم `TO`، حدِّد المستخدمين والأدوار التي ينبغي تطبيق السياسة عليها.

* `TO user1, user2`: تُطبَّق على مستخدمين/أدوار محدَّدين
* `TO ALL`: تُطبَّق على جميع المستخدمين
* `TO ALL EXCEPT user1, user2`: تُطبَّق على جميع المستخدمين باستثناء المحدَّدين

:::note
على عكس سياسات الصفوف، لا تؤثر سياسات الإخفاء في المستخدمين الذين لا تُطبَّق عليهم السياسة. وإذا لم تنطبق أي سياسة إخفاء على مستخدم، فسيَرى البيانات الأصلية.
:::

<div id="priority-clause">
  ## بند PRIORITY
</div>

عندما تستهدف عدة سياسات الإخفاء العمود نفسه لمستخدم معيّن، فإن بند `PRIORITY` يحدّد ترتيب تطبيقها. وتُطبَّق السياسات من الأعلى أولويةً إلى الأدنى.

الأولوية الافتراضية هي 0. أمّا السياسات ذات الأولوية نفسها فتُطبَّق بترتيب غير محدد.

مثال:

```sql
-- Applied second (lower priority)
CREATE MASKING POLICY mask1 ON users
UPDATE email = 'low@priority.com'
TO analyst
PRIORITY 1;

-- Applied first (higher priority)
CREATE MASKING POLICY mask2 ON users
UPDATE email = 'high@priority.com'
TO analyst
PRIORITY 10;

-- analyst sees 'low@priority.com' because it's applied last
```

:::note اعتبارات متعلقة بالأداء

* قد تؤثر سياسات الإخفاء في أداء الاستعلام وفقًا لتعقيد التعبير
* قد تُعطَّل بعض التحسينات للجداول التي لديها سياسات إخفاء نشطة
  :::