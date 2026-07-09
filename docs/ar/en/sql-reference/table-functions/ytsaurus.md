---
description: 'تتيح دالة الجدول قراءة البيانات من عنقود YTsaurus.'
sidebar_label: 'ytsaurus'
sidebar_position: 85
slug: /sql-reference/table-functions/ytsaurus
title: 'ytsaurus'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-function">
  # دالة الجدول YTsaurus
</div>

<ExperimentalBadge />

تتيح دالة الجدول قراءة البيانات من عنقود YTsaurus.

<div id="syntax">
  ## الصيغة
</div>

```sql
ytsaurus(http_proxy_url, cypress_path, oauth_token, format)
```

:::info
هذه ميزة تجريبية، وقد تتغير في الإصدارات المستقبلية على نحو غير متوافق مع الإصدارات السابقة.
فعِّل استخدام دالة الجدول YTsaurus
باستخدام الإعداد [allow&#95;experimental&#95;ytsaurus&#95;table&#95;function](/ar/operations/settings/settings#allow_experimental_ytsaurus_table_engine).
أدخِل الأمر `set allow_experimental_ytsaurus_table_function = 1`.
:::

<div id="arguments">
  ## المعاملات
</div>

* `http_proxy_url` — عنوان URL لوكيل HTTP الخاص بـ YTsaurus.
* `cypress_path` — مسار Cypress لمصدر البيانات.
* `oauth_token` — رمز OAuth.
* `format` — [التنسيق](/ar/interfaces/formats) الخاص بمصدر البيانات.

**القيمة المُعادة**

جدول بالبنية المحددة لقراءة البيانات من مسار Cypress المحدد في عنقود YTsaurus.

**انظر أيضًا**

* [محرك YTsaurus](/ar/engines/table-engines/integrations/ytsaurus.md)