---
description: 'يحتوي هذا الجدول على رسائل تحذيرية حول خادم ClickHouse.'
keywords: [ 'جدول نظام', 'تحذيرات' ]
slug: /operations/system-tables/system_warnings
title: 'system.warnings'
doc_type: 'مرجع'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud />

<div id="description">
  ## الوصف
</div>

يعرض هذا الجدول تحذيرات بشأن خادم ClickHouse.
تُدمَج التحذيرات من النوع نفسه في تحذير واحد.
على سبيل المثال، إذا تجاوز العدد N من قواعد البيانات المرفقة العتبة القابلة للتهيئة T، فسيُعرَض إدخال واحد يحتوي على القيمة الحالية N بدلًا من N إدخالات منفصلة.
إذا انخفضت القيمة الحالية إلى ما دون العتبة، فسيُزال الإدخال من الجدول.

يمكن تهيئة الجدول باستخدام هذه الإعدادات:

* [max&#95;table&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_table_num_to_warn)
* [max&#95;database&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_database_num_to_warn)
* [max&#95;dictionary&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_dictionary_num_to_warn)
* [max&#95;view&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_view_num_to_warn)
* [max&#95;part&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_part_num_to_warn)
* [max&#95;pending&#95;mutations&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_pending_mutations_to_warn)
* [max&#95;pending&#95;mutations&#95;execution&#95;time&#95;to&#95;warn](/ar/operations/server-configuration-parameters/settings#max_pending_mutations_execution_time_to_warn)
* [max&#95;named&#95;collection&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_named_collection_num_to_warn)
* [resource&#95;overload&#95;warnings](/ar/operations/settings/server-overload#resource-overload-warnings)

<div id="columns">
  ## الأعمدة
</div>

* `message` ([String](../../sql-reference/data-types/string.md)) — رسالة تحذير.
* `message_format_string` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — سلسلة التنسيق المستخدمة في تنسيق الرسالة.

<div id="example">
  ## مثال
</div>

```sql title="Query"
 SELECT * FROM system.warnings LIMIT 2 \G;
```

```text title="Response"
Row 1:
──────
message:               The number of active parts is more than 10.
message_format_string: The number of active parts is more than {}.

Row 2:
──────
message:               The number of attached databases is more than 2.
message_format_string: The number of attached databases is more than {}.
```