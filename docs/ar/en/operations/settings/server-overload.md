---
description: 'التحكم في السلوك عند الحمل الزائد على CPU على الخادم.'
sidebar_label: 'فرط الحمل على الخادم'
slug: /operations/settings/server-overload
title: 'فرط الحمل على الخادم'
doc_type: 'reference'
---

<div id="overview">
  ## نظرة عامة
</div>

قد يتعرض الخادم أحيانًا لحمل زائد لأسباب مختلفة. ولتحديد مستوى الحمل الزائد الحالي على CPU،
يحسب خادم ClickHouse نسبة وقت انتظار CPU (المقياس `OSCPUWaitMicroseconds`) إلى وقت الانشغال
(المقياس `OSCPUVirtualTimeMicroseconds`). وعندما يتجاوز الحمل على الخادم نسبةً معينة،
فقد يكون من المنطقي رفض بعض الاستعلامات أو حتى طلبات الاتصال، حتى لا يزداد الحمل أكثر.

يوجد إعداد على مستوى الخادم باسم `os_cpu_busy_time_threshold` يتحكم في الحد الأدنى لوقت الانشغال اللازم لاعتبار أن CPU
ينفذ عملًا مفيدًا. وإذا كانت القيمة الحالية للمقياس `OSCPUVirtualTimeMicroseconds` أقل من هذه القيمة،
فيُفترض أن الحمل الزائد على CPU يساوي 0.

<div id="rejecting-queries">
  ## رفض الاستعلامات
</div>

يُتحكَّم في سلوك رفض الاستعلامات عبر إعدادات على مستوى الاستعلام هي `min_os_cpu_wait_time_ratio_to_throw` و
`max_os_cpu_wait_time_ratio_to_throw`. إذا كانت هذه الإعدادات معيّنة وكان `min_os_cpu_wait_time_ratio_to_throw` أقل
من `max_os_cpu_wait_time_ratio_to_throw`، فسيُرفض الاستعلام وسيُطرَح الخطأ `SERVER_OVERLOADED`
باحتمالٍ معيّن إذا كانت نسبة زيادة التحميل لا تقل عن `min_os_cpu_wait_time_ratio_to_throw`. ويُحدَّد هذا الاحتمال
باستخدام الاستيفاء الخطي بين النسبتين الدنيا والعليا. على سبيل المثال، إذا كان `min_os_cpu_wait_time_ratio_to_throw = 2`،
و`max_os_cpu_wait_time_ratio_to_throw = 6`، و`cpu_overload = 4`، فسيُرفض الاستعلام باحتمال `0.5`.

<div id="dropping-connections">
  ## قطع الاتصالات
</div>

يُتحكَّم في قطع الاتصالات بواسطة إعدادات على مستوى الخادم هي `min_os_cpu_wait_time_ratio_to_drop_connection` و
`max_os_cpu_wait_time_ratio_to_drop_connection`. يمكن تغيير هذه الإعدادات دون إعادة تشغيل الخادم. الفكرة من وراء
هذه الإعدادات مشابهة لفكرة رفض الاستعلامات. والفرق الوحيد هنا هو أنه إذا كان الخادم تحت حمل زائد،
فستُرفَض محاولة الاتصال من طرف الخادم.

<div id="resource-overload-warnings">
  ## تحذيرات التحميل الزائد للموارد
</div>

يسجّل ClickHouse أيضًا تحذيرات الحمل الزائد على CPU والذاكرة في جدول `system.warnings` عندما يكون الخادم تحت حملٍ زائد. يمكنك
تخصيص هذه العتبات من خلال إعدادات الخادم.

**مثال**

```xml

<resource_overload_warnings>
    <cpu_overload_warn_ratio>0.9</cpu_overload_warn_ratio>
    <cpu_overload_clear_ratio>0.8</cpu_overload_clear_ratio>
    <cpu_overload_duration_seconds>600</cpu_overload_duration_seconds>
    <memory_overload_warn_ratio>0.9</memory_overload_warn_ratio>
    <memory_overload_clear_ratio>0.8</memory_overload_clear_ratio>
    <memory_overload_duration_seconds>600</memory_overload_duration_seconds>
</resource_overload_warnings>
```