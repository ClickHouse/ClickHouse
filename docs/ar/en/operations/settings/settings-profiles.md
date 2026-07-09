---
description: 'مجموعة من الإعدادات مُجمَّعة تحت الاسم نفسه.'
sidebar_label: 'ملفات تعريف الإعدادات'
sidebar_position: 61
slug: /operations/settings/settings-profiles
title: 'ملفات تعريف الإعدادات'
doc_type: 'مرجع'
---

ملف تعريف الإعدادات هو مجموعة من الإعدادات مُجمَّعة تحت الاسم نفسه.

:::note
يدعم ClickHouse أيضًا [سير العمل الموجَّه بـ SQL](/ar/operations/access-rights#access-control-usage) لإدارة ملفات تعريف الإعدادات. نوصي باستخدامه.
:::

يمكن أن يحمل ملف التعريف أي اسم، كما يمكنك تحديد ملف التعريف نفسه لمستخدمين مختلفين. وأهم ما يمكنك تضمينه في ملف تعريف الإعدادات هو `readonly=1`، إذ يضمن وصولًا للقراءة فقط.

يمكن لملفات تعريف الإعدادات أن ترث من بعضها البعض. ولاستخدام الوراثة، حدِّد إعداد `profile` واحدًا أو عدة إعدادات `profile` قبل الإعدادات الأخرى المدرجة في ملف التعريف. وإذا كان أحد الإعدادات معرّفًا في ملفات تعريف مختلفة، فسيُستخدم آخر تعريف له.

لتطبيق جميع الإعدادات في ملف تعريف، اضبط إعداد `profile`.

مثال:

ثبّت ملف التعريف `web`.

```sql
SET profile = 'web'
```

تُحدَّد ملفات تعريف الإعدادات في ملف إعدادات المستخدم. ويكون هذا عادةً `users.xml`.

مثال:

```xml
<!-- Settings profiles -->
<profiles>
    <!-- Default settings -->
    <default>
        <!-- The maximum number of threads when running a single query. -->
        <max_threads>8</max_threads>
    </default>

    <!-- Background operations settings -->
    <background>
        <!-- Re-defining maximum number of threads for background operations -->
        <max_threads>12</max_threads>
    </background>

    <!-- Settings for queries from the user interface -->
    <web>
        <max_rows_to_read>1000000000</max_rows_to_read>
        <max_bytes_to_read>100000000000</max_bytes_to_read>

        <max_rows_to_group_by>1000000</max_rows_to_group_by>
        <group_by_overflow_mode>any</group_by_overflow_mode>

        <max_rows_to_sort>1000000</max_rows_to_sort>
        <max_bytes_to_sort>1000000000</max_bytes_to_sort>

        <max_result_rows>100000</max_result_rows>
        <max_result_bytes>100000000</max_result_bytes>
        <result_overflow_mode>break</result_overflow_mode>

        <max_execution_time>600</max_execution_time>
        <min_execution_speed>1000000</min_execution_speed>
        <timeout_before_checking_execution_speed>15</timeout_before_checking_execution_speed>

        <max_columns_to_read>25</max_columns_to_read>
        <max_temporary_columns>100</max_temporary_columns>
        <max_temporary_non_const_columns>50</max_temporary_non_const_columns>

        <max_subquery_depth>2</max_subquery_depth>
        <max_pipeline_depth>25</max_pipeline_depth>
        <max_ast_depth>50</max_ast_depth>
        <max_ast_elements>100</max_ast_elements>

        <max_sessions_for_user>4</max_sessions_for_user>

        <readonly>1</readonly>
    </web>
</profiles>
```

يحدّد المثال ملفي تعريف: `default` و`web`.

لملف التعريف `default` غرض خاص: يجب أن يكون موجودًا دائمًا، ويُطبَّق عند بدء تشغيل الخادم. وبعبارة أخرى، يحتوي ملف التعريف `default` على الإعدادات الافتراضية. ويمكن تغيير اسم ملف التعريف `default` عبر إعداد الخادم `default_profile`.

لملف التعريف `background` غرض خاص: قد يكون موجودًا لتجاوز إعدادات العمليات التي تعمل في الخلفية. هذه المعلمة اختيارية، ويمكن تغيير اسمها عبر إعداد الخادم `background_profile`.

أما ملف التعريف `web` فهو ملف تعريف عادي يمكن تعيينه باستخدام استعلام `SET` أو باستخدام معلمة URL في استعلام HTTP.