---
slug: /sql-reference/statements/create/dictionary/sources/executable-pool
title: 'مصدر القاموس من نوع Executable Pool'
sidebar_position: 4
sidebar_label: 'Executable Pool'
description: 'إعداد Executable Pool كمصدر للقاموس في ClickHouse.'
doc_type: 'مرجع'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

يتيح Executable pool تحميل البيانات من تجمّع من العمليات.
لا يعمل هذا المصدر مع تخطيطات القاموس التي تحتاج إلى تحميل جميع البيانات من المصدر.

يعمل Executable pool إذا كان القاموس [مخزنًا](../layouts/#storing-dictionaries-in-memory) باستخدام أحد التخطيطات التالية:

* `cache`
* `complex_key_cache`
* `ssd_cache`
* `complex_key_ssd_cache`
* `direct`
* `complex_key_direct`

سينشئ Executable pool تجمّعًا من العمليات باستخدام الأمر المحدد، ويُبقيها قيد التشغيل إلى أن تنتهي. يجب أن يقرأ البرنامج البيانات من STDIN ما دامت متاحة، وأن يرسل النتيجة إلى STDOUT. ويمكنه انتظار كتلة البيانات التالية على STDIN. لن يقوم ClickHouse بإغلاق STDIN بعد معالجة كتلة بيانات، بل سيمرر جزءًا آخر من البيانات عند الحاجة. يجب أن يكون البرنامج النصي القابل للتنفيذ مهيأً لهذه الطريقة في معالجة البيانات — أي ينبغي له مراقبة STDIN وتمرير البيانات إلى STDOUT مبكرًا.

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE_POOL(
        command 'while read key; do printf "$key\tData for key $key\n"; done'
        format 'TabSeparated'
        pool_size 10
        max_command_execution_time 10
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التكوين">
    ```xml
    <source>
        <executable_pool>
            <command><command>while read key; do printf "$key\tData for key $key\n"; done</command</command>
            <format>TabSeparated</format>
            <pool_size>10</pool_size>
            <max_command_execution_time>10<max_command_execution_time>
            <implicit_key>false</implicit_key>
        </executable_pool>
    </source>
    ```
  </TabItem>
</Tabs>

حقول الإعدادات:

| الإعداد                       | الوصف                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                     | المسار المطلق إلى الملف القابل للتنفيذ، أو اسم الملف (إذا كان دليل البرنامج مُدرجًا في `PATH`).                                                                                                                                                                                                                                                                                                                                 |
| `format`                      | تنسيق الملف. جميع التنسيقات الموضحة في [Formats](/ar/sql-reference/formats) مدعومة.                                                                                                                                                                                                                                                                                                                                                |
| `pool_size`                   | حجم التجمّع. إذا تم تحديد `0` كقيمة لـ `pool_size`، فلن تكون هناك قيود على حجم التجمّع. القيمة الافتراضية هي `16`.                                                                                                                                                                                                                                                                                                              |
| `command_termination_timeout` | يجب أن يحتوي البرنامج النصي القابل للتنفيذ على حلقة رئيسية للقراءة والكتابة. بعد إتلاف القاموس، يُغلق الـ pipe، وسيكون أمام الملف القابل للتنفيذ `command_termination_timeout` ثانيةً للتوقف قبل أن يرسل ClickHouse إشارة SIGTERM إلى العملية الفرعية. تُحدد هذه القيمة بالثواني. القيمة الافتراضية هي `10`. اختياري.                                                                                                           |
| `max_command_execution_time`  | الحد الأقصى لوقت تنفيذ أمر البرنامج النصي القابل للتنفيذ لمعالجة كتلة بيانات. تُحدد هذه القيمة بالثواني. القيمة الافتراضية هي `10`. اختياري.                                                                                                                                                                                                                                                                                    |
| `command_read_timeout`        | مهلة قراءة البيانات من stdout الخاص بالأمر، بالمللي ثانية. القيمة الافتراضية `10000`. اختياري.                                                                                                                                                                                                                                                                                                                                  |
| `command_write_timeout`       | مهلة كتابة البيانات إلى stdin الخاص بالأمر، بالمللي ثانية. القيمة الافتراضية `10000`. اختياري.                                                                                                                                                                                                                                                                                                                                  |
| `implicit_key`                | يمكن لملف المصدر القابل للتنفيذ إرجاع القيم فقط، وتُحدد المطابقة مع المفاتيح المطلوبة ضمنيًا بحسب ترتيب الصفوف في النتيجة. القيمة الافتراضية هي `false`. اختياري.                                                                                                                                                                                                                                                               |
| `execute_direct`              | إذا كانت قيمة `execute_direct` = `1`، فسيتم البحث عن `command` داخل مجلد user&#95;scripts المحدد بواسطة [user&#95;scripts&#95;path](/ar/operations/server-configuration-parameters/settings#user_scripts_path). يمكن تحديد وسيطات إضافية للبرنامج النصي باستخدام فاصل مسافات. مثال: `script_name arg1 arg2`. إذا كانت قيمة `execute_direct` = `0`، فسيتم تمرير `command` كوسيط إلى `bin/sh -c`. القيمة الافتراضية هي `1`. اختياري. |
| `send_chunk_header`           | يتحكم في ما إذا كان سيتم إرسال عدد الصفوف قبل إرسال جزء من البيانات إلى العملية. القيمة الافتراضية هي `false`. اختياري.                                                                                                                                                                                                                                                                                                         |

لا يمكن تكوين مصدر القاموس هذا إلا عبر تكوين XML. تم تعطيل إنشاء القواميس ذات المصدر القابل للتنفيذ عبر DDL، إذ سيكون مستخدم قاعدة البيانات قادرًا، بخلاف ذلك، على تنفيذ ملف binary عشوائي على عقدة ClickHouse.