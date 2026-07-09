---
slug: /sql-reference/statements/create/dictionary/sources/executable-file
title: 'مصدر القاموس «ملف تنفيذي»'
sidebar_position: 3
sidebar_label: 'ملف تنفيذي'
description: 'قم بتهيئة ملف تنفيذي ليكون مصدرًا للقاموس في ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

يعتمد التعامل مع الملفات القابلة للتنفيذ على [كيفية تخزين القاموس في الذاكرة](../layouts/). إذا كان القاموس مخزّنًا باستخدام `cache` و`complex_key_cache`، فإن ClickHouse يطلب المفاتيح اللازمة بإرسال طلب إلى `STDIN` الخاص بالملف القابل للتنفيذ. بخلاف ذلك، يشغّل ClickHouse الملف القابل للتنفيذ ويتعامل مع مخرجاته على أنها بيانات القاموس.

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE(
        command 'cat /opt/dictionaries/os.tsv'
        format 'TabSeparated'
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <source>
        <executable>
            <command>cat /opt/dictionaries/os.tsv</command>
            <format>TabSeparated</format>
            <implicit_key>false</implicit_key>
        </executable>
    </source>
    ```
  </TabItem>
</Tabs>

حقول الإعدادات:

| Setting                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                     | المسار المطلق للملف القابل للتنفيذ، أو اسم الملف (إذا كان دليل الأمر موجودًا ضمن `PATH`).                                                                                                                                                                                                                                                                                                                                        |
| `format`                      | تنسيق الملف. جميع التنسيقات الموضحة في [Formats](/ar/sql-reference/formats) مدعومة.                                                                                                                                                                                                                                                                                                                                                 |
| `command_termination_timeout` | يجب أن يحتوي البرنامج النصي القابل للتنفيذ على حلقة رئيسية للقراءة والكتابة. بعد حذف القاموس، يُغلَق الأنبوب، ويكون أمام الملف القابل للتنفيذ `command_termination_timeout` ثانية للتوقف قبل أن يرسل ClickHouse إشارة `SIGTERM` إلى العملية الفرعية. تُحدَّد القيمة بالثواني. القيمة الافتراضية هي `10`. اختياري.                                                                                                                |
| `command_read_timeout`        | مهلة قراءة البيانات من `stdout` الخاص بالأمر، بالمللي ثانية. القيمة الافتراضية `10000`. اختياري.                                                                                                                                                                                                                                                                                                                                 |
| `command_write_timeout`       | مهلة كتابة البيانات إلى `stdin` الخاص بالأمر، بالمللي ثانية. القيمة الافتراضية `10000`. اختياري.                                                                                                                                                                                                                                                                                                                                 |
| `implicit_key`                | يمكن لملف المصدر القابل للتنفيذ أن يعيد القيم فقط، ويُحدَّد تطابقها مع المفاتيح المطلوبة ضمنيًا وفق ترتيب الصفوف في النتيجة. القيمة الافتراضية هي `false`.                                                                                                                                                                                                                                                                       |
| `execute_direct`              | إذا كانت قيمة `execute_direct` = `1`، فسيُبحث عن `command` داخل مجلد user&#95;scripts المحدد بواسطة [user&#95;scripts&#95;path](/ar/operations/server-configuration-parameters/settings#user_scripts_path). ويمكن تحديد وسائط إضافية للبرنامج النصي باستخدام فاصل مسافة. مثال: `script_name arg1 arg2`. وإذا كانت قيمة `execute_direct` = `0`، فسيُمرَّر `command` بوصفه وسيطًا إلى `bin/sh -c`. القيمة الافتراضية هي `0`. اختياري. |
| `send_chunk_header`           | يتحكم في ما إذا كان سيتم إرسال عدد الصفوف قبل إرسال دفعة من البيانات إلى العملية. القيمة الافتراضية هي `false`. اختياري.                                                                                                                                                                                                                                                                                                         |

لا يمكن تهيئة مصدر القاموس هذا إلا عبر تهيئة XML. كما أن إنشاء القواميس ذات المصدر القابل للتنفيذ عبر DDL معطّل؛ وإلا فسيكون بمقدور مستخدم قاعدة البيانات تنفيذ ملفات تنفيذية عشوائية على عقدة ClickHouse.