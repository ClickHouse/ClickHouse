---
description: 'دليل لتهيئة واستخدام برامج نصية لبدء التشغيل الخاصة بـ SQL في ClickHouse من أجل
  إنشاء المخطط وعمليات الترحيل تلقائيًا'
sidebar_label: 'برامج نصية لبدء التشغيل'
slug: /operations/startup-scripts
title: 'برامج نصية لبدء التشغيل'
doc_type: 'guide'
---

يمكن لـ ClickHouse تشغيل أي استعلامات SQL من تهيئة الخادم عند بدء التشغيل. ويمكن أن يكون ذلك مفيدًا لعمليات الترحيل أو لإنشاء المخطط تلقائيًا.

```xml
<clickhouse>
    <startup_scripts>
        <throw_on_error>false</throw_on_error>
        <scripts>
            <query>CREATE ROLE OR REPLACE test_role</query>
        </scripts>
        <scripts>
            <query>CREATE TABLE TestTable (id UInt64) ENGINE=TinyLog</query>
            <condition>SELECT 1;</condition>
        </scripts>
        <scripts>
            <query>CREATE DICTIONARY test_dict (...) SOURCE(CLICKHOUSE(...))</query>
            <user>default</user>
        </scripts>
    </startup_scripts>
</clickhouse>
```

ينفّذ ClickHouse جميع الاستعلامات من `startup_scripts` بالتتابع وبالترتيب المحدد. وإذا فشل أيٌّ من الاستعلامات، فلن يتوقف تنفيذ الاستعلامات التالية. ومع ذلك، إذا ضُبط `throw_on_error` على `true`،
فلن يبدأ الخادم إذا وقع خطأ أثناء تنفيذ البرامج النصية.

يمكنك تحديد استعلام شرطي في ملف الإعدادات. وفي هذه الحالة، لا يُنفَّذ الاستعلام المقابل إلا إذا أعاد استعلام الشرط القيمة `1` أو `true`.

:::note
إذا أعاد استعلام الشرط أي قيمة غير `1` أو `true`، فستُفسَّر النتيجة على أنها `false`، ولن يُنفَّذ الاستعلام المقابل.
:::