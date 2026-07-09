---
slug: /sql-reference/statements/create/dictionary/sources/local-file
title: 'مصدر القاموس: ملف محلي'
sidebar_position: 2
sidebar_label: 'ملف محلي'
description: 'هيّئ ملفًا محليًا كمصدر للقاموس في ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

يقوم مصدر الملف المحلي بتحميل بيانات القاموس من ملف على نظام الملفات المحلي. ويُعدّ ذلك مفيدًا لجداول `lookup` الصغيرة والثابتة التي يمكن تخزينها كملفات مسطحة بتنسيقات مثل TSV وCSV أو أي [تنسيق مدعوم](/ar/sql-reference/formats).

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف التكوين">
    ```xml
    <source>
      <file>
        <path>/opt/dictionaries/os.tsv</path>
        <format>TabSeparated</format>
      </file>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

حقول الإعداد:

| الإعداد  | الوصف                                                                            |
| -------- | -------------------------------------------------------------------------------- |
| `path`   | المسار المطلق للملف.                                                             |
| `format` | تنسيق الملف. جميع التنسيقات الموضحة في [Formats](/ar/sql-reference/formats) مدعومة. |

عند إنشاء قاموس بالمصدر `FILE` عبر أمر DDL (`CREATE DICTIONARY ...`)، يجب أن يكون ملف المصدر موجودًا في الدليل `user_files` لمنع مستخدمي قاعدة البيانات من الوصول إلى ملفات عشوائية على عقدة ClickHouse.

**انظر أيضًا**

* [دالة القاموس](/ar/sql-reference/table-functions/dictionary)