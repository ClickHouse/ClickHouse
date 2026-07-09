---
slug: /sql-reference/statements/create/dictionary/sources/yamlregexptree
title: 'مصدر القاموس YAMLRegExpTree'
sidebar_position: 15
sidebar_label: 'YAMLRegExpTree'
description: 'هيّئ ملف YAML كمصدر لقواميس شجرة التعبيرات النمطية.'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

يحمّل المصدر `YAMLRegExpTree` شجرة التعبيرات النمطية من ملف YAML على نظام الملفات المحلي.
وهو مخصّص حصريًا للاستخدام مع تخطيط القاموس [`regexp_tree`](../layouts/regexp-tree.md)
ويوفّر تعيينات هرمية من التعبيرات النمطية إلى السمات لعمليات البحث القائمة على الأنماط، مثل تحليل user agent.

:::note
لا يتوفر المصدر `YAMLRegExpTree` إلا في ClickHouse Open Source.
أما في ClickHouse Cloud، فصدّر القاموس إلى CSV وحمّله عبر [مصدر جدول ClickHouse](./clickhouse.md) بدلًا من ذلك.
راجع [استخدام قواميس regexp&#95;tree في ClickHouse Cloud](../layouts/regexp-tree#use-regular-expression-tree-dictionary-in-clickhouse-cloud) لمزيد من التفاصيل.
:::

<div id="configuration">
  ## الإعداد
</div>

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0);
```

حقول الإعداد:

| الإعداد | الوصف                                                                                                                              |
| ------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `PATH`  | المسار المطلق لملف YAML الذي يحتوي على شجرة التعبيرات النمطية. عند إنشائه عبر DDL، يجب أن يكون الملف موجودًا في دليل `user_files`. |

<div id="yaml-file-structure">
  ## بنية ملف YAML
</div>

يحتوي ملف YAML على قائمة بعُقد شجرة التعبيرات النمطية. ويمكن أن تتضمن كل عقدة سماتٍ وعُقدًا فرعية، مما يُشكّل تسلسلاً هرميًا:

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

لكل عقدة البنية التالية:

* **`regexp`**: التعبير النمطي لهذه العقدة.
* **attributes**: سمات القاموس المعرّفة من قِبل المستخدم (مثل `name` و`version`). قد تحتوي قيم السمات على **مراجع خلفية** إلى مجموعات الالتقاط في التعبير النمطي، وتُكتب على هيئة `\1` أو `$1` (الأرقام من 1 إلى 9). وتُستبدل هذه بالمجموعة الملتقطة المطابقة عند وقت تنفيذ الاستعلام.
* **child nodes**: قائمة بالعقد الفرعية، لكل منها سماتها الخاصة، وقد تحتوي اختياريًا على مزيد من العقد الفرعية. اسم قائمة العقد الفرعية اختياري (مثل `versions` أعلاه). تتبع مطابقة السلاسل النصية أسلوب العمق أولًا: إذا طابقت سلسلة نصية عقدةً ما، فسيُتحقَّق أيضًا من عقدها الفرعية. وتكون لسمات أعمق عقدة مطابقة الأسبقية، فتتجاوز سمات العقدة الأصل التي تحمل الاسم نفسه.

<div id="related-pages">
  ## صفحات ذات صلة
</div>

* [تخطيط قاموس regexp&#95;tree](../layouts/regexp-tree.md) — إعدادات التخطيط، وأمثلة على الاستعلامات، وأوضاع المطابقة
* [dictGet](/ar/sql-reference/functions/ext-dict-functions#dictGet), [dictGetAll](/ar/sql-reference/functions/ext-dict-functions#dictGetAll) — دوال للاستعلام عن قواميس regexp tree