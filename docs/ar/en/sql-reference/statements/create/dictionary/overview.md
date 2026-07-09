---
description: 'توثيق إنشاء القواميس وتهيئتها'
sidebar_label: 'نظرة عامة'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary
title: 'CREATE DICTIONARY'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import CloudSupportedBadge from '@theme/badges/CloudSupportedBadge';

<div id="create-dictionary">
  # CREATE DICTIONARY
</div>

القاموس هو تعيين (`key -> attributes`) مفيد لأنواع مختلفة من القوائم المرجعية.
يدعم ClickHouse دوالًا خاصة للتعامل مع القواميس يمكن استخدامها في الاستعلامات. ويُعد استخدام القواميس مع هذه الدوال أسهل وأكثر كفاءة من استخدام `JOIN` مع الجداول المرجعية.

يمكن إنشاء القواميس بطريقتين:

* [باستخدام استعلام DDL](#creating-a-dictionary-with-a-ddl-query) (موصى به)
* [باستخدام ملف تهيئة](#creating-a-dictionary-with-a-configuration-file)

<div id="creating-a-dictionary-with-a-ddl-query">
  ## إنشاء قاموس باستخدام استعلام DDL
</div>

<CloudSupportedBadge />

يمكن إنشاء القواميس باستخدام استعلامات DDL.
وهذه هي الطريقة الموصى بها، لأن القواميس المُنشأة عبر DDL توفّر المزايا التالية:

* لا تُضاف أي سجلات إضافية إلى ملفات تهيئة الخادم.
* يمكن استخدام القواميس ككيانات أساسية مثل الجداول أو العروض.
* يمكن قراءة البيانات مباشرةً باستخدام صيغة `SELECT` المألوفة بدلًا من دوال الجدول الخاصة بالقواميس. لاحظ أنه عند الوصول إلى قاموس مباشرةً عبر عبارة `SELECT`، فإن القاموس المخزَّن مؤقتًا لن يعيد إلا البيانات المخزَّنة مؤقتًا، بينما يعيد القاموس غير المخزَّن مؤقتًا جميع البيانات التي يخزّنها.
* يمكن إعادة تسمية القواميس بسهولة.

<div id="syntax">
  ### الصيغة
</div>

```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1  type1  [DEFAULT | EXPRESSION expr1] [IS_OBJECT_ID],
    key2  type2  [DEFAULT | EXPRESSION expr2],
    attr1 type2  [DEFAULT | EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2  [DEFAULT | EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

| العبارة                                     | الوصف                                                                                                                     |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [السمات](./attributes.md)                   | تُحدَّد سمات القاموس بطريقة مشابهة لأعمدة الجدول. الخاصية الوحيدة المطلوبة هي النوع، وقد تكون لبقية الخصائص قيم افتراضية. |
| PRIMARY KEY                                 | يحدّد عمود المفتاح أو أعمدته لعمليات البحث في القاموس. وبحسب بنية التخزين، يمكن تحديد سمة واحدة أو أكثر كمفاتيح.          |
| [`SOURCE`](./sources/overview.md)           | يحدّد مصدر البيانات للقاموس (مثل جدول ClickHouse أو HTTP أو PostgreSQL).                                                  |
| [`LAYOUT`](./layouts/overview.md)           | يتحكّم في كيفية تخزين القاموس في الذاكرة (مثل `FLAT` و`HASHED` و`CACHE`).                                                 |
| [`LIFETIME`](./lifetime.md)                 | يضبط فترة تحديث القاموس.                                                                                                  |
| [`ON CLUSTER`](../../../distributed-ddl.md) | ينشئ القاموس على عنقود. اختياري.                                                                                          |
| `SETTINGS`                                  | إعدادات إضافية للقاموس. اختياري.                                                                                          |
| `COMMENT`                                   | يضيف تعليقًا نصيًا إلى القاموس. اختياري.                                                                                  |

<div id="creating-a-dictionary-with-a-configuration-file">
  ## إنشاء قاموس باستخدام ملف تهيئة
</div>

<CloudNotSupportedBadge />

:::note
لا ينطبق إنشاء القاموس باستخدام ملف تهيئة على ClickHouse Cloud. يُرجى استخدام DDL (انظر أعلاه)، وإنشاء القاموس بصفتك المستخدم `default`.
:::

يأتي ملف تهيئة القاموس بالتنسيق التالي:

```xml
<clickhouse>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of dictionary sections in a configuration file. -->
    </dictionary>

</clickhouse>
```

يمكنك إعداد أي عدد من القواميس في الملف نفسه.

<div id="related-content">
  ## محتوى ذو صلة
</div>

* [بُنى التخزين](/ar/sql-reference/statements/create/dictionary/layouts) — كيفية تخزين القواميس في الذاكرة
* [المصادر](/ar/sql-reference/statements/create/dictionary/sources) — الاتصال بمصادر البيانات
* [مدة الصلاحية](./lifetime.md) — إعداد التحديث التلقائي
* [السمات](./attributes.md) — إعدادات المفتاح والسمات
* [القواميس المضمّنة](./embedded.md) — قواميس geobase المضمّنة
* [system.dictionaries](../../../../operations/system-tables/dictionaries.md) — جدول نظام يتضمن معلومات عن القواميس