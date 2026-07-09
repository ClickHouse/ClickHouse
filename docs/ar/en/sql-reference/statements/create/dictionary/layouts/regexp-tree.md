---
slug: /sql-reference/statements/create/dictionary/layouts/regexp-tree
title: 'تخطيط قاموس شجرة التعبيرات النمطية'
sidebar_label: 'Regexp Tree'
sidebar_position: 12
description: 'هيّئ قاموس شجرة التعبيرات النمطية لإجراء عمليات بحث مستندة إلى الأنماط.'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="overview">
  ## نظرة عامة
</div>

يتيح لك القاموس `regexp_tree` تعيين المفاتيح إلى القيم استنادًا إلى أنماط تعبيرات نمطية هرمية.
وهو مُحسَّن لعمليات lookup المعتمدة على مطابقة الأنماط (مثل تصنيف السلاسل النصية، كسلاسل user agent، عبر مطابقة أنماط regex) بدلًا من المطابقة التامة للمفاتيح.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/ESlAhUJMoz8?si=sY2OVm-zcuxlDRaX" title="مقدمة إلى قواميس شجرة regex في ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

<div id="use-regular-expression-tree-dictionary-in-clickhouse-open-source">
  ## استخدام قاموس شجرة التعبيرات النمطية مع المصدر YAMLRegExpTree
</div>

<CloudNotSupportedBadge />

تُعرَّف قواميس شجرة التعبيرات النمطية في ClickHouse مفتوح المصدر باستخدام المصدر [`YAMLRegExpTree`](../sources/yamlregexptree.md)، مع تمرير مسار إلى ملف YAML يحتوي على شجرة التعبير النمطي.

```sql title="Query"
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

يمثل مصدر القاموس [`YAMLRegExpTree`](../sources/yamlregexptree.md) بنية شجرة تعبيرات نمطية. على سبيل المثال:

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

يتكوّن هذا الإعداد من قائمة بعُقد شجرة التعبيرات النمطية. ولكل عقدة البنية التالية:

* **regexp**: التعبير النمطي للعقدة.
* **attributes**: قائمة بسمات القاموس التي يعرّفها المستخدم. في هذا المثال، توجد سمتان: `name` و`version`. تحدد العقدة الأولى كلتا السمتين. أما العقدة الثانية فتحدد السمة `name` فقط. وتوفَّر السمة `version` من خلال العقد الفرعية للعقدة الثانية.
  * قد تحتوي قيمة السمة على **مراجع خلفية** تشير إلى مجموعات الالتقاط في التعبير النمطي المطابِق. في المثال، تتكوّن قيمة السمة `version` في العقدة الأولى من مرجع خلفي `\1` إلى مجموعة الالتقاط `(\d+[\.\d]*)` في التعبير النمطي. وتتراوح أرقام المراجع الخلفية من 1 إلى 9، وتُكتب بالشكل `$1` أو `\1` (للرقم 1). ويُستبدل المرجع الخلفي بمجموعة الالتقاط المطابِقة أثناء تنفيذ الاستعلام.
* **child nodes**: قائمة بالعقد الفرعية لعقدة شجرة `regexp`، ولكل منها سماتها الخاصة وعقدها الفرعية (إن وجدت). تجري مطابقة السلاسل النصية بأسلوب العمق أولًا. وإذا طابقت سلسلة نصية عقدة `regexp`، يتحقق القاموس مما إذا كانت تطابق أيضًا العقد الفرعية التابعة لها. وإذا كان الأمر كذلك، تُسنَد سمات أعمق عقدة مطابِقة. وتستبدل سمات العقدة الفرعية السمات المناظرة لها في العقد الأصلية إذا كانت تحمل الاسم نفسه. ويمكن أن يكون اسم العقد الفرعية في ملفات YAML أي اسم، مثل `versions` في المثال أعلاه.

لا تسمح قواميس شجرة `regexp` بالوصول إلا باستخدام الدوال `dictGet` و`dictGetOrDefault` و`dictGetAll`. على سبيل المثال:

```sql title="Query"
SELECT dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024');
```

```text title="Response"
┌─dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024')─┐
│ ('Android','12')                                                │
└─────────────────────────────────────────────────────────────────┘
```

في هذه الحالة، نطابق أولًا التعبير النمطي `\d+/tclwebkit(?:\d+[\.\d]*)` في العقدة الثانية من الطبقة العليا.
ثم يواصل القاموس البحث في العقد الفرعية، ويجد أن السلسلة تطابق أيضًا `3[12]/tclwebkit`.
ونتيجةً لذلك، تكون قيمة السمة `name` هي `Android` (محددة في الطبقة الأولى)، وتكون قيمة السمة `version` هي `12` (محددة في العقدة الفرعية).

باستخدام ملف إعدادات YAML متقدم، يمكنك استخدام قواميس شجرة التعبيرات النمطية كمحلل لسلسلة وكيل المستخدم.
يدعم ClickHouse ‏[uap-core](https://github.com/ua-parser/uap-core)، ويمكنك الاطلاع على كيفية استخدامه في الاختبار الوظيفي [02504&#95;regexp&#95;dictionary&#95;ua&#95;parser](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/02504_regexp_dictionary_ua_parser.sh)

<div id="collecting-attribute-values">
  ### جمع قيم السمات
</div>

أحيانًا يكون من المفيد إرجاع قيم من عدة تعبيرات نمطية تمت مطابقتها، بدلًا من إرجاع قيمة العقدة الورقية فقط. في هذه الحالات، يمكن استخدام الدالة المتخصصة [`dictGetAll`](/ar/sql-reference/functions/ext-dict-functions.md#dictGetAll). إذا كانت للعقدة قيمة سمة من النوع `T`، فستُرجع `dictGetAll` قيمة من النوع `Array(T)` تحتوي على صفر أو أكثر من القيم.

بشكل افتراضي، لا يكون عدد المطابقات المُعادة لكل مفتاح مقيّدًا. ويمكن تمرير حدّ اختياري باعتباره الوسيط الرابع إلى `dictGetAll`. تُعبَّأ المصفوفة وفق *الترتيب الطوبولوجي*، ما يعني أن العقد الابنة تأتي قبل العقد الأصل، وأن العقد الشقيقة تتبع الترتيب الوارد في المصدر.

مثال:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    tag String,
    topological_index Int64,
    captured Nullable(String),
    parent String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0)
```

```yaml
# /var/lib/clickhouse/user_files/regexp_tree.yaml
- regexp: 'clickhouse\.com'
  tag: 'ClickHouse'
  topological_index: 1
  paths:
    - regexp: 'clickhouse\.com/docs(.*)'
      tag: 'ClickHouse Documentation'
      topological_index: 0
      captured: '\1'
      parent: 'ClickHouse'

- regexp: '/docs(/|$)'
  tag: 'Documentation'
  topological_index: 2

- regexp: 'github.com'
  tag: 'GitHub'
  topological_index: 3
  captured: 'NULL'
```

```sql title="Query"
CREATE TABLE urls (url String) ENGINE=MergeTree ORDER BY url;
INSERT INTO urls VALUES ('clickhouse.com'), ('clickhouse.com/docs/en'), ('github.com/clickhouse/tree/master/docs');
SELECT url, dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2) FROM urls;
```

```text title="Response"
┌─url────────────────────────────────────┬─dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2)─┐
│ clickhouse.com                         │ (['ClickHouse'],[1],[],[])                                                            │
│ clickhouse.com/docs/en                 │ (['ClickHouse Documentation','ClickHouse'],[0,1],['/en'],['ClickHouse'])              │
│ github.com/clickhouse/tree/master/docs │ (['Documentation','GitHub'],[2,3],[NULL],[])                                          │
└────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="matching-modes">
  ### أوضاع المطابقة
</div>

يمكن تعديل سلوك مطابقة الأنماط باستخدام بعض إعدادات القاموس:

* `regexp_dict_flag_case_insensitive`: يستخدم مطابقة غير حساسة لحالة الأحرف (القيمة الافتراضية هي `false`). ويمكن تجاوز ذلك في التعبيرات الفردية باستخدام `(?i)` و `(?-i)`.
* `regexp_dict_flag_dotall`: يسمح للرمز `.` بمطابقة أحرف السطر الجديد (القيمة الافتراضية هي `false`).

<div id="use-regular-expression-tree-dictionary-in-clickhouse-cloud">
  ## استخدام قاموس شجرة التعبيرات النمطية في ClickHouse Cloud
</div>

يعمل المصدر [`YAMLRegExpTree`](../sources/yamlregexptree.md) في ClickHouse Open Source، لكنه لا يعمل في ClickHouse Cloud.
لاستخدام قواميس شجرة التعبيرات النمطية في ClickHouse Cloud، أنشئ أولًا محليًا في ClickHouse Open Source قاموس شجرة تعبيرات نمطية من ملف YAML، ثم أفرغ هذا القاموس في ملف CSV باستخدام دالة الجدول `dictionary` وعبارة [INTO OUTFILE](/ar/sql-reference/statements/select/into-outfile.md).

```sql
SELECT * FROM dictionary(regexp_dict) INTO OUTFILE('regexp_dict.csv')
```

محتوى ملف CSV هو:

```text
1,0,"Linux/(\d+[\.\d]*).+tlinux","['version','name']","['\\1','TencentOS']"
2,0,"(\d+)/tclwebkit(\d+[\.\d]*)","['comment','version','name']","['test $1 and $2','$1','Android']"
3,2,"33/tclwebkit","['version']","['13']"
4,2,"3[12]/tclwebkit","['version']","['12']"
5,2,"3[12]/tclwebkit","['version']","['11']"
6,2,"3[12]/tclwebkit","['version']","['10']"
```

مخطط الملف المُفرَّغ هو:

* `id UInt64`: معرّف عقدة `RegexpTree`.
* `parent_id UInt64`: معرّف العقدة الأصل.
* `regexp String`: سلسلة التعبير النمطي.
* `keys Array(String)`: أسماء السمات المعرّفة من المستخدم.
* `values Array(String)`: قيم السمات المعرّفة من المستخدم.

لإنشاء القاموس في ClickHouse Cloud، أنشئ أولًا جدول `regexp_dictionary_source_table` ببنية الجدول التالية:

```sql
CREATE TABLE regexp_dictionary_source_table
(
    id UInt64,
    parent_id UInt64,
    regexp String,
    keys   Array(String),
    values Array(String)
) ENGINE=Memory;
```

ثم حدِّث ملف CSV المحلي عبر

```bash
clickhouse client \
    --host MY_HOST \
    --secure \
    --password MY_PASSWORD \
    --query "
    INSERT INTO regexp_dictionary_source_table
    SELECT * FROM input ('id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')
    FORMAT CSV" < regexp_dict.csv
```

يمكنك الاطلاع على كيفية [إدراج الملفات المحلية](/ar/integrations/data-ingestion/insert-local-files) لمزيد من التفاصيل. بعد تهيئة جدول المصدر، يمكننا إنشاء RegexpTree انطلاقًا من جدول المصدر:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_dictionary_source_table'))
LIFETIME(0)
LAYOUT(regexp_tree);
```