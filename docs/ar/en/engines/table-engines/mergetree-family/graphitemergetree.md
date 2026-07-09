---
description: 'مصمم لتقليل بيانات Graphite وتجميعها/حساب متوسطاتها (rollup).'
sidebar_label: 'GraphiteMergeTree'
sidebar_position: 90
slug: /engines/table-engines/mergetree-family/graphitemergetree
title: 'محرك جدول GraphiteMergeTree'
doc_type: 'guide'
---

صُمم هذا المحرك لتقليل بيانات [Graphite](http://graphite.readthedocs.io/en/latest/index.html) وتجميعها/حساب متوسطاتها (rollup). وقد يكون مفيدًا للمطورين الذين يريدون استخدام ClickHouse كمخزن بيانات لـ Graphite.

يمكنك استخدام أي محرك جدول في ClickHouse لتخزين بيانات Graphite إذا لم تكن بحاجة إلى rollup، ولكن إذا كنت بحاجة إليه فاستخدم `GraphiteMergeTree`. يقلّل هذا المحرك من حجم التخزين ويزيد من كفاءة الاستعلامات الواردة من Graphite.

يرث هذا المحرك خصائص [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).

<div id="creating-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value Float64,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

اطّلع على وصف مفصل لتعليمة [CREATE TABLE](/ar/sql-reference/statements/create/table).

يجب أن يحتوي جدول بيانات Graphite على الأعمدة التالية:

* اسم المقياس (Graphite sensor). نوع البيانات: `String`.

* وقت قياس المقياس. نوع البيانات: `DateTime`.

* قيمة المقياس. نوع البيانات: `Float64`.

* إصدار المقياس. نوع البيانات: أي نوع رقمي (يحفظ ClickHouse الصفوف ذات الإصدار الأعلى، أو آخر صف تمت كتابته إذا كانت الإصدارات متساوية. وتُحذف الصفوف الأخرى أثناء دمج أجزاء البيانات).

يجب تحديد أسماء هذه الأعمدة في تهيئة rollup.

**معلمات GraphiteMergeTree**

* `config_section` — اسم القسم في ملف التهيئة الذي تُحدَّد فيه قواعد rollup.

**بنود الاستعلام**

عند إنشاء جدول `GraphiteMergeTree`، تكون [البنود](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) نفسها مطلوبة كما عند إنشاء جدول `MergeTree`.

<details markdown="1">
  <summary>طريقة مهجورة لإنشاء جدول</summary>

  :::note
  لا تستخدم هذه الطريقة في المشاريع الجديدة، وحوّل المشاريع القديمة إلى الطريقة الموضحة أعلاه إن أمكن.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      EventDate Date,
      Path String,
      Time DateTime,
      Value Float64,
      Version <Numeric_type>
      ...
  ) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
  ```

  جميع المعلمات، باستثناء `config_section`، لها المعنى نفسه كما في `MergeTree`.

  * `config_section` — اسم القسم في ملف التهيئة الذي تُحدَّد فيه قواعد rollup.
</details>

<div id="rollup-configuration">
  ## تهيئة rollup
</div>

تُحدَّد إعدادات rollup بواسطة المعلمة [graphite&#95;rollup](../../../operations/server-configuration-parameters/settings.md#graphite) في تهيئة الخادم. ويمكن أن يكون اسم المعلمة أيًّا كان. يمكنك إنشاء عدة تهيئات واستخدامها مع جداول مختلفة.

بنية تهيئة rollup:

required-columns
patterns

<div id="required-columns">
  ### الأعمدة المطلوبة
</div>

<div id="path_column_name">
  #### `path_column_name`
</div>

`path_column_name` — اسم العمود الذي يخزّن اسم المقياس (Graphite sensor). القيمة الافتراضية: `Path`.

<div id="time_column_name">
  #### `time_column_name`
</div>

`time_column_name` — اسم العمود الذي يخزّن وقت قياس المقياس. القيمة الافتراضية: `Time`.

<div id="value_column_name">
  #### `value_column_name`
</div>

`value_column_name` — اسم العمود الذي يخزّن قيمة المقياس عند الوقت المحدَّد في `time_column_name`. القيمة الافتراضية: `Value`.

<div id="version_column_name">
  #### `version_column_name`
</div>

`version_column_name` — اسم العمود الذي يخزّن إصدار المقياس. القيمة الافتراضية: `Timestamp`.

<div id="patterns">
  ### الأنماط
</div>

بنية القسم `patterns`:

```text
pattern
    rule_type
    regexp
    function
pattern
    rule_type
    regexp
    age + precision
    ...
pattern
    rule_type
    regexp
    function
    age + precision
    ...
pattern
    ...
default
    function
    age + precision
    ...
```

:::important
يجب ترتيب الأنماط ترتيبًا صارمًا كما يلي:

1. الأنماط التي لا تحتوي على `function` أو `retention`.
2. الأنماط التي تحتوي على كلٍّ من `function` و`retention`.
3. النمط `default`.
   :::

عند معالجة صف، يتحقق ClickHouse من القواعد في أقسام `pattern`. يمكن لكل قسم من أقسام `pattern` (بما في ذلك `default`) أن يحتوي على معلمة `function` للتجميع، أو معلمات `retention`، أو كليهما. إذا طابق اسم المقياس `regexp`، فستُطبَّق القواعد من قسم `pattern` (أو الأقسام)؛ وإلا فستُستخدم القواعد من قسم `default`.

الحقول الخاصة بأقسام `pattern` و`default`:

* `rule_type` - نوع القاعدة. لا يُطبَّق إلا على مقاييس معيّنة. يستخدمه المحرك للفصل بين المقاييس العادية والمقاييس ذات الوسوم. معلمة اختياري. قيمة افتراضية: `all`.
  لا تكون هناك حاجة إليه عندما لا يكون الأداء بالغ الأهمية، أو عند استخدام نوع واحد فقط من المقاييس، مثل plain metrics. افتراضيًا، لا يتم إنشاء سوى مجموعة واحدة من القواعد. أمّا إذا تم تعريف أي نوع من الأنواع الخاصة، فسيتم إنشاء مجموعتين مختلفتين: واحدة للمقاييس العادية (root.branch.leaf) وأخرى للمقاييس ذات الوسوم (root.branch.leaf;tag1=value1).
  وتُدرَج القواعد الافتراضية في كلتا المجموعتين.
  القيم الصالحة:
  * `all` (الافتراضي) - قاعدة عامة تُستخدم عند حذف `rule_type`.
  * `plain` - قاعدة للمقاييس العادية. يُعالَج الحقل `regexp` على أنه regular expression.
  * `tagged` - قاعدة للمقاييس ذات الوسوم (تُخزَّن المقاييس في DB بالتنسيق `someName?tag1=value1&tag2=value2&tag3=value3`). يجب أن يكون regular expression مرتبًا حسب أسماء الوسوم، ويجب أن يكون الوسم الأول هو `__name__` إذا كان موجودًا. يُعالَج الحقل `regexp` على أنه regular expression.
  * `tag_list` - قاعدة للمقاييس ذات الوسوم، وهي DSL بسيطة لتسهيل وصف المقاييس بتنسيق graphite: `someName;tag1=value1;tag2=value2` أو `someName` أو `tag1=value1;tag2=value2`. يُحوَّل الحقل `regexp` إلى قاعدة `tagged`. ولا حاجة إلى الفرز حسب أسماء الوسوم، إذ سيُجرى ذلك تلقائيًا. ويمكن أن تكون قيمة الوسم (وليس اسمه) regular expression، مثل `env=(dev|staging)`.
* `regexp` – نمط لاسم المقياس (regular expression أو DSL).
* `age` – الحد الأدنى لعمر البيانات بالثواني.
* `precision`– مدى الدقة في تحديد عمر البيانات بالثواني. يجب أن يكون divisor للعدد 86400 (عدد الثواني في اليوم).
* `function` – اسم دالة التجميع التي ستُطبَّق على البيانات التي يقع عمرها ضمن النطاق `[age, age + precision]`. الدوال المقبولة: min / max / any / avg. ويُحسَب المتوسط بشكل غير دقيق، مثل متوسط المتوسطات.

<div id="configuration-example">
  ### مثال على التهيئة بدون أنواع القواعد
</div>

```xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

<div id="configuration-typed-example">
  ### مثال على التهيئة لأنواع القواعد
</div>

```xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <rule_type>plain</rule_type>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)min\?</regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp><![CDATA[^someName\?(.*&)*tag1=value1(&|$)]]></regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tag_list</rule_type>
        <regexp>someName;tag2=value2</regexp>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

:::note
يُجرى التجميع التراكمي للبيانات أثناء عمليات الدمج. وعادةً لا تبدأ عمليات الدمج للأقسام القديمة، لذا يجب، لإجراء التجميع التراكمي، تحفيز عملية دمج غير مجدولة باستخدام [optimize](../../../sql-reference/statements/optimize.md). أو يمكنك استخدام أدوات إضافية، مثل [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer).
:::