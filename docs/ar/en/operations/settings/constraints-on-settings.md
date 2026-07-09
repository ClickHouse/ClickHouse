---
description: 'يمكن تعريف القيود على الإعدادات في قسم `profiles` ضمن ملف التهيئة `user.xml`، وهي تمنع المستخدمين من تغيير بعض الإعدادات باستخدام الاستعلام `SET`.'
sidebar_label: 'القيود على الإعدادات'
sidebar_position: 62
slug: /operations/settings/constraints-on-settings
title: 'القيود على الإعدادات'
doc_type: 'reference'
---

<div id="overview">
  ## نظرة عامة
</div>

في ClickHouse، تشير &quot;القيود&quot; المفروضة على الإعدادات إلى الحدود والقواعد التي
يمكنك تعيينها لها. ويمكن تطبيق هذه القيود للحفاظ على
استقرار قاعدة البيانات وأمانها وسلوكها المتوقع.

<div id="defining-constraints">
  ## تحديد القيود
</div>

يمكن تحديد قيود الإعدادات في قسم `profiles` ضمن ملف التهيئة `user.xml`.
وهي تمنع المستخدمين من تغيير بعض الإعدادات باستخدام عبارة
[`SET`](/ar/sql-reference/statements/set).

تُحدَّد القيود كما يلي:

```xml
<profiles>
  <user_name>
    <constraints>
      <setting_name_1>
        <min>lower_boundary</min>
      </setting_name_1>
      <setting_name_2>
        <max>upper_boundary</max>
      </setting_name_2>
      <setting_name_3>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
      </setting_name_3>
      <setting_name_4>
        <readonly/>
      </setting_name_4>
      <setting_name_5>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <changeable_in_readonly/>
      </setting_name_5>
      <setting_name_6>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <disallowed>value1</disallowed>
        <disallowed>value2</disallowed>
        <disallowed>value3</disallowed>
        <changeable_in_readonly/>
      </setting_name_6>
    </constraints>
  </user_name>
</profiles>
```

إذا حاول المستخدم مخالفة القيود، فسيُرفَع استثناء ويظل
الإعداد دون تغيير.

<div id="types-of-constraints">
  ## أنواع القيود
</div>

هناك عدة أنواع من القيود التي يدعمها ClickHouse:

* `min`
* `max`
* `disallowed`
* `readonly` (بالاسم المستعار `const`)
* `changeable_in_readonly`

يحدّد القيدان `min` و`max` الحدين الأدنى والأقصى لإعداد رقمي،
ويمكن استخدامهما معًا.

يمكن استخدام القيد `disallowed` لتحديد قيمة أو قيم معيّنة
لا ينبغي السماح بها لإعداد محدد.

يحدّد القيد `readonly` أو `const` أن المستخدم لا يمكنه تغيير
الإعداد المقابل إطلاقًا.

يتيح نوع القيد `changeable_in_readonly` للمستخدمين تغيير الإعداد
ضمن النطاق `min`/`max` حتى إذا كان الإعداد `readonly` مضبوطًا على `1`،
أما بخلاف ذلك فلا يُسمح بتغيير الإعدادات في وضع `readonly=1`.

:::note
لا يكون `changeable_in_readonly` مدعومًا إلا إذا كان `settings_constraints_replace_previous`
مفعّلًا:

```xml
<access_control_improvements>
  <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
</access_control_improvements>
```

:::

<div id="multiple-constraint-profiles">
  ## ملفات تعريف القيود المتعددة
</div>

إذا كان هناك عدة ملفات تعريف نشطة لمستخدم معيّن، فستُدمَج القيود.
تعتمد عملية الدمج على `settings_constraints_replace_previous`:

* **true** (موصى به): تُستبدل القيود الخاصة بالإعداد نفسه أثناء
  الدمج، بحيث يُستخدم القيد الأخير وتُتجاهل جميع القيود السابقة.
  ويشمل ذلك الحقول غير المُعيّنة في القيد الجديد.
* **false** (افتراضي): تُدمَج القيود الخاصة بالإعداد نفسه بطريقة
  يُؤخذ فيها كل نوع من القيود غير المُعيَّن من ملف التعريف السابق، ويُستبدل كل
  نوع من القيود المُعيَّن بالقيمة من ملف التعريف الجديد.

<div id="read-only">
  ## وضع القراءة فقط
</div>

يُفعَّل وضع القراءة فقط عبر الإعداد `readonly`، ولا ينبغي الخلط بينه
وبين نوع القيد `readonly`:

* `readonly=0`: لا توجد قيود على وضع القراءة فقط.
* `readonly=1`: يُسمح فقط باستعلامات القراءة، ولا يمكن تغيير الإعدادات
  ما لم يتم تعيين `changeable_in_readonly`.
* `readonly=2`: يُسمح فقط باستعلامات القراءة، ولكن يمكن تغيير الإعدادات،
  باستثناء الإعداد `readonly` نفسه.

<div id="example-read-only">
  ### مثال
</div>

ليتضمّن ملف `users.xml` الأسطر التالية:

```xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

ستؤدي جميع الاستعلامات التالية إلى ظهور استثناءات:

```sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

```text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

:::note
يُعامَل ملف التعريف `default` معاملةً خاصة: فجميع القيود المحددة لملف
التعريف `default` تصبح القيود الافتراضية، ولذلك فهي تقيّد جميع المستخدمين
ما لم تُستبدل صراحةً بقيود خاصة لهؤلاء المستخدمين.
:::

<div id="constraints-on-merge-tree-settings">
  ## قيود على إعدادات MergeTree
</div>

يمكن تعيين قيود على [إعدادات MergeTree](merge-tree-settings.md).
تُطبَّق هذه القيود عند إنشاء جدول بمحرك MergeTree
أو عند تعديل إعدادات التخزين الخاصة به.

يجب أن تسبق البادئة `merge_tree_` اسم إعداد MergeTree عند
الإشارة إليه في قسم `<constraints>`.

<div id="example-read-only">
  ### مثال
</div>

يمكنك منع إنشاء جداول جديدة مع تحديد `storage_policy` بشكل صريح

```xml
<profiles>
  <default>
    <constraints>
      <merge_tree_storage_policy>
        <const/>
      </merge_tree_storage_policy>
    </constraints>
  </default>
</profiles>
```