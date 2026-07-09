---
description: 'توثيق SETTINGS PROFILE'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 48
slug: /sql-reference/statements/alter/settings-profile
title: 'ALTER SETTINGS PROFILE'
doc_type: 'reference'
---

يعدّل ملفات تعريف الإعدادات.

الصيغة:

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL SETTINGS]
    [DROP ALL PROFILES]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

تتيح عبارة `ON CLUSTER` تعديل ملفات تعريف الإعدادات على مستوى العنقود، راجع [DDL الموزع](../../../sql-reference/distributed-ddl.md).

<div id="replacing-vs-modifying">
  ## استبدال الإعدادات أم تعديلها
</div>

يدعم `ALTER SETTINGS PROFILE` طريقتين مختلفتين لتغيير الإعدادات وملفات التعريف الموروث منها لملف تعريف معيّن. ويختلف سلوك كل منهما كثيرًا، لذا من المهم اختيار الطريقة المناسبة.

<div id="replacing-form">
  ### صيغة الاستبدال: عبارة `SETTINGS` / `INHERIT` المجرّدة
</div>

تستبدل عبارة `SETTINGS` المجرّدة (من دون `ADD` أو `MODIFY` أو `DROP`) **قائمة الإعدادات بالكامل وجميع ملفات التعريف الأصلية** بما تدرجه تحديدًا. ويُحذف بصمت أي شيء كان موجودًا سابقًا ولم يُدرج — ولا يوجد أي تحذير.

```sql
CREATE SETTINGS PROFILE OR REPLACE p
    SETTINGS max_execution_time = 10, enable_lazy_columns_replication = 1;

ALTER SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360;

SHOW CREATE SETTINGS PROFILE p;
-- → CREATE SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360
-- max_execution_time and enable_lazy_columns_replication are gone.
```

:::warning
لأن صيغة `SETTINGS` المجرّدة تُعدّ استبدالًا كاملًا، فإن استخدامها من أجل &quot;تجاوز إعداد واحد&quot; فوق ملف إعدادات أساسي مُعبّأ مسبقًا سيؤدي إلى حذف كل إعداد آخر (وجميع ملفات التعريف الأصلية) من ذلك الملف. إذا كنت تريد فقط تغيير إعداد واحد مع الإبقاء على الباقي، فاستخدم الصيغة التزايدية `MODIFY`/`ADD`/`DROP` الموضحة أدناه.
:::

هذا هو السلوك نفسه لـ `SETTINGS` في [`CREATE SETTINGS PROFILE`](../create/settings-profile.md): إذ تحدد العبارة قائمة الإعدادات الكاملة.

<div id="incremental-form">
  ### الصيغة التزايدية: `ADD` / `MODIFY` / `DROP`
</div>

تُغيّر الكلمات المفتاحية `ADD` و`MODIFY` و`DROP` الإدخالات الفردية مع ترك كل ما عدا ذلك في ملف التعريف دون تغيير:

* `ADD SETTINGS variable = value [constraints]` — يضيف إعدادًا غير موجود بعد.
* `MODIFY SETTINGS variable = value [constraints]` — يستبدل إدخال إعداد واحد. ويُستبدل الإدخال بالكامل (القيمة والقيود)، لذا أعد تحديد `MIN`/`MAX`/`READONLY`/إلخ إذا كنت تريد الاحتفاظ بها.
* `DROP SETTINGS variable [,...]` — يزيل الإعدادات المذكورة.
* `ADD PROFILES 'profile_name' [,...]` / `DROP PROFILES 'profile_name' [,...]` — يضيف أو يزيل ملفات التعريف الأصلية (الموروثة).
* `DROP ALL SETTINGS` / `DROP ALL PROFILES` — يزيل جميع الإعدادات أو جميع ملفات التعريف الأصلية.

يمكن دمج عدة بنود من هذه في عبارة واحدة، على سبيل المثال `DROP SETTINGS a ADD SETTINGS b = 1`.

`SET variable = value` هو اسم مستعار لـ `MODIFY SETTINGS variable = value`. وهو متاح لأن `SET` يبدو طبيعيًا، ولأن كتابة عبارة `SETTINGS` الخاصة بالاستبدال عندما يكون المقصود تغييرًا تزايديًا هي خطأ شائع.

<div id="examples">
  ## أمثلة
</div>

تجاوز إعدادًا واحدًا مع الإبقاء على بقية إعدادات ملف التعريف المُعدّ مسبقًا:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 16106127360;
```

أضِف إعدادًا مقيّدًا جديدًا واحذف إعدادًا آخر:

```sql
ALTER SETTINGS PROFILE my_profile
    DROP SETTINGS readonly
    ADD SETTINGS max_threads = 8 MIN 4 MAX 16 WRITABLE;
```

أدِر ملفات التعريف الأصلية تدريجيًا:

```sql
ALTER SETTINGS PROFILE my_profile ADD PROFILES p1;
ALTER SETTINGS PROFILE my_profile DROP PROFILES p1;
```

تحقّق دائمًا من النتيجة باستخدام [`SHOW CREATE SETTINGS PROFILE`](../show.md):

```sql
SHOW CREATE SETTINGS PROFILE my_profile;
```

<div id="incremental-vs-full-replacement">
  ## التعديل التزايدي مقابل الاستبدال الكامل
</div>

:::warning
تؤدي عبارة `SETTINGS` وحدها إلى **إزالة جميع الإعدادات الحالية وجميع ملفات التعريف الموروثة (الأصل)** من ملف التعريف قبل تطبيق الإعدادات الجديدة.
:::

لتغيير إعداد واحد مع الإبقاء على بقية الإعدادات، استخدم `ADD SETTINGS` أو `MODIFY SETTINGS` (انظر الأمثلة أدناه).

<div id="add-vs-modify">
  ## ADD vs MODIFY
</div>

يحافظ كلٌّ من `ADD SETTINGS` و`MODIFY SETTINGS` على الإعدادات الأخرى في ملف التعريف، لكنهما يتعاملان بشكل مختلف مع إدخالٍ موجود للإعداد *نفسه*:

* `ADD SETTINGS variable = value ...` يحذف أولًا أي إدخال موجود لـ `variable` ثم يُدرِج الإدخال الجديد. لذا فهو **يستبدل القيمة مع جميع القيود** الخاصة بذلك الإعداد. وأي قيم `MIN` أو `MAX` أو حالة قابلية الكتابة (`READONLY`/`WRITABLE`/`CONST`/`CHANGEABLE_IN_READONLY`) كانت معرّفة مسبقًا لـ `variable` ولا تعيد ذكرها، تُزال.
* `MODIFY SETTINGS variable = value ...` **يدمج على مستوى كل حقل**: فهو يتجاوز فقط الحقول التي تحددها فعليًا (القيمة، أو `MIN`، أو `MAX`، أو قابلية الكتابة)، ويُبقي الحقول الأخرى لذلك الإعداد كما هي.

:::tip
باختصار، استخدم `MODIFY SETTINGS` عندما تريد فقط تعديل جانب واحد من الإعداد (مثل تغيير القيمة فقط مع الإبقاء على `MAX` الحالي)؛ واستخدم `ADD SETTINGS` عندما تريد إعادة تعريف الإعداد بالكامل من البداية.
:::

<div id="examples">
  ## أمثلة
</div>

أنشئ ملف تعريف لاستخدامه في الأمثلة التالية:

```sql
CREATE SETTINGS PROFILE OR REPLACE p SETTINGS max_execution_time = 60;
```

<div id="example-modify-settings">
  ### MODIFY SETTINGS
</div>

أضِف إعدادًا واحدًا أو عدّله مع الاحتفاظ ببقية الإعدادات:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000;
SHOW CREATE SETTINGS PROFILE p;
-- CREATE SETTINGS PROFILE p SETTINGS
--     max_execution_time = 60,
--     max_memory_usage = 20000000000
```

لأن `MODIFY` يدمج الحقول حقلًا بحقل، فإن تغيير قيمة أحد الإعدادات فقط يُبقي على قيوده الحالية:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000 MAX 30000000000;
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 25000000000;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_memory_usage = 25000000000 MAX 30000000000  -- the MAX constraint is preserved
```

<div id="example-add-settings">
  ### ADD SETTINGS
</div>

أضِف إعدادًا مع الإبقاء على الإعدادات الأخرى أيضًا، وأعِد تعريفه بالكامل إذا كان موجودًا مسبقًا:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 8 MAX 16 READONLY;
```

بخلاف `MODIFY`، فإن إعادة تنفيذ `ADD` بقيمة فقط تُسقط القيود المحددة مسبقًا لهذا الإعداد:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 4;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_threads = 4   -- the MAX and READONLY constraints are gone
```

<div id="example-drop-settings">
  ### DROP SETTINGS
</div>

أزل إعدادًا واحدًا أو أكثر بالاسم:

```sql
ALTER SETTINGS PROFILE p DROP SETTINGS max_threads;
```

أزل جميع الإعدادات دفعةً واحدةً:

```sql
ALTER SETTINGS PROFILE p DROP ALL SETTINGS;
```

<div id="example-profiles">
  ### العمل مع ملفات التعريف الموروثة
</div>

أضف ملفات التعريف الأصلية (الموروثة) أو أزلها دون التأثير في إعدادات ملف التعريف نفسه:

```sql
ALTER SETTINGS PROFILE p ADD PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP ALL PROFILES;
```