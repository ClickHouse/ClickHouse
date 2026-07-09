---
description: 'توثيق لتعديلات إعدادات الجدول'
sidebar_label: 'SETTING'
sidebar_position: 38
slug: /sql-reference/statements/alter/setting
title: 'تعديلات إعدادات الجدول'
doc_type: 'مرجع'
---

توجد مجموعة من الاستعلامات لتغيير إعدادات الجدول. يمكنك تعديل الإعدادات أو إعادة تعيينها إلى القيم الافتراضية. ويمكن لاستعلام واحد تغيير عدة إعدادات في الوقت نفسه.
إذا لم يكن هناك إعداد بالاسم المحدد، فسيؤدي الاستعلام إلى ظهور استثناء.

**البنية**

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY|RESET SETTING ...
```

:::note
لا تُطبَّق هذه الاستعلامات إلا على جداول [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) فقط.
:::

<div id="modify-setting">
  ## MODIFY SETTING
</div>

يُعدّل إعدادات الجدول.

**الصيغة**

```sql
MODIFY SETTING setting_name=value [, ...]
```

**مثال**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id;

ALTER TABLE example_table MODIFY SETTING max_part_loading_threads=8, max_parts_in_total=50000;
```

<div id="reset-setting">
  ## RESET SETTING
</div>

يعيد تعيين إعدادات الجدول إلى قيمها الافتراضية. وإذا كان أحد الإعدادات مضبوطًا بالفعل على القيمة الافتراضية، فلا يُتخذ أي إجراء.

**الصيغة**

```sql
RESET SETTING setting_name [, ...]
```

**مثال**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id
    SETTINGS max_part_loading_threads=8;

ALTER TABLE example_table RESET SETTING max_part_loading_threads;
```

**انظر أيضًا**

* [إعدادات MergeTree](../../../operations/settings/merge-tree-settings.md)