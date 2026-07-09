---
alias: []
description: 'وثائق تنسيق DWARF'
input_format: true
keywords: ['DWARF']
output_format: false
slug: /interfaces/formats/DWARF
title: 'DWARF'
doc_type: 'مرجع'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✗       |                |

<div id="description">
  ## الوصف
</div>

تُحلِّل تنسيق `DWARF` رموز تصحيح DWARF من ملف ELF (ملف تنفيذي أو مكتبة أو ملف كائن).
وهي مشابهة لـ `dwarfdump`، لكنها أسرع بكثير (مئات الميغابايت/ثانية) وتدعم SQL.
وتُنتج صفًا واحدًا لكل Debug Information Entry ‏(DIE) في القسم `.debug_info`
وتتضمن إدخالات `null` التي يستخدمها ترميز DWARF لإنهاء قوائم العناصر الفرعية في الشجرة.

:::info
يتكوّن `.debug_info` من *وحدات*، وهي تقابل وحدات الترجمة:

* كل وحدة عبارة عن شجرة من *DIE*s، ويكون `compile_unit` DIE هو الجذر فيها.
* لكل DIE *وسم* وقائمة من *السمات*.
* لكل سمة *اسم* و*قيمة* (وأيضًا *صيغة* تحدد كيفية ترميز القيمة).

تمثل DIEs عناصر من الشيفرة المصدرية، ويُبيّن *الوسم* نوع العنصر الذي تمثله. على سبيل المثال، هناك:

* دوال (الوسم = `subprogram`)
* أصناف/‏structs/‏enums (`class_type`/`structure_type`/`enumeration_type`)
* متغيرات (`variable`)
* معاملات الدالة (`formal_parameter`).

وتعكس بنية الشجرة بنية الشيفرة المصدرية المقابلة. على سبيل المثال، يمكن أن يحتوي `class_type` DIE على `subprogram` DIEs تمثل طرائق الصنف.
:::

تُخرج تنسيق `DWARF` الأعمدة التالية:

* `offset` - موضع DIE في القسم `.debug_info`
* `size` - عدد البايتات في DIE المرمّز (بما في ذلك السمات)
* `tag` - نوع DIE؛ تُحذف البادئة التقليدية &quot;DW&#95;TAG&#95;&quot;
* `unit_name` - اسم وحدة الترجمة التي تحتوي هذا DIE
* `unit_offset` - موضع وحدة الترجمة التي تحتوي هذا DIE في القسم `.debug_info`
* `ancestor_tags` - مصفوفة وسوم أسلاف DIE الحالي في الشجرة، مرتبة من الأقرب إلى الأبعد
* `ancestor_offsets` - إزاحات الأسلاف، بالتوازي مع `ancestor_tags`
* بعض السمات الشائعة المكررة من مصفوفة السمات للتسهيل:
  * `name`
  * `linkage_name` - الاسم الكامل المؤهل بعد التشويه؛ ويوجد عادةً للدوال فقط (ولكن ليس لكل الدوال)
  * `decl_file` - اسم ملف الشيفرة المصدرية الذي صُرّح فيه عن هذا الكيان
  * `decl_line` - رقم السطر في الشيفرة المصدرية الذي صُرّح فيه عن هذا الكيان
* مصفوفات متوازية تصف السمات:
  * `attr_name` - اسم السمة؛ تُحذف البادئة التقليدية &quot;DW&#95;AT&#95;&quot;
  * `attr_form` - كيفية ترميز السمة وتفسيرها؛ تُحذف البادئة التقليدية DW&#95;FORM&#95;
  * `attr_int` - القيمة الصحيحة للسمة؛ وتكون 0 إذا لم تكن للسمة قيمة رقمية
  * `attr_str` - القيمة النصية للسمة؛ وتكون فارغة إذا لم تكن للسمة قيمة نصية

<div id="example-usage">
  ## مثال على الاستخدام
</div>

يمكن استخدام تنسيق `DWARF` للعثور على وحدات الترجمة التي تضم أكبر عدد من تعريفات الدوال (بما في ذلك تخصيصات القوالب والدوال الواردة من ملفات الترويسة المضمَّنة):

```sql title="Query"
SELECT
    unit_name,
    count() AS c
FROM file('programs/clickhouse', DWARF)
WHERE tag = 'subprogram' AND NOT has(attr_name, 'declaration')
GROUP BY unit_name
ORDER BY c DESC
LIMIT 3
```

```text title="Response"
┌─unit_name──────────────────────────────────────────────────┬─────c─┐
│ ./src/Core/Settings.cpp                                    │ 28939 │
│ ./src/AggregateFunctions/AggregateFunctionSumMap.cpp       │ 23327 │
│ ./src/AggregateFunctions/AggregateFunctionUniqCombined.cpp │ 22649 │
└────────────────────────────────────────────────────────────┴───────┘

3 rows in set. Elapsed: 1.487 sec. Processed 139.76 million rows, 1.12 GB (93.97 million rows/s., 752.77 MB/s.)
Peak memory usage: 271.92 MiB.
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
