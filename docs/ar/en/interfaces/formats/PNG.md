---
alias: []
description: 'توثيق لصيغة إخراج الصور PNG'
input_format: false
keywords: ['PNG']
output_format: true
slug: /interfaces/formats/PNG
title: 'PNG'
doc_type: 'reference'
---

| الإدخال | الإخراج | اسم بديل |
| ------- | ------- | -------- |
| ✗       | ✔       | ✗        |

<div id="description">
  ## الوصف
</div>

يعرض ناتج الاستعلام على هيئة صورة PNG. ويُعد ذلك مفيدًا كأداة تصور مدمجة.

يُحدَّد حجم الصورة الناتجة بالإعدادين
[`output_format_image_width`](/ar/operations/settings/formats#output_format_image_width) و
[`output_format_image_height`](/ar/operations/settings/formats#output_format_image_height)
(القيمة الافتراضية لكليهما هي 1024). وتُملأ وحدات البكسل التي لا تغطيها النتيجة باللون الأسود
(في وضعي `RGB` ودرجات الرمادي) أو بالأسود الشفاف (في وضع `RGBA`).

يُحدَّد وضع الألوان تلقائيًا استنادًا إلى أسماء الأعمدة وأنواعها في النتيجة:

| الأعمدة             | الوضع                                             |
| ------------------- | ------------------------------------------------- |
| `r`, `g`, `b`       | RGB‏ 8-بت                                         |
| `r`, `g`, `b`, `a`  | RGBA‏ 8-بت                                        |
| `v` من نوع integer  | درجات رمادي 8-بت                                  |
| `v` من نوع `Float*` | درجات رمادي 8-بت (القيم في `[0, 1]` → `[0, 255]`) |
| `v` من نوع `Bool`   | ثنائي (يُعرض كدرجات رمادي 8-بت: `0` أو `255`)     |

تُطابَق أسماء الأعمدة دون مراعاة حالة الأحرف. وإذا تعذر تحديد وضع الألوان
بشكل قاطع (على سبيل المثال، أسماء أعمدة غير معروفة، أو مزج `v` مع `r`/`g`/`b`/`a`، أو غياب أحد `r`/`g`/`b`)،
فإن الاستعلام يطلق استثناءً.

بالنسبة إلى قنوات البكسل، تُقيَّد القيم الصحيحة ضمن `[0, 255]`، أما القيم ذات الفاصلة العائمة
فتُقيَّد ضمن `[0, 1]` ثم تُحوَّل تدريجيًا إلى `[0, 255]`.

يُحدَّد موضع كل سجل في الصورة بأحد وضعين:

* **ضمني** (الافتراضي — عندما لا يكون `x` ولا `y` موجودًا). يقابل كل سجل
  بكسلًا واحدًا؛ وتُملأ وحدات البكسل بترتيب خطوط المسح: من اليسار إلى اليمين، ومن الأعلى إلى الأسفل.
* **صريح** (عندما يكون العمودان `x` و`y` موجودين، وكلاهما من أنواع صحيحة).
  يحدِّد العمودان `x` و`y` إحداثيات البكسل. وتُتجاهل السجلات التي تقع إحداثياتها خارج
  الصورة بصمت. وعند وجود عدة سجلات لها الإحداثيات نفسها،
  تكون الغلبة لآخر سجل (خوارزمية الرسام).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="implicit-rgb">
  ### الإحداثيات الضمنية (صف لكل بكسل)، RGB
</div>

```sql
SELECT
    toUInt8(x * 25) AS r,
    toUInt8(y * 25) AS g,
    toUInt8((x + y) * 12) AS b
FROM
(
    SELECT number % 10 AS x, intDiv(number, 10) AS y FROM numbers(100)
)
INTO OUTFILE 'gradient.png'
FORMAT PNG
SETTINGS output_format_image_width = 10, output_format_image_height = 10;
```

<div id="explicit-grayscale">
  ### إحداثيات صريحة، ودرجات الرمادي
</div>

```sql
SELECT
    toInt32(x) AS x,
    toInt32(y) AS y,
    toUInt8(intensity) AS v
FROM points
INTO OUTFILE 'points.png'
FORMAT PNG
SETTINGS output_format_image_width = 512, output_format_image_height = 512;
```

<div id="terminal-mode">
  ## عرض الصور في الطرفية
</div>

بشكل افتراضي، تكتب صيغة `PNG` بايتات الصورة الخام. ويجعل الإعداد
[`output_format_image_terminal_mode`](/ar/operations/settings/formats#output_format_image_terminal_mode)
الصيغةَ تعرض الصورة مباشرةً في الطرفية باستخدام بروتوكول صور مضمنة بدلًا من ذلك:

| Value              | Behaviour                                                                                                                    |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| &#96;&#96; (empty) | اكتب بايتات الصورة الخام (وهو السلوك الافتراضي).                                                                             |
| `iterm`            | استخدم بروتوكول الصور المضمنة الخاص بـ iTerm2.                                                                               |
| `kitty`            | استخدم بروتوكول الرسوميات الخاص بـ Kitty.                                                                                    |
| `sixel`            | استخدم بروتوكول Sixel. تُختزل الصورة إلى لوحة ألوان ثابتة 6×6×6، وتُركَّب قناة ألفا، إن وُجدت، فوق خلفية سوداء.              |
| `auto`             | إذا كان الناتج طرفية، فاكتشف إمكاناتها واستخدم `iterm` أو `kitty` أو `sixel` (بهذا الترتيب)؛ وإلا فاكتب بايتات الصورة الخام. |

```sql
SELECT toUInt8(x * 25) AS r, toUInt8(y * 25) AS g, toUInt8((x + y) * 12) AS b
FROM (SELECT number % 10 AS x, intDiv(number, 10) AS y FROM numbers(100))
FORMAT PNG
SETTINGS output_format_image_width = 10, output_format_image_height = 10, output_format_image_terminal_mode = 'auto';
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| الإعداد                             | الوصف                                         | القيمة الافتراضية |
| ----------------------------------- | --------------------------------------------- | ----------------- |
| `output_format_image_width`         | عرض الصورة الناتجة بالبكسل.                   | `1024`            |
| `output_format_image_height`        | ارتفاع الصورة الناتجة بالبكسل.                | `1024`            |
| `output_format_image_terminal_mode` | بروتوكول الصورة الطرفية المضمنة (انظر أعلاه). | &#96;&#96; (فارغ) |