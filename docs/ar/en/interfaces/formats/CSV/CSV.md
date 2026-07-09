---
alias: []
description: 'توثيق لتنسيق CSV'
input_format: true
keywords: ['CSV']
output_format: true
slug: /interfaces/formats/CSV
title: 'CSV'
doc_type: 'مرجع'
---

<div id="description">
  ## الوصف
</div>

تنسيق القيم المفصولة بفواصل ([RFC](https://tools.ietf.org/html/rfc4180)).
عند التنسيق، تُحاط الصفوف بعلامات اقتباس مزدوجة. وتُخرَج علامة الاقتباس المزدوجة داخل السلسلة النصية على شكل علامتي اقتباس مزدوجتين متتاليتين.
لا توجد قواعد أخرى لإفلات الأحرف.

* تُحاط قيم التاريخ والتاريخ والوقت بعلامات اقتباس مزدوجة.
* تُخرَج الأرقام من دون علامات اقتباس.
* تُفصل القيم بمحرف فاصل، وهو `,` افتراضيًا. ويُحدَّد هذا المحرف في الإعداد [format&#95;csv&#95;delimiter](/ar/operations/settings/settings-formats.md/#format_csv_delimiter).
* تُفصل الصفوف باستخدام محرف تغذية السطر في Unix ‏(LF).
* تُسلسَل المصفوفات في CSV على النحو التالي:
  * أولًا، تُسلسَل المصفوفة إلى سلسلة نصية كما في تنسيق TabSeparated
  * ثم تُخرَج السلسلة النصية الناتجة في CSV بين علامتي اقتباس مزدوجتين.
* تُسلسَل Tuples في تنسيق CSV كأعمدة منفصلة (أي يُفقَد تداخلها داخل الـ tuple).

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
بشكل افتراضي، يكون الفاصل هو `,`
راجع الإعداد [format&#95;csv&#95;delimiter](/ar/operations/settings/settings-formats.md/#format_csv_delimiter) لمزيد من المعلومات.
:::

عند التحليل، يمكن تحليل جميع القيم سواء وُضعت بين علامات اقتباس أم لا. وتُدعَم كل من علامتَي الاقتباس المزدوجة والمفردة.

يمكن أيضًا تنسيق الصفوف من دون علامات اقتباس. في هذه الحالة، تُحلَّل حتى الوصول إلى محرف الفاصل أو محرف نهاية السطر (CR أو LF).
ومع ذلك، وخلافًا لـ RFC، عند تحليل الصفوف من دون علامات اقتباس، تُتجاهل المسافات البادئة واللاحقة وعلامات الجدولة.
تدعم نهاية السطر الأنواع التالية: Unix ‏(LF)، وWindows ‏(CR LF)، وMac OS Classic ‏(CR LF).

تُنسَّق `NULL` وفقًا للإعداد [format&#95;csv&#95;null&#95;representation](/ar/operations/settings/settings-formats.md/#format_csv_null_representation) (القيمة الافتراضية هي `\N`).

في بيانات الإدخال، يمكن تمثيل قيم `ENUM` كأسماء أو كمُعرّفات.
نحاول أولًا مطابقة قيمة الإدخال مع اسم `ENUM`.
إذا تعذّر ذلك وكانت قيمة الإدخال رقمًا، نحاول مطابقة هذا الرقم مع مُعرّف `ENUM`.
إذا كانت بيانات الإدخال تحتوي فقط على مُعرّفات `ENUM`، فمن المستحسن تمكين الإعداد [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/ar/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) لتحسين تحليل `ENUM`.

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| الإعداد                                                                                                                                                                                  | الوصف                                                                                                                           | الافتراضي | ملاحظات                                                                                                                                                                                                           |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [format&#95;csv&#95;delimiter](/ar/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                                           | الحرف الذي يُعدّ فاصلًا في بيانات CSV.                                                                                          | `,`       |                                                                                                                                                                                                                   |
| [format&#95;csv&#95;allow&#95;single&#95;quotes](/ar/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                               | السماح بالسلاسل النصية المحاطة بعلامات اقتباس مفردة.                                                                            | `true`    |                                                                                                                                                                                                                   |
| [format&#95;csv&#95;allow&#95;double&#95;quotes](/ar/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                               | السماح بالسلاسل النصية المحاطة بعلامات اقتباس مزدوجة.                                                                           | `true`    |                                                                                                                                                                                                                   |
| [format&#95;csv&#95;null&#95;representation](/ar/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                                   | تمثيل NULL مخصص في تنسيق CSV.                                                                                                  | `\N`      |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;empty&#95;as&#95;default](/ar/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                                     | التعامل مع الحقول الفارغة في مدخلات CSV على أنها default value.                                                                 | `true`    | بالنسبة إلى تعبيرات القيم الافتراضية المعقدة، يجب أيضًا تفعيل [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ar/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields). |
| [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/ar/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                                         | التعامل مع قيم enum المُدرجة في CSV formats على أنها فهارس enum.                                                                | `false`   |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;use&#95;best&#95;effort&#95;in&#95;schema&#95;inference](/ar/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)   | استخدام بعض التحسينات والاستدلالات infer لاستنتاج schema في تنسيق CSV. وإذا كان معطّلًا، فسيُستنتج كل الحقول على أنها Strings. | `true`    |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;arrays&#95;as&#95;nested&#95;csv](/ar/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                         | عند قراءة Array من CSV، يُفترض أن عناصره قد خضعت لعملية serialization بصيغة CSV متداخلة ثم وُضعت داخل String.                   | `false`   |                                                                                                                                                                                                                   |
| [output&#95;format&#95;csv&#95;crlf&#95;end&#95;of&#95;line](/ar/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                               | إذا ضُبطت على true، فستكون نهاية السطر في output format الخاص بـ CSV هي `\r\n` بدلًا من `\n`.                                   | `false`   |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;skip&#95;first&#95;lines](/ar/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                                     | تخطّي العدد المحدد من الأسطر في بداية البيانات.                                                                                 | `0`       |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;detect&#95;header](/ar/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                               | اكتشاف header الذي يحتوي على الأسماء والأنواع تلقائيًا في تنسيق CSV.                                                           | `true`    |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;skip&#95;trailing&#95;empty&#95;lines](/ar/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                               | تخطّي الأسطر الفارغة اللاحقة في نهاية البيانات.                                                                                 | `false`   |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;trim&#95;whitespaces](/ar/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                                         | إزالة المسافات وعلامات الجدولة من سلاسل CSV غير المحاطة بعلامات اقتباس.                                                         | `true`    |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;allow&#95;whitespace&#95;or&#95;tab&#95;as&#95;delimiter](/ar/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter) | السماح باستخدام المسافة البيضاء أو علامة الجدولة كفاصل بين الحقول في سلاسل CSV.                                                 | `false`   |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;allow&#95;variable&#95;number&#95;of&#95;columns](/ar/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)             | السماح بعدد متغيّر من الأعمدة في تنسيق CSV، مع تجاهل الأعمدة الإضافية واستخدام القيم الافتراضية للأعمدة المفقودة.              | `false`   |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;use&#95;default&#95;on&#95;bad&#95;values](/ar/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                           | السماح بتعيين default value للعمود عند فشل deserialization لحقل CSV بسبب قيمة غير صالحة.                                        | `false`   |                                                                                                                                                                                                                   |
| [input&#95;format&#95;csv&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/ar/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)                 | محاولة استنتاج الأرقام من string fields أثناء schema inference.                                                                 | `false`   |                                                                                                                                                                                                                   |