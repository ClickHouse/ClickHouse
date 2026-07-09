---
alias: ['TSV']
description: 'توثيق تنسيق TSV'
input_format: true
keywords: ['TabSeparated', 'TSV']
output_format: true
slug: /interfaces/formats/TabSeparated
title: 'TabSeparated'
doc_type: 'مرجع'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✔       | `TSV`          |

<div id="description">
  ## الوصف
</div>

في تنسيق TabSeparated، تُكتب البيانات صفًا تلو الآخر. يحتوي كل صف على قيم تفصل بينها علامات تبويب. وتتبع كلَّ قيمة علامةُ تبويب، باستثناء القيمة الأخيرة في الصف، إذ يتبعها محرف تغذية سطر. ويُفترض استخدام محارف تغذية السطر الخاصة بـ Unix حصراً في جميع المواضع. ويجب أيضًا أن ينتهي الصف الأخير بمحرف تغذية سطر. وتُكتب القيم بتنسيق نصي، من دون علامات اقتباسٍ محيطة، مع إفلات المحارف الخاصة.

يتوفر هذا التنسيق أيضًا تحت الاسم `TSV`.

يُعد تنسيق `TabSeparated` مناسبًا لمعالجة البيانات باستخدام برامج مخصصة وبرامج نصية. ويُستخدم افتراضيًا في واجهة HTTP، وفي وضع الدفعات لعميل سطر الأوامر. كما يتيح هذا التنسيق نقل البيانات بين أنظمة إدارة قواعد البيانات المختلفة. على سبيل المثال، يمكنك الحصول على ملف dump من MySQL وتحميله إلى ClickHouse، أو العكس.

يدعم تنسيق `TabSeparated` إخراج القيم الإجمالية (عند استخدام WITH TOTALS) والقيم القصوى (عندما تُضبط &#39;extremes&#39; على 1). وفي هذه الحالات، تُخرَج القيم الإجمالية والقيم القصوى بعد البيانات الرئيسية. ويُفصل بين النتيجة الرئيسية والقيم الإجمالية والقيم القصوى بسطر فارغ. مثال:

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated

2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

<div id="tabseparated-data-formatting">
  ## تنسيق البيانات
</div>

تُكتب الأعداد الصحيحة بالصيغة العشرية. ويمكن أن تبدأ الأعداد بالمحرف الإضافي `+` (ويُتجاهل عند التحليل، ولا يُسجَّل عند التنسيق). ولا يجوز أن تحتوي الأعداد غير السالبة على إشارة السالب. وعند القراءة، يُسمح بتحليل سلسلة فارغة على أنها صفر، أو (بالنسبة إلى الأنواع الموقَّعة) سلسلة لا تتكوّن إلا من إشارة ناقص على أنها صفر. وقد تُحلَّل الأعداد التي لا تتسع لها نوع البيانات المقابل إلى عدد مختلف، من دون ظهور رسالة خطأ.

تُكتب الأعداد ذات الفاصلة العائمة بالصيغة العشرية. وتُستخدم النقطة فاصلاً عشريًا. كما أن الصيغ الأسّية مدعومة، وكذلك &#39;inf&#39; و&#39;+inf&#39; و&#39;-inf&#39; و&#39;nan&#39;. ويمكن أن يبدأ تمثيل العدد ذي الفاصلة العائمة بنقطة عشرية أو ينتهي بها.
أثناء التنسيق، قد تُفقد الدقة في الأعداد ذات الفاصلة العائمة.
وأثناء التحليل، لا يُشترط بدقة قراءة أقرب عدد يمكن للآلة تمثيله.

تُكتب التواريخ بتنسيق YYYY-MM-DD وتُحلَّل بالتنسيق نفسه، مع السماح بأي محارف بوصفها فواصل.
وتُكتب التواريخ المصحوبة بوقت بالتنسيق `YYYY-MM-DD hh:mm:ss` وتُحلَّل بالتنسيق نفسه، مع السماح بأي محارف بوصفها فواصل.
ويحدث كل ذلك وفق المنطقة الزمنية للنظام وقت بدء تشغيل العميل أو الخادم (بحسب الجهة التي تنسّق البيانات). أمّا بالنسبة إلى التواريخ المصحوبة بوقت، فلا يكون التوقيت الصيفي محددًا. لذا، إذا كان التفريغ يحتوي على أوقات تقع ضمن التوقيت الصيفي، فلن يطابق التفريغ البيانات بشكل لا لبس فيه، وسيختار التحليل أحد الوقتين.
وأثناء عملية القراءة، يمكن تحليل التواريخ غير الصحيحة والتواريخ المصحوبة بوقت مع تجاوز طبيعي أو كتواريخ وأوقات فارغة، من دون رسالة خطأ.

واستثناءً من ذلك، يُدعَم أيضًا تحليل التواريخ المصحوبة بوقت بتنسيق Unix timestamp، إذا كان يتكوّن من 10 أرقام عشرية بالضبط. ولا تعتمد النتيجة على المنطقة الزمنية. ويجري التمييز تلقائيًا بين التنسيقين `YYYY-MM-DD hh:mm:ss` و`NNNNNNNNNN`.

تُخرَج السلاسل النصية مع إفلات المحارف الخاصة باستخدام الشرطة المائلة العكسية. وتُستخدم تسلسلات الهروب التالية في الإخراج: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. ويدعم التحليل أيضًا التسلسلات `\a` و`\v` و`\xHH` (تسلسلات إفلات سداسية عشرية) وأي تسلسلات `\c`، حيث تكون `c` أي محرف (وتُحوَّل هذه التسلسلات إلى `c`). لذلك، تدعم قراءة البيانات التنسيقات التي يمكن فيها كتابة محرف تغذية سطر على هيئة `\n` أو `\` أو كسطر جديد فعلي. على سبيل المثال، يمكن تحليل السلسلة `Hello world` التي تحتوي على سطر جديد بين الكلمتين بدلًا من المسافة بأيٍّ من الصيغ التالية:

```text
Hello\nworld

Hello\
world
```

المتغير الثاني مدعوم لأن MySQL يستخدمه عند كتابة ملفات dump مفصولة بعلامات الجدولة.

الحد الأدنى من المحارف التي تحتاج إلى إفلاتها عند تمرير البيانات بتنسيق TabSeparated هو: علامة الجدولة، ومحرف تغذية سطر ‏(LF)، والشرطة المائلة العكسية.

لا يُفلت إلا عدد قليل من الرموز. وقد تصادف بسهولة قيمة String يفسدها الطرفي لديك عند الإخراج.

تُكتب المصفوفات على شكل قائمة من القيم المفصولة بفواصل داخل `[]`. وتُنسَّق العناصر الرقمية في المصفوفة بشكل عادي. وتُكتب الأنواع `Date` و`DateTime` بين علامات اقتباس مفردة. وتُكتب Strings بين علامات اقتباس مفردة مع تطبيق قواعد الإفلات نفسها المذكورة أعلاه.

تُنسَّق [NULL](/ar/sql-reference/syntax.md) وفقًا للإعداد [format&#95;tsv&#95;null&#95;representation](/ar/operations/settings/settings-formats.md/#format_tsv_null_representation) (القيمة الافتراضية هي `\N`).

في بيانات الإدخال، يمكن تمثيل قيم ENUM كأسماء أو كمعرّفات. أولًا، نحاول مطابقة قيمة الإدخال مع اسم ENUM. وإذا لم تنجح المطابقة وكانت قيمة الإدخال رقمًا، نحاول مطابقة هذا الرقم مع معرّف ENUM.
إذا كانت بيانات الإدخال تحتوي على معرّفات ENUM فقط، فيُوصى بتمكين الإعداد [input&#95;format&#95;tsv&#95;enum&#95;as&#95;number](/ar/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) لتحسين تحليل ENUM.

يُمثَّل كل عنصر من البُنى [Nested](/ar/sql-reference/data-types/nested-data-structures/index.md) على شكل مصفوفة.

على سبيل المثال:

```sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

```sql
INSERT INTO nestedt VALUES ( 1, [1], ['a'])
```

```sql
SELECT * FROM nestedt FORMAT TSV
```

```response
1  [1]    ['a']
```

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-data">
  ### إدراج البيانات
</div>

باستخدام ملف TSV التالي المسمّى `football.tsv`:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

أدخِل البيانات:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparated;
```

<div id="reading-data">
  ### قراءة البيانات
</div>

اقرأ البيانات بتنسيق `TabSeparated`:

```sql
SELECT *
FROM football
FORMAT TabSeparated
```

سيكون الإخراج بتنسيق مفصول بعلامات الجدولة:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| الإعداد                                                                                                                                                  | الوصف                                                                                                                                                                                                                                                                | الافتراضي |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| [`format_tsv_null_representation`](/ar/operations/settings/settings-formats.md/#format_tsv_null_representation)                                             | تمثيل مخصص لـ NULL في تنسيق TSV.                                                                                                                                                                                                                                     | `\N`      |
| [`input_format_tsv_empty_as_default`](/ar/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default)                                       | اعتبر الحقول الفارغة في مدخلات TSV قيماً افتراضية. بالنسبة إلى تعبيرات القيم الافتراضية المعقدة، يجب أيضاً تمكين [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ar/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields). | `false`   |
| [`input_format_tsv_enum_as_number`](/ar/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)                                           | اعتبر قيم enum المُدخلة في تنسيقات TSV على أنها فهارس enum.                                                                                                                                                                                                          | `false`   |
| [`input_format_tsv_use_best_effort_in_schema_inference`](/ar/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) | استخدم بعض التحسينات والاستدلالات لاستنتاج المخطط في تنسيق TSV. إذا كان هذا الإعداد معطلاً، فسيتم استنتاج جميع الحقول على أنها سلاسل نصية.                                                                                                                           | `true`    |
| [`output_format_tsv_crlf_end_of_line`](/ar/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line)                                     | إذا تم تعيينه إلى true، فستكون نهاية السطر في تنسيق إخراج TSV هي `\r\n` بدلاً من `\n`.                                                                                                                                                                               | `false`   |
| [`input_format_tsv_crlf_end_of_line`](/ar/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line)                                       | إذا تم تعيينه إلى true، فستكون نهاية السطر في تنسيق إدخال TSV هي `\r\n` بدلاً من `\n`.                                                                                                                                                                               | `false`   |
| [`input_format_tsv_skip_first_lines`](/ar/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)                                       | تخطَّ العدد المحدد من الأسطر في بداية البيانات.                                                                                                                                                                                                                      | `0`       |
| [`input_format_tsv_detect_header`](/ar/operations/settings/settings-formats.md/#input_format_tsv_detect_header)                                             | اكتشف تلقائياً صف الرأس الذي يحتوي على الأسماء والأنواع في تنسيق TSV.                                                                                                                                                                                                | `true`    |
| [`input_format_tsv_skip_trailing_empty_lines`](/ar/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines)                     | تخطَّ الأسطر الفارغة في نهاية البيانات.                                                                                                                                                                                                                              | `false`   |
| [`input_format_tsv_allow_variable_number_of_columns`](/ar/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns)       | اسمح بعدد متغير من الأعمدة في تنسيق TSV، وتجاهل الأعمدة الإضافية، واستخدم القيم الافتراضية للأعمدة المفقودة.                                                                                                                                                         | `false`   |