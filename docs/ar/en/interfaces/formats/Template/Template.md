---
alias: []
description: 'توثيق تنسيق Template'
input_format: true
keywords: ['Template']
output_format: true
slug: /interfaces/formats/Template
title: 'Template'
doc_type: 'guide'
---

| المدخل | المخرج | الاسم المستعار |
| ------ | ------ | -------------- |
| ✔      | ✔      |                |

<div id="description">
  ## الوصف
</div>

في الحالات التي تحتاج فيها إلى قدر أكبر من التخصيص مما تتيحه التنسيقات القياسية الأخرى،
يتيح تنسيق `Template` للمستخدم تحديد سلسلة تنسيق مخصصة خاصة به مع عناصر نائبة للقيم،
بالإضافة إلى تحديد قواعد الإفلات للبيانات.

ويستخدم الإعدادات التالية:

| Setting                                                                                              | Description                                                                                           |
| ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| [`format_template_row`](#format_template_row)                                                        | يحدد المسار إلى الملف الذي يحتوي على سلاسل التنسيق الخاصة بالصفوف.                                    |
| [`format_template_resultset`](#format_template_resultset)                                            | يحدد المسار إلى الملف الذي يحتوي على سلاسل التنسيق الخاصة بالصفوف                                     |
| [`format_template_rows_between_delimiter`](#format_template_rows_between_delimiter)                  | يحدد الفاصل بين الصفوف، والذي يُطبع (أو يكون متوقعًا) بعد كل صف باستثناء الصف الأخير (`\n` افتراضيًا) |
| `format_template_row_format`                                                                         | يحدد سلسلة التنسيق للصفوف [ضمنيًا](#inline_specification).                                            |
| `format_template_resultset_format`                                                                   | يحدد سلسلة تنسيق مجموعة النتائج [ضمنيًا](#inline_specification).                                      |
| بعض إعدادات التنسيقات الأخرى (مثل `output_format_json_quote_64bit_integers` عند استخدام إفلات `JSON` |                                                                                                       |

<div id="settings-and-escaping-rules">
  ## الإعدادات وقواعد الإفلات
</div>

<div id="format_template_row">
  ### format_template_row
</div>

يحدّد الإعداد `format_template_row` المسار إلى الملف الذي يحتوي على سلاسل تنسيق الصفوف وفق الصياغة التالية:

```text
delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N
```

حيث:

| جزء من الصياغة  | الوصف                                                                                   |
| --------------- | --------------------------------------------------------------------------------------- |
| `delimiter_i`   | فاصل بين القيم (يمكن إفلات الرمز `$` على شكل `$$`)                                      |
| `column_i`      | اسم العمود أو فهرسه الذي يُراد تحديد قيمه أو إدراجها (إذا كان فارغًا، فسيُتخطّى العمود) |
| `serializeAs_i` | قاعدة إفلات لقيم العمود.                                                                |

قواعد الإفلات التالية مدعومة:

| قاعدة الإفلات        | الوصف                                    |
| -------------------- | ---------------------------------------- |
| `CSV`, `JSON`, `XML` | مماثلة للتنسيقات التي تحمل الأسماء نفسها |
| `Escaped`            | مماثلة لـ `TSV`                          |
| `Quoted`             | مماثلة لـ `Values`                       |
| `Raw`                | بدون إفلات، مماثلة لـ `TSVRaw`           |
| `None`               | بدون قاعدة إفلات — انظر الملاحظة أدناه   |

:::note
إذا لم تُحدَّد قاعدة إفلات، فسيُستخدم `None`. لا يكون `XML` مناسبًا إلا للإخراج.
:::

لنلقِ نظرة على مثال. بالنظر إلى سلسلة التنسيق التالية:

```text
Search phrase: ${s:Quoted}, count: ${c:Escaped}, ad price: $$${p:JSON};
```

ستُطبع القيم التالية (عند استخدام `SELECT`) أو ستكون متوقعة (عند استخدام `INPUT`)،
بين فواصل الأعمدة `Search phrase:`, `, count:`, `, ad price: $` والفاصل `;` على الترتيب:

* `s` (مع قاعدة الإفلات `Quoted`)
* `c` (مع قاعدة الإفلات `Escaped`)
* `p` (مع قاعدة الإفلات `JSON`)

على سبيل المثال:

* عند تنفيذ `INSERT`، يطابق السطر أدناه القالب المتوقع، وتُقرأ القيم `bathroom interior design`, `2166`, `$3` في الأعمدة `Search phrase`, `count`, `ad price`.
* عند تنفيذ `SELECT`، يكون السطر أدناه هو الناتج، بافتراض أن القيم `bathroom interior design`, `2166`, `$3` مخزَّنة بالفعل في جدول ضمن الأعمدة `Search phrase`, `count`, `ad price`.

```yaml
Search phrase: 'bathroom interior design', count: 2166, ad price: $3;
```

<div id="format_template_rows_between_delimiter">
  ### format_template_rows_between_delimiter
</div>

يحدّد الإعداد `format_template_rows_between_delimiter` الفاصل بين الصفوف، ويُطبع (أو يُتوقَّع) بعد كل صف باستثناء الصف الأخير (`\n` افتراضيًا)

<div id="format_template_resultset">
  ### format_template_resultset
</div>

يحدّد الإعداد `format_template_resultset` مسار الملف الذي يحتوي على سلسلة تنسيق لمجموعة النتائج.

لسلسلة تنسيق مجموعة النتائج الصياغة نفسها مثل سلسلة تنسيق الصفوف.
كما تتيح تحديد بادئة ولاحقة وطريقة لعرض بعض المعلومات الإضافية، وتحتوي على العناصر النائبة التالية بدلًا من أسماء الأعمدة:

* `data` هي الصفوف التي تتضمن البيانات بتنسيق `format_template_row`، وتفصل بينها القيمة `format_template_rows_between_delimiter`. يجب أن يكون هذا العنصر النائب أول عنصر نائب في سلسلة التنسيق.
* `totals` هو الصف الذي يحتوي على القيم الإجمالية بتنسيق `format_template_row` (عند استخدام WITH TOTALS).
* `min` هو الصف الذي يحتوي على القيم الدنيا بتنسيق `format_template_row` (عندما تُضبط extremes على 1).
* `max` هو الصف الذي يحتوي على القيم القصوى بتنسيق `format_template_row` (عندما تُضبط extremes على 1).
* `rows` هو العدد الإجمالي لصفوف المخرجات.
* `rows_before_limit` هو أدنى عدد من الصفوف كان سيظهر لولا LIMIT. ولا يظهر إلا إذا كان الاستعلام يحتوي على LIMIT. وإذا كان الاستعلام يحتوي على GROUP BY، فإن rows&#95;before&#95;limit&#95;at&#95;least يكون هو العدد الدقيق للصفوف التي كانت ستظهر بدون LIMIT.
* `time` هو وقت تنفيذ الطلب بالثواني.
* `rows_read` هو عدد الصفوف التي تمت قراءتها.
* `bytes_read` هو عدد البايتات (غير المضغوطة) التي تمت قراءتها.

يجب ألا تكون للعناصر النائبة `data` و`totals` و`min` و`max` قاعدة إفلات محددة (أو يجب تحديد `None` صراحةً). أما العناصر النائبة المتبقية، فيمكن تحديد أي قاعدة إفلات لها.

:::note
إذا كانت قيمة الإعداد `format_template_resultset` سلسلة فارغة، فستُستخدم `${data}` بوصفها القيمة الافتراضية.
:::

في استعلامات INSERT، يتيح التنسيق تخطي بعض الأعمدة أو الحقول عند وجود بادئة أو لاحقة (انظر المثال).

<div id="inline_specification">
  ### المواصفة المضمّنة
</div>

غالبًا ما يكون من الصعب، أو غير الممكن، نشر تهيئات التنسيق
(المحددة بواسطة `format_template_row` و`format_template_resultset`) الخاصة بتنسيق Template إلى دليل على جميع العقد في العنقود.
علاوة على ذلك، قد يكون التنسيق بسيطًا جدًا لدرجة أنه لا يحتاج إلى وضعه في ملف.

في هذه الحالات، يمكن استخدام `format_template_row_format` (لـ `format_template_row`) و`format_template_resultset_format` (لـ `format_template_resultset`) لتعيين سلسلة القالب مباشرةً داخل الاستعلام،
بدلًا من تحديدها كمسار إلى الملف الذي يحتوي عليها.

:::note
قواعد سلاسل التنسيق وتسلسلات الهروب هي نفسها المطبقة على:

* [`format_template_row`](#format_template_row) عند استخدام `format_template_row_format`.
* [`format_template_resultset`](#format_template_resultset) عند استخدام `format_template_resultset_format`.
  :::

<div id="example-usage">
  ## مثال على الاستخدام
</div>

لنلقِ نظرة على مثالين لكيفية استخدام تنسيق `Template`: أولًا لاستعلام البيانات، ثم لإدراجها.

<div id="selecting-data">
  ### استعلام البيانات
</div>

```sql title="Query"
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

```text title="/some/path/resultset.format"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

```text title="/some/path/row.format"
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

```html title="Response"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>clickhouse</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

<div id="inserting-data">
  ### إدراج البيانات
</div>

```text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

```sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

```text title="/some/path/resultset.format"
Some header\n${data}\nTotal rows: ${:CSV}\n
```

```text title="/some/path/row.format"
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews` و`UserID` و`Duration` و`Sign` داخل العناصر النائبة هي أسماء أعمدة في الجدول. تُتجاهل القيم التي تأتي بعد `Useless field` في الصفوف وبعد `\nTotal rows:` في اللاحقة.
يجب أن تتطابق جميع الفواصل في بيانات الإدخال تمامًا مع الفواصل في سلاسل التنسيق المحددة.

<div id="in-line-specification">
  ### المواصفة المضمنة
</div>

هل سئمت من تنسيق جداول Markdown يدويًا؟ في هذا المثال، سنستعرض كيف يمكن استخدام تنسيق `Template` وإعدادات المواصفة المضمنة لإنجاز مهمة بسيطة: تنفيذ `SELECT` لأسماء بعض تنسيقات ClickHouse من الجدول `system.formats` وتنسيقها في صورة جدول Markdown. ويمكن تحقيق ذلك بسهولة باستخدام تنسيق `Template` والإعدادين `format_template_row_format` و`format_template_resultset_format`.

في أمثلة سابقة، حددنا سلاسل تنسيق مجموعة النتائج والصفوف في ملفات منفصلة، مع تحديد المسارات إلى تلك الملفات باستخدام الإعدادين `format_template_resultset` و`format_template_row` على التوالي. هنا سنفعل ذلك بشكل مضمن لأن القالب بسيط جدًا، إذ لا يتكون إلا من بضعة رموز `|` و`-` لتكوين جدول Markdown. سنحدد سلسلة قالب مجموعة النتائج باستخدام الإعداد `format_template_resultset_format`. ولإنشاء ترويسة الجدول، أضفنا `|ClickHouse Formats|\n|---|\n` قبل `${data}`. ونستخدم الإعداد `format_template_row_format` لتحديد سلسلة القالب ``|`{0:XML}`|`` لصفوفنا. سيُدرج تنسيق `Template` صفوفنا بالتنسيق المحدد في العنصر النائب `${data}`. في هذا المثال لدينا عمود واحد فقط، ولكن إذا أردت إضافة المزيد، فيمكنك فعل ذلك بإضافة `{1:XML}` و`{2:XML}`... إلخ إلى سلسلة قالب الصف، مع اختيار قاعدة الإفلات المناسبة. في هذا المثال اخترنا قاعدة الإفلات `XML`.

```sql title="Query"
WITH formats AS
(
 SELECT * FROM system.formats
 ORDER BY rand()
 LIMIT 5
)
SELECT * FROM formats
FORMAT Template
SETTINGS
 format_template_row_format='|`${0:XML}`|',
 format_template_resultset_format='|ClickHouse Formats|\n|---|\n${data}\n'
```

انظر إلى ذلك! لقد جنّبنا أنفسنا عناء إضافة كل تلك الرموز `|` و`-` يدويًا لإنشاء جدول Markdown هذا:

```response title="Response"
|ClickHouse Formats|
|---|
|`BSONEachRow`|
|`CustomSeparatedWithNames`|
|`Prometheus`|
|`DWARF`|
|`Avro`|
```