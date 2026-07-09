---
alias: []
description: 'توثيق تنسيق JSON'
input_format: true
keywords: ['JSON']
output_format: true
slug: /interfaces/formats/JSON
title: 'JSON'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✔       |                |

<div id="description">
  ## الوصف
</div>

يقرأ تنسيق `JSON` البيانات ويُخرجها بتنسيق JSON.

يعيد تنسيق `JSON` ما يلي:

| Parameter                    | Description                                                                                                                                                                                                                                                                                                                          |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `meta`                       | أسماء الأعمدة وأنواعها.                                                                                                                                                                                                                                                                                                              |
| `data`                       | جداول البيانات                                                                                                                                                                                                                                                                                                                       |
| `rows`                       | العدد الإجمالي للصفوف المُخرَجة.                                                                                                                                                                                                                                                                                                     |
| `rows_before_limit_at_least` | التقدير الأدنى لعدد الصفوف التي كانت ستظهر لولا LIMIT. لا يُخرج هذا إلا إذا كان الاستعلام يحتوي على LIMIT. ويُحتسب هذا التقدير من كتل البيانات التي جرت معالجتها في مسار تنفيذ الاستعلام قبل تطبيق LIMIT، لكن قد تستبعدها مرحلة LIMIT بعد ذلك. وإذا لم تصل الكتل أصلًا إلى مرحلة LIMIT في مسار تنفيذ الاستعلام، فلن تدخل في التقدير. |
| `statistics`                 | إحصاءات مثل `elapsed` و`rows_read` و`bytes_read`.                                                                                                                                                                                                                                                                                    |
| `totals`                     | القيم الإجمالية (عند استخدام WITH TOTALS).                                                                                                                                                                                                                                                                                           |
| `extremes`                   | القيم القصوى (عند ضبط extremes على 1).                                                                                                                                                                                                                                                                                               |

تنسيق `JSON` متوافق مع JavaScript. ولضمان ذلك، تُفلت بعض المحارف أيضًا بشكل إضافي:

* تُفلت الشرطة المائلة `/` على هيئة `\/`
* تُفلت فواصل الأسطر البديلة `U+2028` و`U+2029`، التي تتسبب في تعطيل بعض المتصفحات، على هيئة `\uXXXX`.
* تُفلت محارف تحكم ASCII: إذ يُستعاض عن backspace وform feed وline feed وcarriage return وhorizontal tab بـ `\b` و`\f` و`\n` و`\r` و`\t`، وكذلك البايتات المتبقية في النطاق 00-1F باستخدام متتاليات `\uXXXX`.
* تُحوَّل متتاليات UTF-8 غير الصالحة إلى محرف الاستبدال � بحيث يتكون النص المُخرج من متتاليات UTF-8 صالحة.

وللتوافق مع JavaScript، تُحاط الأعداد الصحيحة Int64 وUInt64 بعلامات اقتباس مزدوجة افتراضيًا.
ولإزالة علامات الاقتباس، يمكنك ضبط معلمة الإعداد [`output_format_json_quote_64bit_integers`](/ar/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) على `0`.

يدعم ClickHouse [NULL](/ar/sql-reference/syntax.md)، ويُعرض على هيئة `null` في مخرجات JSON. ولتمكين القيم `+nan` و`-nan` و`+inf` و`-inf` في المخرجات، اضبط [output&#95;format&#95;json&#95;quote&#95;denormals](/ar/operations/settings/settings-formats.md/#output_format_json_quote_denormals) على `1`.

<div id="example-usage">
  ## مثال للاستخدام
</div>

مثال:

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

بالنسبة إلى تنسيق الإدخال JSON، إذا كان الإعداد [`input_format_json_validate_types_from_metadata`](/ar/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) مضبوطًا على `1`،
فستُقارَن الأنواع الواردة في البيانات الوصفية ضمن بيانات الإدخال بأنواع الأعمدة المقابلة في الجدول.

<div id="see-also">
  ## انظر أيضًا
</div>

* تنسيق [JSONEachRow](/ar/interfaces/formats/JSONEachRow)
* إعداد [output&#95;format&#95;json&#95;array&#95;of&#95;rows](/ar/operations/settings/settings-formats.md/#output_format_json_array_of_rows)