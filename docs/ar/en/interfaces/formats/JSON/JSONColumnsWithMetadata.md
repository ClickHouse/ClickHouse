---
alias: []
description: 'توثيق تنسيق JSONColumnsWithMetadata'
input_format: true
keywords: ['JSONColumnsWithMetadata']
output_format: true
slug: /interfaces/formats/JSONColumnsWithMetadata
title: 'JSONColumnsWithMetadata'
doc_type: 'مرجع'
---

| إدخال | إخراج | اسم بديل |
| ----- | ----- | -------- |
| ✔     | ✔     |          |

<div id="description">
  ## الوصف
</div>

يختلف عن تنسيق [`JSONColumns`](./JSONColumns.md) في أنه يتضمن أيضًا بعض البيانات الوصفية والإحصاءات (على غرار تنسيق [`JSON`](./JSON.md)).

:::note
يُخزّن تنسيق `JSONColumnsWithMetadata` جميع البيانات مؤقتًا في الذاكرة ثم يُخرجها ككتلة واحدة، لذا قد يؤدي ذلك إلى ارتفاع استهلاك الذاكرة.
:::

<div id="example-usage">
  ## مثال للاستخدام
</div>

مثال:

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
        {
                "num": [42, 43, 44],
                "str": ["hello", "hello", "hello"],
                "arr": [[0,1], [0,1,2], [0,1,2,3]]
        },

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.000272376,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

بالنسبة إلى تنسيق الإدخال `JSONColumnsWithMetadata`، إذا كان الإعداد [`input_format_json_validate_types_from_metadata`](/ar/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) مضبوطًا على `1`،
فستُقارَن الأنواع الواردة في البيانات الوصفية ضمن بيانات الإدخال بأنواع الأعمدة المقابلة في الجدول.

<div id="format-settings">
  ## إعدادات التنسيق
</div>
