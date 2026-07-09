---
alias: []
description: 'وثائق تنسيق TemplateIgnoreSpaces'
input_format: true
keywords: ['TemplateIgnoreSpaces']
output_format: false
slug: /interfaces/formats/TemplateIgnoreSpaces
title: 'TemplateIgnoreSpaces'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✗       |                |

<div id="description">
  ## الوصف
</div>

يشبه [`Template`]، لكنه يتجاهل محارف المسافات البيضاء بين الفواصل والقيم في دفق الإدخال.
ومع ذلك، إذا كانت سلاسل التنسيق تحتوي على محارف مسافات بيضاء، فسيُتوقع وجود هذه المحارف في دفق الإدخال.
كما يتيح تحديد عناصر نائبة فارغة (`${}` أو `${:None}`) لتقسيم بعض الفواصل إلى أجزاء منفصلة بغرض تجاهل المسافات بينها.
ولا تُستخدم هذه العناصر النائبة إلا لتخطي محارف المسافات البيضاء.
يمكن قراءة `JSON` باستخدام هذا التنسيق إذا كانت قيم الأعمدة بالترتيب نفسه في جميع الصفوف.

:::note
هذا التنسيق مناسب للإدخال فقط.
:::

<div id="example-usage">
  ## مثال على الاستخدام
</div>

يمكن استخدام الطلب التالي لإدراج البيانات من مثال المخرجات بتنسيق [JSON](/ar/interfaces/formats/JSON):

```sql
INSERT INTO table_name 
SETTINGS
    format_template_resultset = '/some/path/resultset.format',
    format_template_row = '/some/path/row.format',
    format_template_rows_between_delimiter = ','
FORMAT TemplateIgnoreSpaces
```

```text title="/some/path/resultset.format"
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

```text title="/some/path/row.format"
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
