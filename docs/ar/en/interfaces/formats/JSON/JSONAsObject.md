---
alias: []
description: 'توثيق تنسيق JSONAsObject'
input_format: true
keywords: ['JSONAsObject']
output_format: false
slug: /interfaces/formats/JSONAsObject
title: 'JSONAsObject'
doc_type: 'مرجع'
---

<div id="description">
  ## الوصف
</div>

في هذا التنسيق، يُفسَّر كائن JSON واحد على أنه قيمة [JSON](/ar/sql-reference/data-types/newjson.md) واحدة. وإذا كان الإدخال يحتوي على عدة كائنات JSON (مفصولة بفواصل)، فتُفسَّر على أنها صفوف منفصلة. وإذا كانت بيانات الإدخال محاطة بـ `[]`، فتُفسَّر على أنها مصفوفة من قيم JSON.

لا يمكن تحليل هذا التنسيق إلا لجدول يحتوي على حقل واحد من النوع [JSON](/ar/sql-reference/data-types/newjson.md). ويجب ضبط الأعمدة المتبقية على [`DEFAULT`](/ar/sql-reference/statements/create/table.md/#default) أو [`MATERIALIZED`](/ar/sql-reference/statements/create/view#materialized-view).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="basic-example">
  ### مثال بسيط
</div>

```sql title="Query"
CREATE TABLE json_as_object (json JSON) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_object FORMAT JSONEachRow;
```

```response title="Response"
{"json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
{"json":{}}
{"json":{"any json stucture":"1"}}
```

<div id="an-array-of-json-objects">
  ### مصفوفة من كائنات JSON
</div>

```sql title="Query"
CREATE TABLE json_square_brackets (field JSON) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsObject [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];
SELECT * FROM json_square_brackets FORMAT JSONEachRow;
```

```response title="Response"
{"field":{"id":"1","name":"name1"}}
{"field":{"id":"2","name":"name2"}}
```

<div id="columns-with-default-values">
  ### الأعمدة ذات القيم الافتراضية
</div>

```sql title="Query"
CREATE TABLE json_as_object (json JSON, time DateTime MATERIALIZED now()) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"any json stucture":1}
SELECT time, json FROM json_as_object FORMAT JSONEachRow
```

```response title="Response"
{"time":"2024-09-16 12:18:10","json":{}}
{"time":"2024-09-16 12:18:13","json":{"any json stucture":"1"}}
{"time":"2024-09-16 12:18:08","json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>
