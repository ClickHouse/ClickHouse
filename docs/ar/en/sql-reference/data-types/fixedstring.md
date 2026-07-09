---
description: 'وثائق نوع البيانات FixedString في ClickHouse'
sidebar_label: 'FixedString(N)'
sidebar_position: 10
slug: /sql-reference/data-types/fixedstring
title: 'FixedString(N)'
doc_type: 'reference'
---

سلسلة نصية ثابتة الطول تتكوّن من `N` بايت (وليست أحرفًا ولا نقاط ترميز).

للإعلان عن عمود من النوع `FixedString`، استخدم الصياغة التالية:

```sql
<column_name> FixedString(N)
```

حيث إن `N` عدد طبيعي.

يكون النوع `FixedString` فعّالًا عندما يكون طول البيانات `N` بايتًا بالضبط. وفي جميع الحالات الأخرى، يُحتمل أن يؤدي إلى انخفاض الكفاءة.

أمثلة على القيم التي يمكن تخزينها بكفاءة في الأعمدة من النوع `FixedString`:

* التمثيل الثنائي لعناوين IP (`FixedString(16)` لـ IPv6).
* رموز اللغات (ru&#95;RU, en&#95;US ... ).
* رموز العملات (USD, RUB ... ).
* التمثيل الثنائي لقيم التجزئة (`FixedString(16)` لـ MD5، و`FixedString(32)` لـ SHA256).

لتخزين قيم UUID، استخدم نوع البيانات [UUID](../../sql-reference/data-types/uuid.md).

عند إدراج البيانات، فإن ClickHouse:

* يُكمّل السلسلة النصية ببايتات فارغة إذا كانت تحتوي على أقل من `N` بايت.
* يُطلق الاستثناء `Too large value for FixedString(N)` إذا كانت السلسلة النصية تحتوي على أكثر من `N` بايت.

لننظر إلى الجدول التالي الذي يحتوي على عمود واحد من النوع `FixedString(2)`:

```sql


INSERT INTO FixedStringTable VALUES ('a'), ('ab'), ('');
```

```sql
SELECT
    name,
    toTypeName(name),
    length(name),
    empty(name)
FROM FixedStringTable;
```

```text
┌─name─┬─toTypeName(name)─┬─length(name)─┬─empty(name)─┐
│ a    │ FixedString(2)   │            2 │           0 │
│ ab   │ FixedString(2)   │            2 │           0 │
│      │ FixedString(2)   │            2 │           1 │
└──────┴──────────────────┴──────────────┴─────────────┘
```

لاحظ أن طول قيمة `FixedString(N)` ثابت. تُرجِع الدالة [length](/ar/sql-reference/functions/array-functions#length) القيمة `N` حتى لو كانت قيمة `FixedString(N)` مملوءة فقط ببايتات صفرية، لكن الدالة [empty](/ar/sql-reference/functions/array-functions#empty) تُرجِع `1` في هذه الحالة.

يؤدي تحديد البيانات باستخدام عبارة `WHERE` إلى نتائج مختلفة بحسب كيفية تحديد الشرط:

* إذا استُخدم عامل المساواة `=` أو `==` أو الدالة `equals`، فإن ClickHouse *لا* يأخذ المحرف `\0` في الاعتبار؛ أي إن الاستعلامين `SELECT * FROM FixedStringTable WHERE name = 'a';` و `SELECT * FROM FixedStringTable WHERE name = 'a\0';` يُرجعان النتيجة نفسها.
* إذا استُخدمت عبارة `LIKE`، فإن ClickHouse *يأخذ* المحرف `\0` في الاعتبار، لذلك قد يلزم تحديد المحرف `\0` صراحةً في شرط التصفية.

```sql
SELECT name
FROM FixedStringTable
WHERE name = 'a'
FORMAT JSONStringsEachRow

{"name":"a\u0000"}


SELECT name
FROM FixedStringTable
WHERE name = 'a\0'
FORMAT JSONStringsEachRow

{"name":"a\u0000"}


SELECT name
FROM FixedStringTable
WHERE name = 'a'
FORMAT JSONStringsEachRow

Query id: c32cec28-bb9e-4650-86ce-d74a1694d79e

{"name":"a\u0000"}


SELECT name
FROM FixedStringTable
WHERE name LIKE 'a'
FORMAT JSONStringsEachRow

0 rows in set.


SELECT name
FROM FixedStringTable
WHERE name LIKE 'a\0'
FORMAT JSONStringsEachRow

{"name":"a\u0000"}
```