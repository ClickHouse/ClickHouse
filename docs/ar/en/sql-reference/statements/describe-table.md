---
description: 'مرجع DESCRIBE TABLE'
sidebar_label: 'DESCRIBE TABLE'
sidebar_position: 42
slug: /sql-reference/statements/describe-table
title: 'DESCRIBE TABLE'
doc_type: 'reference'
---

يعرض معلومات عن أعمدة الجدول.

**الصيغة**

```sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

تعيد عبارة `DESCRIBE` صفًا لكل عمود في الجدول، بالقيم التالية من نوع [String](../../sql-reference/data-types/string.md):

* `name` — اسم العمود.
* `type` — نوع العمود.
* `default_type` — عبارة تُستخدَم في [التعبير الافتراضي](/ar/sql-reference/statements/create/table) للعمود: `DEFAULT` أو `MATERIALIZED` أو `ALIAS`. وإذا لم يكن هناك تعبير افتراضي، فستُعاد سلسلة فارغة.
* `default_expression` — تعبير مُحدَّد بعد العبارة `DEFAULT`.
* `comment` — [تعليق العمود](/ar/sql-reference/statements/alter/column#comment-column).
* `codec_expression` — تعبير [codec](/ar/sql-reference/statements/create/table#column_compression_codec) مُطبَّق على العمود.
* `ttl_expression` — تعبير [TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
* `is_subcolumn` — راية تكون قيمتها `1` للأعمدة الفرعية الداخلية. ولا تُضمَّن في النتيجة إلا إذا كان وصف الأعمدة الفرعية مفعّلًا عبر الإعداد [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

تُوصَف جميع الأعمدة في هياكل البيانات [Nested](../../sql-reference/data-types/nested-data-structures/index.md) كلٌّ على حدة. ويُسبَق اسم كل عمود باسم العمود الأصل متبوعًا بنقطة.

لعرض الأعمدة الفرعية الداخلية لأنواع البيانات الأخرى، استخدم الإعداد [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

**مثال**

```sql title="Query"
CREATE TABLE describe_example (
    id UInt64, text String DEFAULT 'unknown' CODEC(ZSTD),
    user Tuple (name String, age UInt8)
) ENGINE = MergeTree() ORDER BY id;

DESCRIBE TABLE describe_example;
DESCRIBE TABLE describe_example SETTINGS describe_include_subcolumns=1;
```

```text title="Response"
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id   │ UInt64                        │              │                    │         │                  │                │
│ text │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │
│ user │ Tuple(name String, age UInt8) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

يعرض الاستعلام الثاني أيضًا الأعمدة الفرعية:

```text title="Response"
┌─name──────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┬─is_subcolumn─┐
│ id        │ UInt64                        │              │                    │         │                  │                │            0 │
│ text      │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │            0 │
│ user      │ Tuple(name String, age UInt8) │              │                    │         │                  │                │            0 │
│ user.name │ String                        │              │                    │         │                  │                │            1 │
│ user.age  │ UInt8                         │              │                    │         │                  │                │            1 │
└───────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┴──────────────┘
```

يمكن أيضًا استخدام عبارة DESCRIBE مع الاستعلامات الفرعية أو التعبيرات القياسية:

```SQL
DESCRIBE SELECT 1 FORMAT TSV;
```

أو

```SQL
DESCRIBE (SELECT 1) FORMAT TSV;
```

```text title="Response"
1       UInt8
```

يعرض هذا الاستخدام بيانات وصفية عن أعمدة النتائج الخاصة بالاستعلام أو الاستعلام الفرعي المحدد. ويفيد ذلك في فهم بنية الاستعلامات المعقدة قبل تنفيذها.

**انظر أيضًا**

* إعداد [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).