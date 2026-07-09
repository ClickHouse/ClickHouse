---
description: 'توثيق المعامل `EXISTS`'
slug: /sql-reference/operators/exists
title: 'EXISTS'
doc_type: 'reference'
---

يفحص المعامل `EXISTS` عدد السجلات الموجودة في نتيجة استعلام فرعي. إذا كانت النتيجة فارغة، فسيُرجع المعامل `0`. وإلا فسيُرجع `1`.

يمكن أيضًا استخدام `EXISTS` في عبارة [WHERE](../../sql-reference/statements/select/where.md).

:::tip
لا يدعم الاستعلام الفرعي الإشارة إلى جداول الاستعلام الرئيسي وأعمدته.
:::

**الصياغة**

```sql
EXISTS(subquery)
```

**مثال**

استعلام للتحقق من وجود قيم في استعلام فرعي:

```sql title="Query"
SELECT EXISTS(SELECT * FROM numbers(10) WHERE number > 8), EXISTS(SELECT * FROM numbers(10) WHERE number > 11)
```

```text title="Response"
┌─in(1, _subquery1)─┬─in(1, _subquery2)─┐
│                 1 │                 0 │
└───────────────────┴───────────────────┘
```

استعلام يحتوي على استعلام فرعي يُرجع عدة صفوف:

```sql title="Query"
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 8);
```

```text title="Response"
┌─count()─┐
│      10 │
└─────────┘
```

استعلام يتضمن استعلامًا فرعيًا لا يُرجع أي نتيجة:

```sql title="Query"
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 11);
```

```text title="Response"
┌─count()─┐
│       0 │
└─────────┘
```