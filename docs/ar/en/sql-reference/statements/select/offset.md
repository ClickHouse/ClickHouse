---
description: 'توثيق OFFSET'
sidebar_label: 'OFFSET'
slug: /sql-reference/statements/select/offset
title: 'عبارة OFFSET وFETCH'
doc_type: 'reference'
---

يتيح لك `OFFSET` و`FETCH` جلب البيانات على دفعات. وهما يحددان كتلة صفوف تريد استرجاعها باستعلام واحد.

```sql
-- SQL Standard style:
[OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]

-- MySQL/PostgreSQL style:
[LIMIT [n, ]m] [OFFSET offset_row_count]
```

يمكن أن تكون قيمة `offset_row_count` أو `fetch_row_count` رقمًا أو قيمة حرفية. ويمكنك حذف `fetch_row_count`؛ وتكون قيمتها الافتراضية 1.

يحدد `OFFSET` عدد الصفوف التي يجب تخطيها قبل البدء في إرجاع الصفوف من مجموعة نتائج الاستعلام. ويؤدي `OFFSET n` إلى تخطي أول `n` صفوف من النتيجة.

يُدعَم `OFFSET` السالب: إذ يؤدي `OFFSET -n` إلى تخطي آخر `n` صفوف من النتيجة.

كما يُدعَم `OFFSET` الكسري: `OFFSET n` — إذا كان 0 &lt; n &lt; 1، فسيتم تخطي أول n * 100% من النتيجة.

مثال:
• `OFFSET 0.1` - يتخطى أول 10% من النتيجة.

> **ملاحظة**
> • يجب أن يكون الكسر عددًا من النوع [Float64](../../data-types/float.md) أقل من 1 وأكبر من الصفر.
> • إذا أسفر الحساب عن عدد كسري من الصفوف، فسيُقرَّب إلى العدد الصحيح التالي.

يحدد `FETCH` الحد الأقصى لعدد الصفوف التي يمكن أن تتضمنها نتيجة الاستعلام.

يُستخدم الخيار `ONLY` لإرجاع الصفوف التي تأتي مباشرة بعد الصفوف التي تخطاها `OFFSET`. وفي هذه الحالة، يكون `FETCH` بديلًا عن عبارة [LIMIT](../../../sql-reference/statements/select/limit.md). على سبيل المثال، الاستعلام التالي

```sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

مطابق تمامًا للاستعلام

```sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

يُستخدم الخيار `WITH TIES` لإرجاع أي صفوف إضافية تتساوى في المرتبة الأخيرة ضمن مجموعة النتائج وفقًا لعبارة `ORDER BY`. على سبيل المثال، إذا تم تعيين `fetch_row_count` إلى 5، ولكن كان هناك صفّان إضافيان تتطابق فيهما قيم أعمدة `ORDER BY` مع قيم الصف الخامس، فستتضمن مجموعة النتائج سبعة صفوف.

:::note
وفقًا للمعيار، يجب أن تأتي عبارة `OFFSET` قبل عبارة `FETCH` إذا وُجدتا كلتاهما.
:::

:::note
قد تعتمد الإزاحة الفعلية أيضًا على إعداد [offset](../../../operations/settings/settings.md#offset).
:::

<div id="examples">
  ## أمثلة
</div>

جدول الإدخال:

```text
┌─a─┬─b─┐
│ 1 │ 1 │
│ 2 │ 1 │
│ 3 │ 4 │
│ 1 │ 3 │
│ 5 │ 4 │
│ 0 │ 6 │
│ 5 │ 7 │
└───┴───┘
```

استخدام الخيار `ONLY`:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

استخدام الخيار `WITH TIES`:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```