---
description: 'توثيق لعبارة PARALLEL WITH'
sidebar_label: 'PARALLEL WITH'
sidebar_position: 53
slug: /sql-reference/statements/parallel_with
title: 'عبارة PARALLEL WITH'
doc_type: 'reference'
---

يتيح تنفيذ عدة عبارات بالتوازي.

<div id="syntax">
  ## الصيغة
</div>

```sql
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
```

ينفّذ العبارات `statement1` و`statement2` و`statement3` ... بالتوازي مع بعضها بعضًا. ويُتجاهَل ناتج هذه العبارات.

قد يكون تنفيذ العبارات بالتوازي أسرع من تنفيذ تسلسل من العبارات نفسها في كثير من الحالات. على سبيل المثال، من المرجّح أن يكون `statement1 PARALLEL WITH statement2 PARALLEL WITH statement3` أسرع من `statement1; statement2; statement3`.

<div id="examples">
  ## أمثلة
</div>

ينشئ جدولين بالتوازي:

```sql
CREATE TABLE table1(x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2(y String) ENGINE = MergeTree ORDER BY tuple();
```

يحذف جدولين بالتوازي:

```sql
DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;
```

<div id="settings">
  ## الإعدادات
</div>

يحدّد الإعداد [max&#95;threads](../../operations/settings/settings.md#max_threads) عدد الخيوط التي يتم إنشاؤها.

<div id="comparison-with-union">
  ## مقارنة مع UNION
</div>

تُعدّ عبارة `PARALLEL WITH` مشابهةً إلى حدّ ما لـ [UNION](select/union.md)، إذ تنفّذ أيضًا معاملاتها بالتوازي. ومع ذلك، توجد بعض الاختلافات:

* لا تُرجع `PARALLEL WITH` أي نتائج من تنفيذ معاملاتها، ويمكنها فقط إعادة طرح استثناء صادر عنها إن وُجد؛
* لا تتطلّب `PARALLEL WITH` أن يكون لمعاملاتها نفس مجموعة أعمدة النتائج؛
* يمكن لـ `PARALLEL WITH` تنفيذ أي عبارة SQL (وليس `SELECT` فقط).