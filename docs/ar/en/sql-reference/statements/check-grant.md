---
description: 'توثيق CHECK GRANT'
sidebar_label: 'CHECK GRANT'
sidebar_position: 56
slug: /sql-reference/statements/check-grant
title: 'عبارة CHECK GRANT'
doc_type: 'reference'
---

يُستخدم الاستعلام `CHECK GRANT` للتحقّق مما إذا كان قد تم منح المستخدم/الدور الحالي امتيازًا معيّنًا.

<div id="syntax">
  ## البنية
</div>

البنية الأساسية للاستعلام كما يلي:

```sql
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*}
```

* `privilege` — نوع الامتياز.

<div id="examples">
  ## أمثلة
</div>

إذا كان المستخدم قد مُنح هذا الامتياز من قبل، فستكون قيمة الاستجابة `check_grant` هي `1`. وإلا فستكون قيمة الاستجابة `check_grant` هي `0`.

إذا كان `table_1.col1` موجودًا وكان المستخدم الحالي قد مُنح امتياز `SELECT`/`SELECT(con)` أو دورًا (يتضمن هذا الامتياز)، فستكون الاستجابة `1`.

```sql
CHECK GRANT SELECT(col1) ON table_1;
```

```text
┌─result─┐
│      1 │
└────────┘
```

إذا لم يكن `table_2.col2` موجودًا، أو لم يُمنَح المستخدم الحالي امتياز `SELECT`/`SELECT(con)` أو دورًا (يتضمن هذا الامتياز)، فستكون الاستجابة `0`.

```sql
CHECK GRANT SELECT(col2) ON table_2;
```

```text
┌─result─┐
│      0 │
└────────┘
```

<div id="wildcard">
  ## أحرف البدل
</div>

عند تحديد الامتيازات، يمكنك استخدام النجمة (`*`) بدلًا من اسم جدول أو قاعدة بيانات. يُرجى الرجوع إلى [منح أحرف البدل](../../sql-reference/statements/grant.md#wildcard-grants) للاطلاع على قواعد أحرف البدل.