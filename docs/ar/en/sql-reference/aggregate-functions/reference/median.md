---
description: 'الدوال `median*` هي أسماء مستعارة للدوال `quantile*` المناظرة.
  وتحسب الوسيط لعينة من بيانات رقمية.'
slug: /sql-reference/aggregate-functions/reference/median
title: 'median'
doc_type: 'مرجع'
---

الدوال `median*` هي أسماء مستعارة للدوال `quantile*` المناظرة. وتحسب الوسيط لعينة من بيانات رقمية.

الدوال:

* `median` — اسم مستعار لـ [quantile](/ar/sql-reference/aggregate-functions/reference/quantile).
* `medianDeterministic` — اسم مستعار لـ [quantileDeterministic](/ar/sql-reference/aggregate-functions/reference/quantileDeterministic.md).
* `medianExact` — اسم مستعار لـ [quantileExact](/ar/sql-reference/aggregate-functions/reference/quantileExact.md).
* `medianExactWeighted` — اسم مستعار لـ [quantileExactWeighted](/ar/sql-reference/aggregate-functions/reference/quantileExactWeighted.md).
* `medianTiming` — اسم مستعار لـ [quantileTiming](/ar/sql-reference/aggregate-functions/reference/quantileTiming.md).
* `medianTimingWeighted` — اسم مستعار لـ [quantileTimingWeighted](/ar/sql-reference/aggregate-functions/reference/quantileTimingWeighted.md).
* `medianTDigest` — اسم مستعار لـ [quantileTDigest](/ar/sql-reference/aggregate-functions/reference/quantileTDigest.md).
* `medianTDigestWeighted` — اسم مستعار لـ [quantileTDigestWeighted](/ar/sql-reference/aggregate-functions/reference/quantileTDigestWeighted.md).
* `medianBFloat16` — اسم مستعار لـ [quantileBFloat16](/ar/sql-reference/aggregate-functions/reference/quantileBFloat16.md).
* `medianDD` — اسم مستعار لـ [quantileDD](/ar/sql-reference/aggregate-functions/reference/quantileDD.md).

**مثال**

جدول الإدخال:

```text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

```sql title="Query"
SELECT medianDeterministic(val, 1) FROM t;
```

```text title="Response"
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```