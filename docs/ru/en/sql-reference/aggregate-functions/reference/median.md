---
description: 'Функции `median*` являются псевдонимами соответствующих функций `quantile*`.
  Они вычисляют медиану выборки числовых данных.'
slug: /sql-reference/aggregate-functions/reference/median
title: 'median'
doc_type: 'reference'
---

Функции `median*` являются псевдонимами соответствующих функций `quantile*`. Они вычисляют медиану выборки числовых данных.

Функции:

* `median` — Псевдоним для [quantile](/ru/sql-reference/aggregate-functions/reference/quantile).
* `medianDeterministic` — Псевдоним для [quantileDeterministic](/ru/sql-reference/aggregate-functions/reference/quantileDeterministic.md).
* `medianExact` — Псевдоним для [quantileExact](/ru/sql-reference/aggregate-functions/reference/quantileExact.md).
* `medianExactWeighted` — Псевдоним для [quantileExactWeighted](/ru/sql-reference/aggregate-functions/reference/quantileExactWeighted.md).
* `medianTiming` — Псевдоним для [quantileTiming](/ru/sql-reference/aggregate-functions/reference/quantileTiming.md).
* `medianTimingWeighted` — Псевдоним для [quantileTimingWeighted](/ru/sql-reference/aggregate-functions/reference/quantileTimingWeighted.md).
* `medianTDigest` — Псевдоним для [quantileTDigest](/ru/sql-reference/aggregate-functions/reference/quantileTDigest.md).
* `medianTDigestWeighted` — Псевдоним для [quantileTDigestWeighted](/ru/sql-reference/aggregate-functions/reference/quantileTDigestWeighted.md).
* `medianBFloat16` — Псевдоним для [quantileBFloat16](/ru/sql-reference/aggregate-functions/reference/quantileBFloat16.md).
* `medianDD` — Псевдоним для [quantileDD](/ru/sql-reference/aggregate-functions/reference/quantileDD.md).

**Пример**

Исходная таблица:

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