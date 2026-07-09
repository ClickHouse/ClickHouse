---
description: 'Les fonctions `median*` sont des alias des fonctions `quantile*`
  correspondantes. Elles calculent la médiane d''un échantillon de données numériques.'
slug: /sql-reference/aggregate-functions/reference/median
title: 'median'
doc_type: 'reference'
---

Les fonctions `median*` sont des alias des fonctions `quantile*` correspondantes. Elles calculent la médiane d&#39;un échantillon de données numériques.

Fonctions :

* `median` — alias de [quantile](/fr/sql-reference/aggregate-functions/reference/quantile).
* `medianDeterministic` — alias de [quantileDeterministic](/fr/sql-reference/aggregate-functions/reference/quantileDeterministic.md).
* `medianExact` — alias de [quantileExact](/fr/sql-reference/aggregate-functions/reference/quantileExact.md).
* `medianExactWeighted` — alias de [quantileExactWeighted](/fr/sql-reference/aggregate-functions/reference/quantileExactWeighted.md).
* `medianTiming` — alias de [quantileTiming](/fr/sql-reference/aggregate-functions/reference/quantileTiming.md).
* `medianTimingWeighted` — alias de [quantileTimingWeighted](/fr/sql-reference/aggregate-functions/reference/quantileTimingWeighted.md).
* `medianTDigest` — alias de [quantileTDigest](/fr/sql-reference/aggregate-functions/reference/quantileTDigest.md).
* `medianTDigestWeighted` — alias de [quantileTDigestWeighted](/fr/sql-reference/aggregate-functions/reference/quantileTDigestWeighted.md).
* `medianBFloat16` — alias de [quantileBFloat16](/fr/sql-reference/aggregate-functions/reference/quantileBFloat16.md).
* `medianDD` — alias de [quantileDD](/fr/sql-reference/aggregate-functions/reference/quantileDD.md).

**Exemple**

Table d&#39;entrée :

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