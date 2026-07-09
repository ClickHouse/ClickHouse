---
description: 'Las funciones `median*` son alias de las correspondientes funciones `quantile*`.
  Calculan la mediana de una muestra de datos numéricos.'
slug: /sql-reference/aggregate-functions/reference/median
title: 'median'
doc_type: 'reference'
---

Las funciones `median*` son alias de las correspondientes funciones `quantile*`. Calculan la mediana de una muestra de datos numéricos.

Funciones:

* `median` — Alias de [quantile](/es/sql-reference/aggregate-functions/reference/quantile).
* `medianDeterministic` — Alias de [quantileDeterministic](/es/sql-reference/aggregate-functions/reference/quantileDeterministic.md).
* `medianExact` — Alias de [quantileExact](/es/sql-reference/aggregate-functions/reference/quantileExact.md).
* `medianExactWeighted` — Alias de [quantileExactWeighted](/es/sql-reference/aggregate-functions/reference/quantileExactWeighted.md).
* `medianTiming` — Alias de [quantileTiming](/es/sql-reference/aggregate-functions/reference/quantileTiming.md).
* `medianTimingWeighted` — Alias de [quantileTimingWeighted](/es/sql-reference/aggregate-functions/reference/quantileTimingWeighted.md).
* `medianTDigest` — Alias de [quantileTDigest](/es/sql-reference/aggregate-functions/reference/quantileTDigest.md).
* `medianTDigestWeighted` — Alias de [quantileTDigestWeighted](/es/sql-reference/aggregate-functions/reference/quantileTDigestWeighted.md).
* `medianBFloat16` — Alias de [quantileBFloat16](/es/sql-reference/aggregate-functions/reference/quantileBFloat16.md).
* `medianDD` — Alias de [quantileDD](/es/sql-reference/aggregate-functions/reference/quantileDD.md).

**Ejemplo**

Tabla de entrada:

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