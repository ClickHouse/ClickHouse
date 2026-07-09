---
description: 'As funções `median*` são aliases das funções `quantile*`
  correspondentes. Elas calculam a mediana de uma amostra de dados numéricos.'
slug: /sql-reference/aggregate-functions/reference/median
title: 'median'
doc_type: 'reference'
---

As funções `median*` são aliases das funções `quantile*` correspondentes. Elas calculam a mediana de uma amostra de dados numéricos.

Funções:

* `median` — Alias de [quantile](/pt-BR/sql-reference/aggregate-functions/reference/quantile).
* `medianDeterministic` — Alias de [quantileDeterministic](/pt-BR/sql-reference/aggregate-functions/reference/quantileDeterministic.md).
* `medianExact` — Alias de [quantileExact](/pt-BR/sql-reference/aggregate-functions/reference/quantileExact.md).
* `medianExactWeighted` — Alias de [quantileExactWeighted](/pt-BR/sql-reference/aggregate-functions/reference/quantileExactWeighted.md).
* `medianTiming` — Alias de [quantileTiming](/pt-BR/sql-reference/aggregate-functions/reference/quantileTiming.md).
* `medianTimingWeighted` — Alias de [quantileTimingWeighted](/pt-BR/sql-reference/aggregate-functions/reference/quantileTimingWeighted.md).
* `medianTDigest` — Alias de [quantileTDigest](/pt-BR/sql-reference/aggregate-functions/reference/quantileTDigest.md).
* `medianTDigestWeighted` — Alias de [quantileTDigestWeighted](/pt-BR/sql-reference/aggregate-functions/reference/quantileTDigestWeighted.md).
* `medianBFloat16` — Alias de [quantileBFloat16](/pt-BR/sql-reference/aggregate-functions/reference/quantileBFloat16.md).
* `medianDD` — Alias de [quantileDD](/pt-BR/sql-reference/aggregate-functions/reference/quantileDD.md).

**Exemplo**

Tabela de entrada:

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