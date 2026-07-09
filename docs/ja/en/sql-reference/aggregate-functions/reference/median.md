---
description: '`median*` 関数は、対応する `quantile*`
  関数の別名です。数値データのサンプルの中央値を計算します。'
slug: /sql-reference/aggregate-functions/reference/median
title: 'median'
doc_type: 'reference'
---

`median*` 関数は、対応する `quantile*` 関数の別名です。数値データのサンプルの中央値を計算します。

関数:

* `median` — [quantile](/ja/sql-reference/aggregate-functions/reference/quantile) の別名。
* `medianDeterministic` — [quantileDeterministic](/ja/sql-reference/aggregate-functions/reference/quantileDeterministic.md) の別名。
* `medianExact` — [quantileExact](/ja/sql-reference/aggregate-functions/reference/quantileExact.md) の別名。
* `medianExactWeighted` — [quantileExactWeighted](/ja/sql-reference/aggregate-functions/reference/quantileExactWeighted.md) の別名。
* `medianTiming` — [quantileTiming](/ja/sql-reference/aggregate-functions/reference/quantileTiming.md) の別名。
* `medianTimingWeighted` — [quantileTimingWeighted](/ja/sql-reference/aggregate-functions/reference/quantileTimingWeighted.md) の別名。
* `medianTDigest` — [quantileTDigest](/ja/sql-reference/aggregate-functions/reference/quantileTDigest.md) の別名。
* `medianTDigestWeighted` — [quantileTDigestWeighted](/ja/sql-reference/aggregate-functions/reference/quantileTDigestWeighted.md) の別名。
* `medianBFloat16` — [quantileBFloat16](/ja/sql-reference/aggregate-functions/reference/quantileBFloat16.md) の別名。
* `medianDD` — [quantileDD](/ja/sql-reference/aggregate-functions/reference/quantileDD.md) の別名。

**例**

入力テーブル:

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