---
description: '`median*` 함수는 대응하는 `quantile*` 함수의 별칭입니다.
  이 함수들은 숫자 데이터 샘플의 중앙값을 계산합니다.'
slug: /sql-reference/aggregate-functions/reference/median
title: 'median'
doc_type: 'reference'
---

`median*` 함수는 대응하는 `quantile*` 함수의 별칭입니다. 이 함수들은 숫자 데이터 샘플의 중앙값을 계산합니다.

Functions:

* `median` — [quantile](/ko/sql-reference/aggregate-functions/reference/quantile)의 별칭입니다.
* `medianDeterministic` — [quantileDeterministic](/ko/sql-reference/aggregate-functions/reference/quantileDeterministic.md)의 별칭입니다.
* `medianExact` — [quantileExact](/ko/sql-reference/aggregate-functions/reference/quantileExact.md)의 별칭입니다.
* `medianExactWeighted` — [quantileExactWeighted](/ko/sql-reference/aggregate-functions/reference/quantileExactWeighted.md)의 별칭입니다.
* `medianTiming` — [quantileTiming](/ko/sql-reference/aggregate-functions/reference/quantileTiming.md)의 별칭입니다.
* `medianTimingWeighted` — [quantileTimingWeighted](/ko/sql-reference/aggregate-functions/reference/quantileTimingWeighted.md)의 별칭입니다.
* `medianTDigest` — [quantileTDigest](/ko/sql-reference/aggregate-functions/reference/quantileTDigest.md)의 별칭입니다.
* `medianTDigestWeighted` — [quantileTDigestWeighted](/ko/sql-reference/aggregate-functions/reference/quantileTDigestWeighted.md)의 별칭입니다.
* `medianBFloat16` — [quantileBFloat16](/ko/sql-reference/aggregate-functions/reference/quantileBFloat16.md)의 별칭입니다.
* `medianDD` — [quantileDD](/ko/sql-reference/aggregate-functions/reference/quantileDD.md)의 별칭입니다.

**예시**

입력 테이블:

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