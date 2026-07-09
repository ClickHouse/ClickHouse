---
description: 'ALTER MASKING POLICY 문서'
sidebar_label: '마스킹 정책'
sidebar_position: 48
slug: /sql-reference/statements/alter/masking-policy
title: 'ALTER MASKING POLICY'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="alter-masking-policy">
  # ALTER MASKING POLICY
</div>

기존 마스킹 정책를 수정합니다.

구문:

```sql
ALTER MASKING POLICY [IF EXISTS] policy_name ON [database.]table
    [UPDATE column1 = expression1 [, column2 = expression2 ...]]
    [WHERE condition]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
    [PRIORITY priority_number]
```

모든 절은 선택 사항입니다. 지정한 절만 업데이트됩니다.