---
description: 'ALTER MASKING POLICY 参考文档'
sidebar_label: '数据脱敏策略'
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

修改现有的数据脱敏策略。

语法：

```sql
ALTER MASKING POLICY [IF EXISTS] policy_name ON [database.]table
    [UPDATE column1 = expression1 [, column2 = expression2 ...]]
    [WHERE condition]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
    [PRIORITY priority_number]
```

所有子句均为可选。只会更新已指定的子句。