---
description: 'مرجع ALTER سياسة إخفاء'
sidebar_label: 'سياسة إخفاء'
sidebar_position: 48
slug: /sql-reference/statements/alter/masking-policy
title: 'ALTER سياسة إخفاء'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="alter-masking-policy">
  # ALTER سياسة إخفاء
</div>

يُعدِّل سياسة إخفاء موجودة.

الصياغة:

```sql
ALTER MASKING POLICY [IF EXISTS] policy_name ON [database.]table
    [UPDATE column1 = expression1 [, column2 = expression2 ...]]
    [WHERE condition]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
    [PRIORITY priority_number]
```

جميع البنود اختيارية. لن تُحدَّث إلا البنود المحددة.