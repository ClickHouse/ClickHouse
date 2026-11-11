---
description: 'System table containing information about all masking policies in the system.'
keywords: ['system table', 'masking_policies']
slug: /operations/system-tables/masking_policies
title: 'system.masking_policies'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge/>

# system.masking_policies

Contains information about all masking policies defined in the system.

Columns:

- `name` ([String](/sql-reference/data-types/string.md)) — Name of a masking policy. The full name format is `short_name ON database.table`.
- `short_name` ([String](/sql-reference/data-types/string.md)) — Short name of a masking policy. For example, if the full name is `mask_email ON mydb.mytable`, the short name is `mask_email`.
- `database` ([String](/sql-reference/data-types/string.md)) — Database name.
- `table` ([String](/sql-reference/data-types/string.md)) — Table name.
- `id` ([UUID](/sql-reference/data-types/uuid.md)) — Masking policy ID.
- `storage` ([String](/sql-reference/data-types/string.md)) — Name of the directory where the masking policy is stored.
- `update_assignments` ([Nullable(String)](/sql-reference/data-types/nullable.md)) — UPDATE assignments that define how data should be masked. For example: `email = '***masked***', phone = '***-***-****'`.
- `where_condition` ([Nullable(String)](/sql-reference/data-types/nullable.md)) — Optional WHERE condition that specifies when the masking should be applied.
- `priority` ([Int64](/sql-reference/data-types/int-uint.md)) — Priority for applying multiple masking policies. Higher priority policies are applied first. Default is 0.
- `apply_to_all` ([UInt8](/sql-reference/data-types/int-uint.md)) — Shows whether the masking policy applies to all roles and/or users. 1 if true, 0 otherwise.
- `apply_to_list` ([Array(String)](/sql-reference/data-types/array.md)) — List of the roles and/or users to which the masking policy is applied.
- `apply_to_except` ([Array(String)](/sql-reference/data-types/array.md)) — The masking policy is applied to all roles and/or users except the listed ones. Only populated when `apply_to_all` is 1.
