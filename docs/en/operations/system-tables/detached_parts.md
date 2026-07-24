---
description: 'System table containing information about detached parts of MergeTree
  tables'
keywords: ['system table', 'detached_parts']
slug: /operations/system-tables/detached_parts
title: 'system.detached_parts'
---

Contains information about detached parts of [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables. The `reason` column specifies why the part was detached.

For user-detached parts, the reason is empty. Such parts can be attached with [ALTER TABLE ATTACH PARTITION\|PART](/sql-reference/statements/alter/partition#attach-partitionpart) command.

For the description of other columns, see [system.parts](../../operations/system-tables/parts.md).

If part name is invalid, values of some columns may be `NULL`. Such parts can be deleted with [ALTER TABLE DROP DETACHED PART](/sql-reference/statements/alter/partition#drop-detached-partitionpart).
