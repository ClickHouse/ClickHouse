---
description: 'System iceberg snapshot history'
keywords: ['system iceberg_history']
slug: /operations/system-tables/iceberg_history
title: 'system.iceberg_history'
---

# system.iceberg_history

Contains snapshot history of iceberg table.

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — The name of the database the table is in.

- `name` ([String](../../sql-reference/data-types/string.md)) — Table name.

- `made_current_at` ([DateTime](../../sql-reference/data-types/uuid.md)) — Time when the snapshot was made current snapshot.

- `snapshot_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — Snapshot id.

- `parent_id` ([Int64](../../sql-reference/data-types/int-uint.md)) - Snapshot id of the parent snapshot.

- `is_current_ancestor` ([Bool](../../sql-reference/data-types/boolean.md)) - Flag that indicates whether this snapshot is an ancestor of the current snapshot.
