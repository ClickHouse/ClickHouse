---
description: 'System table containing information about the settings of S3Queue tables.
  Available from server version `24.10`.'
keywords: ['system table', 's3_queue_settings']
slug: /operations/system-tables/s3_queue_settings
title: 'system.s3_queue_settings'
---

# system.s3_queue_settings

Contains information about the settings of [S3Queue](../../engines/table-engines/integrations/s3queue.md) tables. Available from server version `24.10`.

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `table` ([String](../../sql-reference/data-types/string.md)) — Database name.
- `name` ([String](../../sql-reference/data-types/string.md)) — Setting name.
- `value` ([String](../../sql-reference/data-types/string.md)) — Setting value.
- `changed` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — Whether the setting was explicitly defined in the config or explicitly changed.
- `description` ([String](../../sql-reference/data-types/string.md)) — Setting description.
- `alterable` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — Shows whether the setting can be changes via `ALTER TABLE ... MODIFY SETTING`.
    - `0` — Current user can alter the setting.
    - `1` — Current user can't alter the setting.
- `type` ([String](../../sql-reference/data-types/string.md)) — Setting type (implementation specific string value).
