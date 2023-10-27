---
slug: /en/operations/system-tables/blob_storage_log
---
# blob_storage_log

Contains information about blob storage operations.

Columns:

- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Date of the entry.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Time of the entry with microseconds precision.
- `event_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type of the operation. Possible values:
    - `'Upload'`
    - `'Delete'`
    - `'MultiPartUploadCreate'`
    - `'MultiPartUploadWrite'`
    - `'MultiPartUploadComplete'`
    - `'MultiPartUploadAbort'`


**Example**

``` sql
SELECT * FROM system.blob_storage_log LIMIT 10
```
