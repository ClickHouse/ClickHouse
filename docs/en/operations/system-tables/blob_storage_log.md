---
slug: /en/operations/system-tables/blob_storage_log
---
# blob_storage_log

Contains information about blob storage operations.

Columns:

- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Date of the entry.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Time of the entry with microseconds precision.
- `event_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type of the operation. Possible values:
    - `Upload`
    - `Delete`
    - `MultiPartUploadCreate`
    - `MultiPartUploadWrite`
    - `MultiPartUploadComplete`
    - `MultiPartUploadAbort`
- `data_size` ([UInt64](../../sql-reference/data-types/uint.md)) — Size of the data in bytes. Only for `Upload` and `MultiPartUploadWrite` events.
- `query_id` ([String](../../sql-reference/data-types/string.md)) — Query ID. Set for write operations where it is possible to determine the query ID.




**Example**

``` sql
SELECT * FROM system.blob_storage_log LIMIT 10
```
