---
slug: /en/operations/system-tables/blob_storage_log
---
# blob_storage_log

Contains logging entries with information about various blob storage operations such as uploads and deletes.

Columns:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Date of the event.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Time of the event.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Time of the event with microseconds precision.
- `event_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type of the event. Possible values:
    - `'Upload'`
    - `'Delete'`
    - `'MultiPartUploadCreate'`
    - `'MultiPartUploadWrite'`
    - `'MultiPartUploadComplete'`
    - `'MultiPartUploadAbort'`
- `query_id` ([String](../../sql-reference/data-types/string.md)) — Identifier of the query associated with the event, if any.
- `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Identifier of the thread performing the operation.
- `thread_name` ([String](../../sql-reference/data-types/string.md)) — Name of the thread performing the operation.
- `disk_name` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) — Name of the associated disk.
- `bucket` ([String](../../sql-reference/data-types/string.md)) — Name of the bucket.
- `remote_path` ([String](../../sql-reference/data-types/string.md)) — Path to the remote resource.
- `local_path` ([String](../../sql-reference/data-types/string.md)) — Path to the metadata file on the local system, which references the remote resource.
- `data_size` ([UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Size of the data involved in the upload event.
- `error` ([String](../../sql-reference/data-types/string.md)) — Error message associated with the event, if any.

**Example**

Suppose a blob storage operation uploads a file, and an event is logged:

```sql
SELECT * FROM system.blob_storage_log WHERE query_id = '7afe0450-504d-4e4b-9a80-cd9826047972' ORDER BY event_date, event_time_microseconds \G
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-10-31
event_time:              2023-10-31 16:03:40
event_time_microseconds: 2023-10-31 16:03:40.481437
event_type:              Upload
query_id:                7afe0450-504d-4e4b-9a80-cd9826047972
thread_id:               2381740
disk_name:               disk_s3
bucket:                  bucket1
remote_path:             rrr/kxo/tbnqtrghgtnxkzgtcrlutwuslgawe
local_path:              store/654/6549e8b3-d753-4447-8047-d462df6e6dbe/tmp_insert_all_1_1_0/checksums.txt
data_size:               259
error:
```

In this example, upload operation was associated with the `INSERT` query with ID `7afe0450-504d-4e4b-9a80-cd9826047972`. The local metadata file `store/654/6549e8b3-d753-4447-8047-d462df6e6dbe/tmp_insert_all_1_1_0/checksums.txt` refers to remote path `rrr/kxo/tbnqtrghgtnxkzgtcrlutwuslgawe` in bucket `bucket1` on disk `disk_s3`, with a size of 259 bytes.

**See Also**

- [External Disks for Storing Data](../../operations/storing-data.md)
