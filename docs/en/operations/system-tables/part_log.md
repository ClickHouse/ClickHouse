---
description: 'System table containing information about events that occurred with
  data parts in the MergeTree family tables, such as adding or merging of data.'
keywords: ['system table', 'part_log']
slug: /operations/system-tables/part_log
title: 'system.part_log'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.part_log

<SystemTableCloud/>

The `system.part_log` table is created only if the [part_log](/operations/server-configuration-parameters/settings#part_log) server setting is specified.

This table contains information about events that occurred with [data parts](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) in the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) family tables, such as adding or merging data.

The `system.part_log` table contains the following columns:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `query_id` ([String](../../sql-reference/data-types/string.md)) — Identifier of the `INSERT` query that created this data part.
- `event_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type of the event that occurred with the data part. Can have one of the following values:
  - `NewPart` — Inserting of a new data part.
  - `MergePartsStart` — Merging of data parts has started.
  - `MergeParts` — Merging of data parts has finished.
  - `DownloadPart` — Downloading a data part.
  - `RemovePart` — Removing or detaching a data part using [DETACH PARTITION](/sql-reference/statements/alter/partition#detach-partitionpart).
  - `MutatePartStart` — Mutating of a data part has started.
  - `MutatePart` — Mutating of a data part has finished.
  - `MovePart` — Moving the data part from the one disk to another one.
- `merge_reason` ([Enum8](../../sql-reference/data-types/enum.md)) — The reason for the event with type `MERGE_PARTS`. Can have one of the following values:
  - `NotAMerge` — The current event has the type other than `MERGE_PARTS`.
  - `RegularMerge` — Some regular merge.
  - `TTLDeleteMerge` — Cleaning up expired data.
  - `TTLRecompressMerge` — Recompressing data part with the.
- `merge_algorithm` ([Enum8](../../sql-reference/data-types/enum.md)) — Merge algorithm for the event with type `MERGE_PARTS`. Can have one of the following values:
  - `Undecided`
  - `Horizontal`
  - `Vertical`
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Event time with microseconds precision.
- `duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Duration.
- `database` ([String](../../sql-reference/data-types/string.md)) — Name of the database the data part is in.
- `table` ([String](../../sql-reference/data-types/string.md)) — Name of the table the data part is in.
- `table_uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — UUID of the table the data part belongs to.
- `part_name` ([String](../../sql-reference/data-types/string.md)) — Name of the data part.
- `partition_id` ([String](../../sql-reference/data-types/string.md)) — ID of the partition that the data part was inserted to. The column takes the `all` value if the partitioning is by `tuple()`.
- `partition` ([String](../../sql-reference/data-types/string.md)) - The partition name.
- `part_type` ([String](../../sql-reference/data-types/string.md)) - The type of the part. Possible values: Wide and Compact.
- `disk_name` ([String](../../sql-reference/data-types/string.md)) - The disk name data part lies on.
- `path_on_disk` ([String](../../sql-reference/data-types/string.md)) — Absolute path to the folder with data part files.
- `rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of rows in the data part.
- `size_in_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Size of the data part in bytes.
- `merged_from` ([Array(String)](../../sql-reference/data-types/array.md)) — An array of names of the parts which the current part was made up from (after the merge).
- `bytes_uncompressed` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Size of uncompressed bytes.
- `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of rows was read during the merge.
- `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of bytes was read during the merge.
- `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — The maximum difference between the amount of allocated and freed memory in the context of this thread.
- `error` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The code number of the occurred error.
- `exception` ([String](../../sql-reference/data-types/string.md)) — Text message of the occurred error.
- `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/map.md)) — ProfileEvents that measure different metrics. The description of them can be found in the table [system.events](/operations/system-tables/events).

The `system.part_log` table is created after the first inserting data to the `MergeTree` table.

**Example**

```sql
SELECT * FROM system.part_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
query_id:
event_type:              MergeParts
merge_reason:            RegularMerge
merge_algorithm:         Vertical
event_date:              2025-07-19
event_time:              2025-07-19 23:54:19
event_time_microseconds: 2025-07-19 23:54:19.710761
duration_ms:             2158
database:                default
table:                   github_events
table_uuid:              1ad33424-f5f5-402b-ac03-ec82282634ab
part_name:               all_1_7_1
partition_id:            all
partition:               tuple()
part_type:               Wide
disk_name:               default
path_on_disk:            ./data/store/1ad/1ad33424-f5f5-402b-ac03-ec82282634ab/all_1_7_1/
rows:                    3285726 -- 3.29 million
size_in_bytes:           438968542 -- 438.97 million
merged_from:             ['all_1_1_0','all_2_2_0','all_3_3_0','all_4_4_0','all_5_5_0','all_6_6_0','all_7_7_0']
bytes_uncompressed:      1373137767 -- 1.37 billion
read_rows:               3285726 -- 3.29 million
read_bytes:              1429206946 -- 1.43 billion
peak_memory_usage:       303611887 -- 303.61 million
error:                   0
exception:
ProfileEvents:           {'FileOpen':703,'ReadBufferFromFileDescriptorRead':3824,'ReadBufferFromFileDescriptorReadBytes':439601681,'WriteBufferFromFileDescriptorWrite':592,'WriteBufferFromFileDescriptorWriteBytes':438988500,'ReadCompressedBytes':439601681,'CompressedReadBufferBlocks':6314,'CompressedReadBufferBytes':1539835748,'OpenedFileCacheHits':50,'OpenedFileCacheMisses':484,'OpenedFileCacheMicroseconds':222,'IOBufferAllocs':1914,'IOBufferAllocBytes':319810140,'ArenaAllocChunks':8,'ArenaAllocBytes':131072,'MarkCacheMisses':7,'CreatedReadBufferOrdinary':534,'DiskReadElapsedMicroseconds':139058,'DiskWriteElapsedMicroseconds':51639,'AnalyzePatchRangesMicroseconds':28,'ExternalProcessingFilesTotal':1,'RowsReadByMainReader':170857759,'WaitMarksLoadMicroseconds':988,'LoadedMarksFiles':7,'LoadedMarksCount':14,'LoadedMarksMemoryBytes':728,'Merge':2,'MergeSourceParts':14,'MergedRows':3285733,'MergedColumns':4,'GatheredColumns':51,'MergedUncompressedBytes':1429207058,'MergeTotalMilliseconds':2158,'MergeExecuteMilliseconds':2155,'MergeHorizontalStageTotalMilliseconds':145,'MergeHorizontalStageExecuteMilliseconds':145,'MergeVerticalStageTotalMilliseconds':2008,'MergeVerticalStageExecuteMilliseconds':2006,'MergeProjectionStageTotalMilliseconds':5,'MergeProjectionStageExecuteMilliseconds':4,'MergingSortedMilliseconds':7,'GatheringColumnMilliseconds':56,'ContextLock':2091,'PartsLockHoldMicroseconds':77,'PartsLockWaitMicroseconds':1,'RealTimeMicroseconds':2157475,'CannotWriteToWriteBufferDiscard':36,'LogTrace':6,'LogDebug':59,'LoggerElapsedNanoseconds':514040,'ConcurrencyControlSlotsGranted':53,'ConcurrencyControlSlotsAcquired':53}
```
