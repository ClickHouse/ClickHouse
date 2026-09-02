ATTACH TABLE _ UUID '360cd2f1-d799-47c7-9086-219cb8a54f27'
(
    `hostname` LowCardinality(String) COMMENT 'Hostname of the server executing the query.',
    `query_id` String COMMENT 'Identifier of the INSERT query that created this data part.',
    `event_type` Enum8('NewPart' = 1, 'MergeParts' = 2, 'DownloadPart' = 3, 'RemovePart' = 4, 'MutatePart' = 5, 'MovePart' = 6, 'MergePartsStart' = 7, 'MutatePartStart' = 8) COMMENT 'Type of the event that occurred with the data part. Can have one of the following values: NewPart — Inserting of a new data part, MergePartsStart — Merging of data parts has started, MergeParts — Merging of data parts has finished, DownloadPart — Downloading a data part, RemovePart — Removing or detaching a data part using [DETACH PARTITION](/reference/statements/alter/partition#detach-partitionpart).MutatePartStart — Mutating of a data part has started, MutatePart — Mutating of a data part has finished, MovePart — Moving the data part from the one disk to another one.',
    `merge_reason` Enum8('NotAMerge' = 1, 'RegularMerge' = 2, 'TTLDeleteMerge' = 3, 'TTLRecompressMerge' = 4, 'TTLDropMerge' = 5) COMMENT 'The reason for the event with type MERGE_PARTS. Can have one of the following values: NotAMerge — The current event has the type other than MERGE_PARTS, RegularMerge — Some regular merge, TTLDeleteMerge, TTLDropMerge — Cleaning up expired data. TTLRecompressMerge — Recompressing data part with the. ',
    `merge_algorithm` Enum8('Undecided' = 0, 'Vertical' = 1, 'Horizontal' = 2) COMMENT 'Merge algorithm for the event with type MERGE_PARTS. Can have one of the following values: Undecided, Horizontal, Vertical',
    `event_date` Date COMMENT 'Event date.',
    `event_time` DateTime COMMENT 'Event time.',
    `event_time_microseconds` DateTime64(6) COMMENT 'Event time with microseconds precision.',
    `duration_ms` UInt64 COMMENT 'Duration of this operation.',
    `database` String COMMENT 'Name of the database the data part is in.',
    `table` String COMMENT 'Name of the table the data part is in.',
    `table_uuid` UUID COMMENT 'UUID of the table the data part belongs to.',
    `part_name` String COMMENT 'Name of the data part.',
    `partition_id` String COMMENT 'ID of the partition that the data part was inserted to. The column takes the `all` value if the partitioning is by `tuple()`.',
    `partition` String COMMENT 'The partition name.',
    `part_type` String COMMENT 'The type of the part. Possible values: Wide and Compact.',
    `part_storage_type` String COMMENT 'The type of `DataPartStorage`. Possible values: `Packed` - most part files are stored in a single archive (projections and a few service files such as `txn_version.txt` are written separately), `Full` - each file is stored separately.',
    `disk_name` String COMMENT 'The disk name data part lies on.',
    `path_on_disk` String COMMENT 'Absolute path to the folder with data part files.',
    `rows` UInt64 COMMENT 'The number of rows in the data part.',
    `size_in_bytes` UInt64 COMMENT 'Size of the data part on disk in bytes.',
    `merged_from` Array(String) COMMENT 'An array of the source parts names which the current part was made up from.',
    `bytes_uncompressed` UInt64 COMMENT 'Uncompressed size of the resulting part in bytes.',
    `read_rows` UInt64 COMMENT 'The number of rows was read during the merge.',
    `read_bytes` UInt64 COMMENT 'The number of bytes was read during the merge.',
    `peak_memory_usage` UInt64 COMMENT 'The maximum amount of used during merge RAM',
    `deduplication_block_ids` Array(String) COMMENT 'An array of block IDs used for deduplication when inserting this part.',
    `error` UInt16 COMMENT 'The error code of the occurred exception.',
    `exception` String COMMENT 'Text message of the occurred error.',
    `mutation_ids` Array(String) COMMENT 'An array of mutation IDs applied to the source part (merged_from) for the event with type MUTATE_PART_START and MUTATE_PART.',
    `ProfileEvents` Map(LowCardinality(String), UInt64) COMMENT 'All the profile events captured during this operation.',
    `projections_duration_ms` Map(LowCardinality(String), UInt64) COMMENT 'Per-projection merge/rebuild duration in milliseconds.',
    `ProfileEvents.Names` Array(String) ALIAS mapKeys(ProfileEvents),
    `ProfileEvents.Values` Array(UInt64) ALIAS mapValues(ProfileEvents),
    `name` String ALIAS part_name,
    INDEX event_time_index event_time TYPE minmax GRANULARITY 1,
    INDEX event_time_microseconds_index event_time_microseconds TYPE minmax GRANULARITY 1,
    INDEX query_id_index query_id TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
SETTINGS index_granularity = 8192
COMMENT 'This table contains information about events that occurred with data parts in the MergeTree family tables, such as adding or merging data.\n\nIt is safe to truncate or drop this table at any time.'
