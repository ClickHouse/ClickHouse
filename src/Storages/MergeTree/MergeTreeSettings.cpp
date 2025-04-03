#include <Columns/IColumn.h>
#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/BaseSettingsProgramOptions.h>
#include <Core/MergeSelectorAlgorithm.h>
#include <Core/SettingsChangesHistory.h>
#include <Disks/DiskFromAST.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/isDiskFunction.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Common/Exception.h>
#include <Common/NamePrompter.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <boost/program_options.hpp>
#include <fmt/ranges.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>

#include "config.h"

#if !CLICKHOUSE_CLOUD
constexpr UInt64 default_min_bytes_for_wide_part = 10485760lu;
constexpr bool default_allow_remote_fs_zero_copy_replication = false;
#else
constexpr UInt64 default_min_bytes_for_wide_part = 1024lu * 1024lu * 1024lu;
constexpr bool default_allow_remote_fs_zero_copy_replication = true; /// TODO: Fix
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int BAD_ARGUMENTS;
    extern const int READONLY;
}

// clang-format off

/** These settings represent fine tunes for internal details of MergeTree storages
  * and should not be changed by the user without a reason.
  */
#define MERGE_TREE_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, min_compress_block_size, 0, R"(
    Minimum size of blocks of uncompressed data required for compression when
    writing the next mark. You can also specify this setting in the global settings
    (see [min_compress_block_size](/operations/settings/merge-tree-settings#min_compress_block_size)
    setting). The value specified when the table is created overrides the global value
    for this setting.
    )", 0) \
    DECLARE(UInt64, max_compress_block_size, 0, R"(
    Maximum size of blocks of uncompressed data before compressing for writing
    to a table. You can also specify this setting in the global settings
    (see [max_compress_block_size](/operations/settings/merge-tree-settings#max_compress_block_size)
    setting). The value specified when the table is created overrides the global
    value for this setting.
    )", 0) \
    DECLARE(UInt64, index_granularity, 8192, R"(
    Maximum number of data rows between the marks of an index. I.e how many rows
    correspond to one primary key value.
    )", 0) \
    DECLARE(UInt64, max_digestion_size_per_segment, 256_MiB, R"(
    Max number of bytes to digest per segment to build GIN index.
    )", 0) \
    \
    /** Data storing format settings. */ \
    DECLARE(UInt64, min_bytes_for_wide_part, default_min_bytes_for_wide_part, R"(
    Minimum number of bytes/rows in a data part that can be stored in `Wide`
    format. You can set one, both or none of these settings.
    )", 0) \
    DECLARE(UInt64, min_rows_for_wide_part, 0, R"(
    Minimal number of rows to create part in wide format instead of compact
    )", 0) \
    DECLARE(UInt64, max_merge_delayed_streams_for_parallel_write, 40, R"(
    The maximum number of streams (columns) that can be flushed in parallel
    (analog of max_insert_delayed_streams_for_parallel_write for merges). Works
    only for Vertical merges.
    )", 0) \
    DECLARE(Float, ratio_of_defaults_for_sparse_serialization, 0.9375f, R"(
    Minimal ratio of the number of _default_ values to the number of _all_ values
    in a column. Setting this value causes the column to be stored using sparse
    serializations.

    If a column is sparse (contains mostly zeros), ClickHouse can encode it in
    a sparse format and automatically optimize calculations - the data does not
    require full decompression during queries. To enable this sparse
    serialization, define the `ratio_of_defaults_for_sparse_serialization`
    setting to be less than 1.0. If the value is greater than or equal to 1.0,
    then the columns will be always written using the normal full serialization.

    Possible values:

    - Float between `0` and `1` to enable sparse serialization
    - `1.0` (or greater) if you do not want to use sparse serialization

    **Example**

    Notice the `s` column in the following table is an empty string for 95% of
    the rows. In `my_regular_table` we do not use sparse serialization, and in
    `my_sparse_table` we set `ratio_of_defaults_for_sparse_serialization` to
    0.95:

    ```sql
    CREATE TABLE my_regular_table
    (
        `id` UInt64,
        `s` String
    )
    ENGINE = MergeTree
    ORDER BY id;

    INSERT INTO my_regular_table
    SELECT
        number AS id,
        number % 20 = 0 ? toString(number): '' AS s
    FROM
        numbers(10000000);


    CREATE TABLE my_sparse_table
    (
        `id` UInt64,
        `s` String
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.95;

    INSERT INTO my_sparse_table
    SELECT
        number,
        number % 20 = 0 ? toString(number): ''
    FROM
        numbers(10000000);
    ```

    Notice the `s` column in `my_sparse_table` uses less storage space on disk:

    ```sql
    SELECT table, name, data_compressed_bytes, data_uncompressed_bytes FROM system.columns
    WHERE table LIKE 'my_%_table';
    ```

    ```response
    ┌─table────────────┬─name─┬─data_compressed_bytes─┬─data_uncompressed_bytes─┐
    │ my_regular_table │ id   │              37790741 │                75488328 │
    │ my_regular_table │ s    │               2451377 │                12683106 │
    │ my_sparse_table  │ id   │              37790741 │                75488328 │
    │ my_sparse_table  │ s    │               2283454 │                 9855751 │
    └──────────────────┴──────┴───────────────────────┴─────────────────────────┘
    ```

    You can verify if a column is using the sparse encoding by viewing the
    `serialization_kind` column of the `system.parts_columns` table:

    ```sql
    SELECT column, serialization_kind FROM system.parts_columns
    WHERE table LIKE 'my_sparse_table';
    ```

    You can see which parts of `s` were stored using the sparse serialization:

    ```response
    ┌─column─┬─serialization_kind─┐
    │ id     │ Default            │
    │ s      │ Default            │
    │ id     │ Default            │
    │ s      │ Default            │
    │ id     │ Default            │
    │ s      │ Sparse             │
    │ id     │ Default            │
    │ s      │ Sparse             │
    │ id     │ Default            │
    │ s      │ Sparse             │
    │ id     │ Default            │
    │ s      │ Sparse             │
    │ id     │ Default            │
    │ s      │ Sparse             │
    │ id     │ Default            │
    │ s      │ Sparse             │
    │ id     │ Default            │
    │ s      │ Sparse             │
    │ id     │ Default            │
    │ s      │ Sparse             │
    │ id     │ Default            │
    │ s      │ Sparse             │
    └────────┴────────────────────┘
    ```
    )", 0) \
    DECLARE(Bool, replace_long_file_name_to_hash, true, R"(
    If the file name for column is too long (more than 'max_file_name_length'
    bytes) replace it to SipHash128
    )", 0) \
    DECLARE(UInt64, max_file_name_length, 127, R"(
    The maximal length of the file name to keep it as is without hashing.
    Takes effect only if setting `replace_long_file_name_to_hash` is enabled.
    The value of this setting does not include the length of file extension. So,
    it is recommended to set it below the maximum filename length (usually 255
    bytes) with some gap to avoid filesystem errors.
    )", 0) \
    DECLARE(UInt64, min_bytes_for_full_part_storage, 0, R"(
    Only available in ClickHouse Cloud. Minimal uncompressed size in bytes to
    use full type of storage for data part instead of packed
    )", 0) \
    DECLARE(UInt64, min_rows_for_full_part_storage, 0, R"(
    Only available in ClickHouse Cloud. Minimal number of rows to use full type
    of storage for data part instead of packed
    )", 0) \
    DECLARE(UInt64, compact_parts_max_bytes_to_buffer, 128 * 1024 * 1024, R"(
    Only available in ClickHouse Cloud. Maximal number of bytes to write in a
    single stripe in compact parts
    )", 0) \
    DECLARE(UInt64, compact_parts_max_granules_to_buffer, 128, R"(
    Only available in ClickHouse Cloud. Maximal number of granules to write in a
    single stripe in compact parts
    )", 0) \
    DECLARE(UInt64, compact_parts_merge_max_bytes_to_prefetch_part, 16 * 1024 * 1024, R"(
    Only available in ClickHouse Cloud. Maximal size of compact part to read it
    in a whole to memory during merge.
    )", 0) \
    DECLARE(UInt64, merge_max_bytes_to_prewarm_cache, 1ULL * 1024 * 1024 * 1024, R"(
    Only available in ClickHouse Cloud. Maximal size of part (compact or packed)
    to prewarm cache during merge.
    )", 0) \
    DECLARE(UInt64, merge_total_max_bytes_to_prewarm_cache, 15ULL * 1024 * 1024 * 1024, R"(
    Only available in ClickHouse Cloud. Maximal size of parts in total to prewarm
    cache during merge.
    )", 0) \
    DECLARE(Bool, load_existing_rows_count_for_old_parts, false, R"(
    If enabled along with [exclude_deleted_rows_for_part_size_in_merge](#exclude_deleted_rows_for_part_size_in_merge),
    deleted rows count for existing data parts will be calculated during table
    starting up. Note that it may slow down start up table loading.

    Possible values:
    - `true`
    - `false`

    **See Also**
    - [exclude_deleted_rows_for_part_size_in_merge](#exclude_deleted_rows_for_part_size_in_merge) setting
    )", 0) \
    DECLARE(Bool, use_compact_variant_discriminators_serialization, true, R"(
    Enables compact mode for binary serialization of discriminators in Variant
    data type.
    This mode allows to use significantly less memory for storing discriminators
    in parts when there is mostly one variant or a lot of NULL values.
    )", 0) \
    \
    /** Merge selector settings. */ \
    DECLARE(UInt64, merge_selector_blurry_base_scale_factor, 0, R"(
    Controls when the logic kicks in relatively to the number of parts in
    partition. The bigger the factor the more belated reaction will be.
    )", 0) \
    DECLARE(UInt64, merge_selector_window_size, 1000, R"(
    How many parts to look at once.
    )", 0) \
    \
    /** Merge settings. */ \
    DECLARE(UInt64, merge_max_block_size, 8192, R"(
    The number of rows that are read from the merged parts into memory.

    Possible values:
    - Any positive integer.

    Merge reads rows from parts in blocks of `merge_max_block_size` rows, then
    merges and writes the result into a new part. The read block is placed in RAM,
    so `merge_max_block_size` affects the size of the RAM required for the merge.
    Thus, merges can consume a large amount of RAM for tables with very wide rows
    (if the average row size is 100kb, then when merging 10 parts,
    (100kb * 10 * 8192) = ~ 8GB of RAM). By decreasing `merge_max_block_size`,
    you can reduce the amount of RAM required for a merge but slow down a merge.
    )", 0) \
    DECLARE(UInt64, merge_max_block_size_bytes, 10 * 1024 * 1024, R"(
    How many bytes in blocks should be formed for merge operations. By default
    has the same value as `index_granularity_bytes`.
    )", 0) \
    DECLARE(UInt64, max_bytes_to_merge_at_max_space_in_pool, 150ULL * 1024 * 1024 * 1024, R"(
    The maximum total parts size (in bytes) to be merged into one part, if there
    are enough resources available. Corresponds roughly to the maximum possible
    part size created by an automatic background merge.

    Possible values:

    - Any positive integer.

    The merge scheduler periodically analyzes the sizes and number of parts in
    partitions, and if there are enough free resources in the pool, it starts
    background merges. Merges occur until the total size of the source parts is
    larger than `max_bytes_to_merge_at_max_space_in_pool`.

    Merges initiated by [OPTIMIZE FINAL](/sql-reference/statements/optimize)
    ignore `max_bytes_to_merge_at_max_space_in_pool` (only the free disk space
    is taken into account).
    )", 0) \
    DECLARE(UInt64, max_bytes_to_merge_at_min_space_in_pool, 1024 * 1024, R"(
    The maximum total part size (in bytes) to be merged into one part, with the
    minimum available resources in the background pool.

    Possible values:
    - Any positive integer.

    `max_bytes_to_merge_at_min_space_in_pool` defines the maximum total size of
    parts which can be merged despite the lack of available disk space (in pool).
    This is necessary to reduce the number of small parts and the chance of
    `Too many parts` errors.
    Merges book disk space by doubling the total merged parts sizes.
    Thus, with a small amount of free disk space, a situation may occur in which
    there is free space, but this space is already booked by ongoing large merges,
    so other merges are unable to start, and the number of small parts grows
    with every insert.
    )", 0) \
    DECLARE(UInt64, max_replicated_merges_in_queue, 1000, R"(
    How many tasks of merging and mutating parts are allowed simultaneously in
    ReplicatedMergeTree queue.
    )", 0) \
    DECLARE(UInt64, max_replicated_mutations_in_queue, 8, R"(
    How many tasks of mutating parts are allowed simultaneously in
    ReplicatedMergeTree queue.
    )", 0) \
    DECLARE(UInt64, max_replicated_merges_with_ttl_in_queue, 1, R"(
    How many tasks of merging parts with TTL are allowed simultaneously in
    ReplicatedMergeTree queue.
    )", 0) \
    DECLARE(UInt64, number_of_free_entries_in_pool_to_lower_max_size_of_merge, 8, R"(
    When there is less than the specified number of free entries in pool
    (or replicated queue), start to lower maximum size of merge to process
    (or to put in queue).
    This is to allow small merges to process - not filling the pool with long
    running merges.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(UInt64, number_of_free_entries_in_pool_to_execute_mutation, 20, R"(
    When there is less than specified number of free entries in pool, do not
    execute part mutations. This is to leave free threads for regular merges and
    to avoid "Too many parts" errors.

    Possible values:
    - Any positive integer.

    **Usage**

    The value of the `number_of_free_entries_in_pool_to_execute_mutation` setting
    should be less than the value of the [background_pool_size](/operations/server-configuration-parameters/settings.md/#background_pool_size)
    * [background_merges_mutations_concurrency_ratio](/operations/server-configuration-parameters/settings.md/#background_merges_mutations_concurrency_ratio).
    Otherwise, ClickHouse will throw an exception.
    )", 0) \
    DECLARE(UInt64, max_number_of_mutations_for_replica, 0, R"(
    Limit the number of part mutations per replica to the specified amount.
    Zero means no limit on the number of mutations per replica (the execution can
    still be constrained by other settings).
    )", 0) \
    DECLARE(UInt64, max_number_of_merges_with_ttl_in_pool, 2, R"(When there is
    more than specified number of merges with TTL entries in pool, do not assign
    new merge with TTL. This is to leave free threads for regular merges and
    avoid \"Too many parts\"
    )", 0) \
    DECLARE(Seconds, old_parts_lifetime, 8 * 60, R"(
    The time (in seconds) of storing inactive parts to protect against data loss
    during spontaneous server reboots.

    Possible values:
    - Any positive integer.

    After merging several parts into a new part, ClickHouse marks the original
    parts as inactive and deletes them only after `old_parts_lifetime` seconds.
    Inactive parts are removed if they are not used by current queries, i.e. if
    the `refcount` of the part is 1.

    `fsync` is not called for new parts, so for some time new parts exist only
    in the server's RAM (OS cache). If the server is rebooted spontaneously, new
    parts can be lost or damaged. To protect data inactive parts are not deleted
    immediately.

    During startup ClickHouse checks the integrity of the parts. If the merged
    part is damaged ClickHouse returns the inactive parts to the active list,
    and later merges them again. Then the damaged part is renamed (the `broken_`
    prefix is added) and moved to the `detached` folder. If the merged part is
    not damaged, then the original inactive parts are renamed (the `ignored_`
    prefix is added) and moved to the `detached` folder.

    The default `dirty_expire_centisecs` value (a Linux kernel setting) is 30
    seconds (the maximum time that written data is stored only in RAM), but under
    heavy loads on the disk system data can be written much later. Experimentally,
    a value of 480 seconds was chosen for `old_parts_lifetime`, during which a
    new part is guaranteed to be written to disk.
    )", 0) \
    DECLARE(Seconds, temporary_directories_lifetime, 86400, R"(
    How many seconds to keep tmp_-directories. You should not lower this value
    because merges and mutations may not be able to work with low value of this
    setting.
    )", 0) \
    DECLARE(Seconds, lock_acquire_timeout_for_background_operations, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, R"(
    For background operations like merges, mutations etc. How many seconds before
    failing to acquire table locks.
    )", 0) \
    DECLARE(UInt64, min_rows_to_fsync_after_merge, 0, R"(
    Minimal number of rows to do fsync for part after merge (0 - disabled)
    )", 0) \
    DECLARE(UInt64, min_compressed_bytes_to_fsync_after_merge, 0, R"(
    Minimal number of compressed bytes to do fsync for part after merge (0 - disabled)
    )", 0) \
    DECLARE(UInt64, min_compressed_bytes_to_fsync_after_fetch, 0, R"(
    Minimal number of compressed bytes to do fsync for part after fetch (0 - disabled)
    )", 0) \
    DECLARE(Bool, fsync_after_insert, false, R"(
    Do fsync for every inserted part. Significantly decreases performance of
    inserts, not recommended to use with wide parts.
    )", 0) \
    DECLARE(Bool, fsync_part_directory, false, R"(
    Do fsync for part directory after all part operations (writes, renames, etc.).
    )", 0) \
    DECLARE(UInt64, non_replicated_deduplication_window, 0, R"(
    The number of the most recently inserted blocks in the non-replicated
    [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table
    for which hash sums are stored to check for duplicates.

    Possible values:
    - Any positive integer.
    - `0` (disable deduplication).

    A deduplication mechanism is used, similar to replicated tables (see
    [replicated_deduplication_window](#replicated_deduplication_window) setting).
    The hash sums of the created parts are written to a local file on a disk.
    )", 0) \
    DECLARE(UInt64, max_parts_to_merge_at_once, 100, R"(
    Max amount of parts which can be merged at once (0 - disabled). Doesn't affect
    OPTIMIZE FINAL query.
    )", 0) \
    DECLARE(Bool, materialize_skip_indexes_on_merge, true, R"(
    When enabled, merges build and store skip indices for new parts.
    Otherwise they can be created/stored by explicit MATERIALIZE INDEX
    )", 0) \
    DECLARE(UInt64, merge_selecting_sleep_ms, 5000, R"(
    Minimum time to wait before trying to select parts to merge again after no
    parts were selected. A lower setting will trigger selecting tasks in
    background_schedule_pool frequently which result in large amount of requests
    to zookeeper in large-scale clusters
    )", 0) \
    DECLARE(UInt64, max_merge_selecting_sleep_ms, 60000, R"(
    Maximum time to wait before trying to select parts to merge again after no
    parts were selected. A lower setting will trigger selecting tasks in
    background_schedule_pool frequently which result in large amount of
    requests to zookeeper in large-scale clusters
    )", 0) \
    DECLARE(Float, merge_selecting_sleep_slowdown_factor, 1.2f, R"(
    The sleep time for merge selecting task is multiplied by this factor when
    there's nothing to merge and divided when a merge was assigned
    )", 0) \
    DECLARE(UInt64, merge_tree_clear_old_temporary_directories_interval_seconds, 60, R"(
    Sets the interval in seconds for ClickHouse to execute the cleanup of old
    temporary directories.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(UInt64, merge_tree_clear_old_parts_interval_seconds, 1, R"(
    Sets the interval in seconds for ClickHouse to execute the cleanup of old
    parts, WALs, and mutations.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(UInt64, min_age_to_force_merge_seconds, 0, R"(
    Merge parts if every part in the range is older than the value of
    `min_age_to_force_merge_seconds`.

    By default, ignores setting `max_bytes_to_merge_at_max_space_in_pool`
    (see `enable_max_bytes_limit_for_min_age_to_force_merge`).

    Possible values:
    - Positive integer.
    )", 0) \
    DECLARE(Bool, min_age_to_force_merge_on_partition_only, false, R"(
    Whether `min_age_to_force_merge_seconds` should be applied only on the entire
    partition and not on subset.

    By default, ignores setting `max_bytes_to_merge_at_max_space_in_pool` (see
    `enable_max_bytes_limit_for_min_age_to_force_merge`).

    Possible values:
    - true, false
    )", false) \
    DECLARE(Bool, enable_max_bytes_limit_for_min_age_to_force_merge, false, R"(
    If settings `min_age_to_force_merge_seconds` and
    `min_age_to_force_merge_on_partition_only` should respect setting
    `max_bytes_to_merge_at_max_space_in_pool`.

    Possible values:
    - `true`
    - `false`
    )", false) \
    DECLARE(UInt64, number_of_free_entries_in_pool_to_execute_optimize_entire_partition, 25, R"(
    When there is less than specified number of free entries in pool, do not
    execute optimizing entire partition in the background (this task generated
    when set `min_age_to_force_merge_seconds` and enable
    `min_age_to_force_merge_on_partition_only`). This is to leave free threads
    for regular merges and avoid "Too many parts".

    Possible values:
    - Positive integer.

    The value of the `number_of_free_entries_in_pool_to_execute_optimize_entire_partition`
    setting should be less than the value of the
    [background_pool_size](/operations/server-configuration-parameters/settings.md/#background_pool_size)
    * [background_merges_mutations_concurrency_ratio](/operations/server-configuration-parameters/settings.md/#background_merges_mutations_concurrency_ratio).
    Otherwise, ClickHouse throws an exception.
    )", 0) \
    DECLARE(Bool, remove_rolled_back_parts_immediately, 1, R"(
    Setting for an incomplete experimental feature.
    )", EXPERIMENTAL) \
    DECLARE(UInt64, replicated_max_mutations_in_one_entry, 10000, R"(
    Max number of mutation commands that can be merged together and executed in
    one MUTATE_PART entry (0 means unlimited)
    )", 0) \
    DECLARE(UInt64, number_of_mutations_to_delay, 500, R"(If table has at least
    that many unfinished mutations, artificially slow down mutations of table.
    Disabled if set to 0
    )", 0) \
    DECLARE(UInt64, number_of_mutations_to_throw, 1000, R"(
    If table has at least that many unfinished mutations, throw 'Too many mutations'
    exception. Disabled if set to 0
    )", 0) \
    DECLARE(UInt64, min_delay_to_mutate_ms, 10, R"(
    Min delay of mutating MergeTree table in milliseconds, if there are a lot of
    unfinished mutations
    )", 0) \
    DECLARE(UInt64, max_delay_to_mutate_ms, 1000, R"(
    Max delay of mutating MergeTree table in milliseconds, if there are a lot of
    unfinished mutations
    )", 0) \
    DECLARE(Bool, exclude_deleted_rows_for_part_size_in_merge, false, R"(
    If enabled, estimated actual size of data parts (i.e., excluding those rows
    that have been deleted through `DELETE FROM`) will be used when selecting
    parts to merge. Note that this behavior is only triggered for data parts
    affected by `DELETE FROM` executed after this setting is enabled.

    Possible values:
    - `true`
    - `false`

    **See Also**
    - [load_existing_rows_count_for_old_parts](#load_existing_rows_count_for_old_parts)
      setting
    )", 0) \
    DECLARE(String, merge_workload, "", R"(
    Used to regulate how resources are utilized and shared between merges and
    other workloads. Specified value is used as `workload` setting value for
    background merges of this table. If not specified (empty string), then
    server setting `merge_workload` is used instead.

    **See Also**
    - [Workload Scheduling](/operations/workload-scheduling.md)
    )", 0) \
    DECLARE(String, mutation_workload, "", R"(
    Used to regulate how resources are utilized and shared between mutations and
    other workloads. Specified value is used as `workload` setting value for
    background mutations of this table. If not specified (empty string), then
    server setting `mutation_workload` is used instead.

    **See Also**
    - [Workload Scheduling](/operations/workload-scheduling.md)
    )", 0) \
    DECLARE(Milliseconds, background_task_preferred_step_execution_time_ms, 50, R"(
    Target time to execution of one step of merge or mutation. Can be exceeded if
    one step takes longer time
    )", 0) \
    DECLARE(Bool, enforce_index_structure_match_on_partition_manipulation, false, R"(
    If this setting is enabled for destination table of a partition manipulation
    query (`ATTACH/MOVE/REPLACE PARTITION`), the indices and projections must be
    identical between the source and destination tables. Otherwise, the destination
    table can have a superset of the source table's indices and projections.
    )", 0) \
    DECLARE(MergeSelectorAlgorithm, merge_selector_algorithm, MergeSelectorAlgorithm::SIMPLE, R"(
    The algorithm to select parts for merges assignment
    )", EXPERIMENTAL) \
    DECLARE(Bool, merge_selector_enable_heuristic_to_remove_small_parts_at_right, true, R"(
    Enable heuristic for selecting parts for merge which removes parts from right
    side of range, if their size is less than specified ratio (0.01) of sum_size.
    Works for Simple and StochasticSimple merge selectors
    )", 0) \
    DECLARE(Float, merge_selector_base, 5.0, R"(Affects write amplification of
    assigned merges (expert level setting, don't change if you don't understand
    what it is doing). Works for Simple and StochasticSimple merge selectors
    )", 0) \
    DECLARE(UInt64, min_parts_to_merge_at_once, 0, R"(
    Minimal amount of data parts which merge selector can pick to merge at once
    (expert level setting, don't change if you don't understand what it is doing).
    0 - disabled. Works for Simple and StochasticSimple merge selectors.
    )", 0) \
    \
    /** Inserts settings. */ \
    DECLARE(UInt64, parts_to_delay_insert, 1000, R"(
    If the number of active parts in a single partition exceeds the
    `parts_to_delay_insert` value, an `INSERT` is artificially slowed down.

    Possible values:
    - Any positive integer.

    ClickHouse artificially executes `INSERT` longer (adds 'sleep') so that the
    background merge process can merge parts faster than they are added.
    )", 0) \
    DECLARE(UInt64, inactive_parts_to_delay_insert, 0, R"(
    If the number of inactive parts in a single partition in the table exceeds
    the `inactive_parts_to_delay_insert` value, an `INSERT` is artificially
    slowed down.

    :::tip
    It is useful when a server fails to clean up parts quickly enough.
    :::

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(UInt64, parts_to_throw_insert, 3000, R"(
    If the number of active parts in a single partition exceeds the
    `parts_to_throw_insert` value, `INSERT` is interrupted with the `Too many
    parts (N). Merges are processing significantly slower than inserts`
    exception.

    Possible values:
    - Any positive integer.

    To achieve maximum performance of `SELECT` queries, it is necessary to
    minimize the number of parts processed, see [Merge Tree](/development/architecture#merge-tree).

    Prior to version 23.6 this setting was set to 300. You can set a higher
    different value, it will reduce the probability of the `Too many parts`
    error, but at the same time `SELECT` performance might degrade. Also in case
    of a merge issue (for example, due to insufficient disk space) you will
    notice it later than you would with the original 300.

    )", 0) \
    DECLARE(UInt64, inactive_parts_to_throw_insert, 0, R"(
    If the number of inactive parts in a single partition more than the
    `inactive_parts_to_throw_insert` value, `INSERT` is interrupted with the
    following error:

    > "Too many inactive parts (N). Parts cleaning are processing significantly
      slower than inserts" exception."

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(UInt64, max_avg_part_size_for_too_many_parts, 1ULL * 1024 * 1024 * 1024, R"(
    The 'too many parts' check according to 'parts_to_delay_insert' and
    'parts_to_throw_insert' will be active only if the average part size (in the
    relevant partition) is not larger than the specified threshold. If it is
    larger than the specified threshold, the INSERTs will be neither delayed or
    rejected. This allows to have hundreds of terabytes in a single table on a
    single server if the parts are successfully merged to larger parts. This
    does not affect the thresholds on inactive parts or total parts.
    )", 0) \
    DECLARE(UInt64, max_delay_to_insert, 1, R"(
    The value in seconds, which is used to calculate the `INSERT` delay, if the
    number of active parts in a single partition exceeds the
    [parts_to_delay_insert](#parts_to_delay_insert) value.

    Possible values:
    - Any positive integer.

    The delay (in milliseconds) for `INSERT` is calculated by the formula:

    ```code
    max_k = parts_to_throw_insert - parts_to_delay_insert
    k = 1 + parts_count_in_partition - parts_to_delay_insert
    delay_milliseconds = pow(max_delay_to_insert * 1000, k / max_k)
    ```
    For example, if a partition has 299 active parts and parts_to_throw_insert
    = 300, parts_to_delay_insert = 150, max_delay_to_insert = 1, `INSERT` is
    delayed for `pow( 1 * 1000, (1 + 299 - 150) / (300 - 150) ) = 1000`
    milliseconds.

    Starting from version 23.1 formula has been changed to:

    ```code
    allowed_parts_over_threshold = parts_to_throw_insert - parts_to_delay_insert
    parts_over_threshold = parts_count_in_partition - parts_to_delay_insert + 1
    delay_milliseconds = max(min_delay_to_insert_ms, (max_delay_to_insert * 1000)
    * parts_over_threshold / allowed_parts_over_threshold)
    ```

    For example, if a partition has 224 active parts and parts_to_throw_insert
    = 300, parts_to_delay_insert = 150, max_delay_to_insert = 1,
    min_delay_to_insert_ms = 10, `INSERT` is delayed for `max( 10, 1 * 1000 *
    (224 - 150 + 1) / (300 - 150) ) = 500` milliseconds.
    )", 0) \
    DECLARE(UInt64, min_delay_to_insert_ms, 10, R"(
    Min delay of inserting data into MergeTree table in milliseconds, if there
    are a lot of unmerged parts in single partition.
    )", 0) \
    DECLARE(UInt64, max_parts_in_total, 100000, R"(
    If the total number of active parts in all partitions of a table exceeds the
    `max_parts_in_total` value `INSERT` is interrupted with the `Too many parts
    (N)` exception.

    Possible values:
    - Any positive integer.

    A large number of parts in a table reduces performance of ClickHouse queries
    and increases ClickHouse boot time. Most often this is a consequence of an
    incorrect design (mistakes when choosing a partitioning strategy - too small
    partitions).
    )", 0) \
    DECLARE(Bool, async_insert, false, R"(
    If true, data from INSERT query is stored in queue and later flushed to
    table in background.
    )", 0) \
    DECLARE(Bool, add_implicit_sign_column_constraint_for_collapsing_engine, false, R"(
    If true, adds an implicit constraint for the `sign` column of a CollapsingMergeTree
    or VersionedCollapsingMergeTree table to allow only valid values (`1` and `-1`).
    )", 0) \
    DECLARE(Milliseconds, sleep_before_commit_local_part_in_replicated_table_ms, 0, R"(
    For testing. Do not change it.
    )", 0) \
    DECLARE(Bool, optimize_row_order, false, R"(
    Controls if the row order should be optimized during inserts to improve the
    compressability of the newly inserted table part.

    Only has an effect for ordinary MergeTree-engine tables. Does nothing for
    specialized MergeTree engine tables (e.g. CollapsingMergeTree).

    MergeTree tables are (optionally) compressed using [compression codecs](/sql-reference/statements/create/table#column_compression_codec).
    Generic compression codecs such as LZ4 and ZSTD achieve maximum compression
    rates if the data exposes patterns. Long runs of the same value typically
    compress very well.

    If this setting is enabled, ClickHouse attempts to store the data in newly
    inserted parts in a row order that minimizes the number of equal-value runs
    across the columns of the new table part.
    In other words, a small number of equal-value runs mean that individual runs
    are long and compress well.

    Finding the optimal row order is computationally infeasible (NP hard).
    Therefore, ClickHouse uses a heuristics to quickly find a row order which
    still improves compression rates over the original row order.

    <details markdown="1">

    <summary>Heuristics for finding a row order</summary>

    It is generally possible to shuffle the rows of a table (or table part)
    freely as SQL considers the same table (table part) in different row order
    equivalent.

    This freedom of shuffling rows is restricted when a primary key is defined
    for the table. In ClickHouse, a primary key `C1, C2, ..., CN` enforces that
    the table rows are sorted by columns `C1`, `C2`, ... `Cn` ([clustered index](https://en.wikipedia.org/wiki/Database_index#Clustered)).
    As a result, rows can only be shuffled within "equivalence classes" of row,
    i.e. rows which have the same values in their primary key columns.
    The intuition is that primary keys with high-cardinality, e.g. primary keys
    involving a `DateTime64` timestamp column, lead to many small equivalence
    classes. Likewise, tables with a low-cardinality primary key, create few and
    large equivalence classes. A table with no primary key represents the extreme
    case of a single equivalence class which spans all rows.

    The fewer and the larger the equivalence classes are, the higher the degree
    of freedom when re-shuffling rows.

    The heuristics applied to find the best row order within each equivalence
    class is suggested by D. Lemire, O. Kaser in
    [Reordering columns for smaller indexes](https://doi.org/10.1016/j.ins.2011.02.002)
    and based on sorting the rows within each equivalence class by ascending
    cardinality of the non-primary key columns.

    It performs three steps:
    1. Find all equivalence classes based on the row values in primary key columns.
    2. For each equivalence class, calculate (usually estimate) the cardinalities
       of the non-primary-key columns.
    3. For each equivalence class, sort the rows in order of ascending
       non-primary-key column cardinality.

    </details>

    If enabled, insert operations incur additional CPU costs to analyze and
    optimize the row order of the new data. INSERTs are expected to take 30-50%
    longer depending on the data characteristics.
    Compression rates of LZ4 or ZSTD improve on average by 20-40%.

    This setting works best for tables with no primary key or a low-cardinality
    primary key, i.e. a table with only few distinct primary key values.
    High-cardinality primary keys, e.g. involving timestamp columns of type
    `DateTime64`, are not expected to benefit from this setting.
    )", 0) \
    DECLARE(Bool, use_adaptive_write_buffer_for_dynamic_subcolumns, true, R"(
    Allow to use adaptive writer buffers during writing dynamic subcolumns to
    reduce memory usage
    )", 0) \
    DECLARE(UInt64, adaptive_write_buffer_initial_size, 16 * 1024, R"(
    Initial size of an adaptive write buffer
    )", 0) \
    DECLARE(UInt64, min_free_disk_bytes_to_perform_insert, 0, R"(
    The minimum number of bytes that should be free in disk space in order to
    insert data. If the number of available free bytes is less than
    `min_free_disk_bytes_to_perform_insert` then an exception is thrown and the
    insert is not executed. Note that this setting:
    - takes into account the `keep_free_space_bytes` setting.
    - does not take into account the amount of data that will be written by the
      `INSERT` operation.
    - is only checked if a positive (non-zero) number of bytes is specified

    Possible values:
    - Any positive integer.

    :::note
    If both `min_free_disk_bytes_to_perform_insert` and `min_free_disk_ratio_to_perform_insert`
    are specified, ClickHouse will count on the value that will allow to perform
    inserts on a bigger amount of free memory.
    :::
    )", 0) \
    DECLARE(Float, min_free_disk_ratio_to_perform_insert, 0.0, R"(
    The minimum free to total disk space ratio to perform an `INSERT`. Must be a
    floating point value between 0 and 1. Note that this setting:
    - takes into account the `keep_free_space_bytes` setting.
    - does not take into account the amount of data that will be written by the
      `INSERT` operation.
    - is only checked if a positive (non-zero) ratio is specified

    Possible values:
    - Float, 0.0 - 1.0

    Note that if both `min_free_disk_ratio_to_perform_insert` and
    `min_free_disk_bytes_to_perform_insert` are specified, ClickHouse will count
    on the value that will allow to perform inserts on a bigger amount of free
    memory.
    )", 0) \
    \
    /* Part removal settings. */ \
    DECLARE(UInt64, simultaneous_parts_removal_limit, 0, R"(
    If there are a lot of outdated parts cleanup thread will try to delete up to
    `simultaneous_parts_removal_limit` parts during one iteration.
    `simultaneous_parts_removal_limit` set to `0` means unlimited.
    )", 0) \
    DECLARE(UInt64, reduce_blocking_parts_sleep_ms, 5000, R"(
    Only available in ClickHouse Cloud. Minimum time to wait before trying to
    reduce blocking parts again after no ranges were dropped/replaced. A lower
    setting will trigger tasks in background_schedule_pool frequently which
    results in large amount of requests to zookeeper in large-scale clusters
    )", 0) \
    \
    /** Replication settings. */ \
    DECLARE(UInt64, replicated_deduplication_window, 1000, R"(
    The number of most recently inserted blocks for which ClickHouse Keeper stores
    hash sums to check for duplicates.

    Possible values:
    - Any positive integer.
    - 0 (disable deduplication)

    The `Insert` command creates one or more blocks (parts). For
    [insert deduplication](../../engines/table-engines/mergetree-family/replication.md),
    when writing into replicated tables, ClickHouse writes the hash sums of the
    created parts into ClickHouse Keeper. Hash sums are stored only for the most
    recent `replicated_deduplication_window` blocks. The oldest hash sums are
    removed from ClickHouse Keeper.

    A large number for `replicated_deduplication_window` slows down `Inserts`
    because more entries need to be compared. The hash sum is calculated from
    the composition of the field names and types and the data of the inserted
    part (stream of bytes).
    )", 0) \
    DECLARE(UInt64, replicated_deduplication_window_seconds, 7 * 24 * 60 * 60 /* one week */, R"(
    The number of seconds after which the hash sums of the inserted blocks are
    removed from ClickHouse Keeper.

    Possible values:
    - Any positive integer.

    Similar to [replicated_deduplication_window](#replicated_deduplication_window),
    `replicated_deduplication_window_seconds` specifies how long to store hash
    sums of blocks for insert deduplication. Hash sums older than
    `replicated_deduplication_window_seconds` are removed from ClickHouse Keeper,
    even if they are less than ` replicated_deduplication_window`.

    The time is relative to the time of the most recent record, not to the wall
    time. If it's the only record it will be stored forever.
    )", 0) \
    DECLARE(UInt64, replicated_deduplication_window_for_async_inserts, 10000, R"(
    The number of most recently async inserted blocks for which ClickHouse Keeper
    stores hash sums to check for duplicates.

    Possible values:
    - Any positive integer.
    - 0 (disable deduplication for async_inserts)

    The [Async Insert](/operations/settings/settings#async_insert) command will
    be cached in one or more blocks (parts). For [insert deduplication](/engines/table-engines/mergetree-family/replication),
    when writing into replicated tables, ClickHouse writes the hash sums of each
    insert into ClickHouse Keeper. Hash sums are stored only for the most recent
    `replicated_deduplication_window_for_async_inserts` blocks. The oldest hash
    sums are removed from ClickHouse Keeper.
    A large number of `replicated_deduplication_window_for_async_inserts` slows
    down `Async Inserts` because it needs to compare more entries.
    The hash sum is calculated from the composition of the field names and types
    and the data of the insert (stream of bytes).
    )", 0) \
    DECLARE(UInt64, replicated_deduplication_window_seconds_for_async_inserts, 7 * 24 * 60 * 60 /* one week */, R"(
    The number of seconds after which the hash sums of the async inserts are
    removed from ClickHouse Keeper.

    Possible values:
    - Any positive integer.

    Similar to [replicated_deduplication_window_for_async_inserts](#replicated_deduplication_window_for_async_inserts),
    `replicated_deduplication_window_seconds_for_async_inserts` specifies how
    long to store hash sums of blocks for async insert deduplication. Hash sums
    older than `replicated_deduplication_window_seconds_for_async_inserts` are
    removed from ClickHouse Keeper, even if they are less than
    `replicated_deduplication_window_for_async_inserts`.

    The time is relative to the time of the most recent record, not to the wall
    time. If it's the only record it will be stored forever.
    )", 0) \
    DECLARE(Milliseconds, async_block_ids_cache_update_wait_ms, 100, R"(
    How long each insert iteration will wait for async_block_ids_cache update
    )", 0) \
    DECLARE(Bool, use_async_block_ids_cache, true, R"(
    If true, we cache the hash sums of the async inserts.

    Possible values:
    - `true`
    - `false`

    A block bearing multiple async inserts will generate multiple hash sums.
    When some of the inserts are duplicated, keeper will only return one
    duplicated hash sum in one RPC, which will cause unnecessary RPC retries.
    This cache will watch the hash sums path in Keeper. If updates are watched
    in the Keeper, the cache will update as soon as possible, so that we are
    able to filter the duplicated inserts in the memory.
    )", 0) \
    DECLARE(UInt64, max_replicated_logs_to_keep, 1000, R"(
    How many records may be in the ClickHouse Keeper log if there is inactive
    replica. An inactive replica becomes lost when when this number exceed.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(UInt64, min_replicated_logs_to_keep, 10, R"(
    Keep about this number of last records in ZooKeeper log, even if they are
    obsolete. It doesn't affect work of tables: used only to diagnose ZooKeeper
    log before cleaning.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(Seconds, prefer_fetch_merged_part_time_threshold, 3600, R"(
    If the time passed since a replication log (ClickHouse Keeper or ZooKeeper)
    entry creation exceeds this threshold, and the sum of the size of parts is
    greater than `prefer_fetch_merged_part_size_threshold`, then prefer fetching
    merged part from a replica instead of doing merge locally. This is to speed
    up very long merges.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(UInt64, prefer_fetch_merged_part_size_threshold, 10ULL * 1024 * 1024 * 1024, R"(
    If the sum of the size of parts exceeds this threshold and the time since a
    replication log entry creation is greater than
    `prefer_fetch_merged_part_time_threshold`, then prefer fetching merged part
    from a replica instead of doing merge locally. This is to speed up very long
    merges.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(Seconds, execute_merges_on_single_replica_time_threshold, 0, R"(
    When this setting has a value greater than zero, only a single replica starts
    the merge immediately, and other replicas wait up to that amount of time to
    download the result instead of doing merges locally. If the chosen replica
    doesn't finish the merge during that amount of time, fallback to standard
    behavior happens.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(Seconds, remote_fs_execute_merges_on_single_replica_time_threshold, 3 * 60 * 60, R"(
    When this setting has a value greater than zero only a single replica starts
    the merge immediately if merged part on shared storage and
    `allow_remote_fs_zero_copy_replication` is enabled.

    :::note
    Zero-copy replication is not ready for production
    Zero-copy replication is disabled by default in ClickHouse version 22.8 and
    higher.

    This feature is not recommended for production use.
    :::

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(Seconds, try_fetch_recompressed_part_timeout, 7200, R"(
    Timeout (in seconds) before starting merge with recompression. During this
    time ClickHouse tries to fetch recompressed part from replica which assigned
    this merge with recompression.

    Recompression works slow in most cases, so we don't start merge with
    recompression until this timeout and trying to fetch recompressed part from
    replica which assigned this merge with recompression.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(Bool, always_fetch_merged_part, false, R"(
    If true, this replica never merges parts and always downloads merged parts
    from other replicas.

    Possible values:
    - true, false
    )", 0) \
    DECLARE(UInt64, number_of_partitions_to_consider_for_merge, 10, R"(
    Only available in ClickHouse Cloud. Up to top N partitions which we will
    consider for merge. Partitions picked in a random weighted way where weight
    is amount of data parts which can be merged in this partition.
    )", 0) \
    DECLARE(UInt64, max_suspicious_broken_parts, 100, R"(
    If the number of broken parts in a single partition exceeds the
    `max_suspicious_broken_parts` value, automatic deletion is denied.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(UInt64, max_suspicious_broken_parts_bytes, 1ULL * 1024 * 1024 * 1024, R"(
    Max size of all broken parts, if more - deny automatic deletion.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_max_suspicious_broken_parts, 0, R"(
    Max broken parts for SMT, if more - deny automatic detach.
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_max_suspicious_broken_parts_bytes, 0, R"(
    Max size of all broken parts for SMT, if more - deny automatic detach.
    )", 0) \
    DECLARE(UInt64, max_files_to_modify_in_alter_columns, 75, R"(
    Do not apply ALTER if number of files for modification(deletion, addition)
    is greater than this setting.

    Possible values:

    - Any positive integer.

    Default value: 75
    )", 0) \
    DECLARE(UInt64, max_files_to_remove_in_alter_columns, 50, R"(
    Do not apply ALTER, if the number of files for deletion is greater than this
    setting.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(Float, replicated_max_ratio_of_wrong_parts, 0.5, R"(
    If the ratio of wrong parts to total number of parts is less than this -
    allow to start.

    Possible values:
    - Float, 0.0 - 1.0
    )", 0) \
    DECLARE(Bool, replicated_can_become_leader, true, R"(
    If true, replicated tables replicas on this node will try to acquire
    leadership.

    Possible values:
    - `true`
    - `false`
    )", 0) \
    DECLARE(Seconds, zookeeper_session_expiration_check_period, 60, R"(
    ZooKeeper session expiration check period, in seconds.

    Possible values:
    - Any positive integer.
    )", 0) \
    DECLARE(Seconds, initialization_retry_period, 60, R"(
    Retry period for table initialization, in seconds.
    )", 0) \
    DECLARE(Bool, detach_old_local_parts_when_cloning_replica, true, R"(
    Do not remove old local parts when repairing lost replica.

    Possible values:
    - `true`
    - `false`
    )", 0) \
    DECLARE(Bool, detach_not_byte_identical_parts, false, R"(
    Enables or disables detaching a data part on a replica after a merge or a
    mutation, if it is not byte-identical to data parts on other replicas. If
    disabled, the data part is removed. Activate this setting if you want to
    analyze such parts later.

    The setting is applicable to `MergeTree` tables with enabled
    [data replication](/engines/table-engines/mergetree-family/replacingmergetree).

    Possible values:

    - `0` — Parts are removed.
    - `1` — Parts are detached.
    )", 0) \
    DECLARE(UInt64, max_replicated_fetches_network_bandwidth, 0, R"(
    Limits the maximum speed of data exchange over the network in bytes per
    second for [replicated](../../engines/table-engines/mergetree-family/replication.md)
    fetches. This setting is applied to a particular table, unlike the
    [`max_replicated_fetches_network_bandwidth_for_server`](/operations/settings/merge-tree-settings#max_replicated_fetches_network_bandwidth)
    setting, which is applied to the server.

    You can limit both server network and network for a particular table, but for
    this the value of the table-level setting should be less than server-level
    one. Otherwise the server considers only the
    `max_replicated_fetches_network_bandwidth_for_server` setting.

    The setting isn't followed perfectly accurately.

    Possible values:

    - Positive integer.
    - `0` — Unlimited.

    Default value: `0`.

    **Usage**

    Could be used for throttling speed when replicating data to add or replace
    new nodes.
    )", 0) \
    DECLARE(UInt64, max_replicated_sends_network_bandwidth, 0, R"(
    Limits the maximum speed of data exchange over the network in bytes per
    second for [replicated](/engines/table-engines/mergetree-family/replacingmergetree)
    sends. This setting is applied to a particular table, unlike the
    [`max_replicated_sends_network_bandwidth_for_server`](/operations/settings/merge-tree-settings#max_replicated_sends_network_bandwidth)
    setting, which is applied to the server.

    You can limit both server network and network for a particular table, but
    for this the value of the table-level setting should be less than
    server-level one. Otherwise the server considers only the
    `max_replicated_sends_network_bandwidth_for_server` setting.

    The setting isn't followed perfectly accurately.

    Possible values:

    - Positive integer.
    - `0` — Unlimited.

    **Usage**

    Could be used for throttling speed when replicating data to add or replace
    new nodes.
    )", 0) \
    DECLARE(Milliseconds, wait_for_unique_parts_send_before_shutdown_ms, 0, R"(
    Before shutdown table will wait for required amount time for unique parts
    (exist only on current replica) to be fetched by other replicas (0 means
    disabled).
    )", 0) \
    DECLARE(Float, fault_probability_before_part_commit, 0, R"(
    For testing. Do not change it.
    )", 0) \
    DECLARE(Float, fault_probability_after_part_commit, 0, R"(
    For testing. Do not change it.
    )", 0) \
    DECLARE(Bool, shared_merge_tree_disable_merges_and_mutations_assignment, false, R"(
    Stop merges assignment for shared merge tree. Only available in ClickHouse
    Cloud
    )", 0) \
    DECLARE(Bool, shared_merge_tree_enable_outdated_parts_check, true, R"(
    Enable outdated parts check. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(Float, shared_merge_tree_partitions_hint_ratio_to_reload_merge_pred_for_mutations, 0.5, R"(
    Will reload merge predicate in merge/mutate selecting task when `<candidate
    partitions for mutations only (partitions that cannot be merged)>/<candidate
    partitions for mutations>` ratio is higher than the setting. Only available
    in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_parts_load_batch_size, 32, R"(
    Amount of fetch parts metadata jobs to schedule at once. Only available in
    ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_max_parts_update_leaders_in_total, 6, R"(
    Maximum number of parts update leaders. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_max_parts_update_leaders_per_az, 2, R"(
    Maximum number of parts update leaders. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_leader_update_period_seconds, 30, R"(
    Maximum period to recheck leadership for parts update. Only available in
    ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_leader_update_period_random_add_seconds, 10, R"(
    Add uniformly distributed value from 0 to x seconds to
    shared_merge_tree_leader_update_period to avoid thundering
    herd effect. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(Bool, shared_merge_tree_read_virtual_parts_from_leader, true, R"(
    Read virtual parts from leader when possible. Only available in ClickHouse
    Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_initial_parts_update_backoff_ms, 50, R"(
    Initial backoff for parts update. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_max_parts_update_backoff_ms, 5000, R"(
    Max backoff for parts update. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_interserver_http_connection_timeout_ms, 100, R"(
    Timeouts for interserver HTTP connection. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_interserver_http_timeout_ms, 10000, R"(
    Timeouts for interserver HTTP communication. Only available in ClickHouse
    Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_max_replicas_for_parts_deletion, 10, R"(
    Max replicas which will participate in parts deletion (killer thread). Only
    available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_max_replicas_to_merge_parts_for_each_parts_range, 5, R"(
    Max replicas which will try to assign potentially conflicting merges (allow
    to avoid redundant conflicts in merges assignment). 0 means disabled. Only
    available in ClickHouse Cloud
    )", 0) \
    DECLARE(Bool, shared_merge_tree_use_outdated_parts_compact_format, false, R"(
    Use compact format for outdated parts: reduces load to Keeper, improves
    outdated parts processing. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(Int64, shared_merge_tree_memo_ids_remove_timeout_seconds, 1800, R"(
    How long we store insert memoization ids to avoid wrong actions during
    insert retries. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_idle_parts_update_seconds, 3600, R"(
    Interval in seconds for parts update without being triggered by ZooKeeper
    watch in the shared merge tree. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_max_outdated_parts_to_process_at_once, 1000, R"(
    Maximum amount of outdated parts leader will try to confirm for removal at
    one HTTP request. Only available in ClickHouse Cloud.
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_postpone_next_merge_for_locally_merged_parts_rows_threshold, 1000000, R"(
    Minimum size of part (in rows) to postpone assigning a next merge just after
    merging it locally. Only available in ClickHouse Cloud.
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_postpone_next_merge_for_locally_merged_parts_ms, 0, R"(
    Time to keep a locally merged part without starting a new merge containing
    this part. Gives other replicas a chance fetch the part and start this merge.
    Only available in ClickHouse Cloud.
    )", 0) \
    DECLARE(UInt64, shared_merge_tree_range_for_merge_window_size, 10, R"(
    Time to keep a locally merged part without starting a new merge containing
    this part. Gives other replicas a chance fetch the part and start this merge.
    Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(Bool, shared_merge_tree_use_too_many_parts_count_from_virtual_parts, 0, R"(
    If enabled too many parts counter will rely on shared data in Keeper, not on
    local replica state. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(Bool, shared_merge_tree_create_per_replica_metadata_nodes, true, R"(
    Enables creation of per-replica /metadata and /columns nodes in ZooKeeper.
    Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(Bool, shared_merge_tree_use_metadata_hints_cache, true, R"(
    Enables requesting FS cache hints from in-memory
    cache on other replicas. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(Bool, shared_merge_tree_try_fetch_part_in_memory_data_from_replicas, false, R"(
    If enabled all the replicas try to fetch part in memory data (like primary
    key, partition info and so on) from other replicas where it already exists.
    )", 0) \
    DECLARE(Bool, allow_reduce_blocking_parts_task, true, R"(
    Background task which reduces blocking parts for shared merge tree tables.
    Only in ClickHouse Cloud
    )", 0) \
    \
    /** Check delay of replicas settings. */ \
    DECLARE(UInt64, min_relative_delay_to_measure, 120, R"(
    Calculate relative replica delay only if absolute delay is not less that
    this value.
    )", 0) \
    DECLARE(UInt64, cleanup_delay_period, 30, R"(
    Minimum period to clean old queue logs, blocks hashes and parts.
    )", 0) \
    DECLARE(UInt64, max_cleanup_delay_period, 300, R"(
    Maximum period to clean old queue logs, blocks hashes and parts.
    )", 0) \
    DECLARE(UInt64, cleanup_delay_period_random_add, 10, R"(
    Add uniformly distributed value from 0 to x seconds to cleanup_delay_period
    to avoid thundering herd effect and subsequent DoS of ZooKeeper in case of
    very large number of tables.
    )", 0) \
    DECLARE(UInt64, cleanup_thread_preferred_points_per_iteration, 150, R"(
    Preferred batch size for background cleanup (points are abstract but 1 point
    is approximately equivalent to 1 inserted block).
    )", 0) \
    DECLARE(UInt64, cleanup_threads, 128, R"(
    Threads for cleanup of outdated threads. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, min_relative_delay_to_close, 300, R"(
    Minimal delay from other replicas to close, stop serving
    requests and not return Ok during status check.
    )", 0) \
    DECLARE(UInt64, min_absolute_delay_to_close, 0, R"(
    Minimal absolute delay to close, stop serving requests and not
    return Ok during status check.
    )", 0) \
    DECLARE(UInt64, enable_vertical_merge_algorithm, 1, R"(
    Enable usage of Vertical merge algorithm.
    )", 0) \
    DECLARE(UInt64, vertical_merge_algorithm_min_rows_to_activate, 16 * 8192, R"(
    Minimal (approximate) sum of rows in
    merging parts to activate Vertical merge algorithm.
    )", 0) \
    DECLARE(UInt64, vertical_merge_algorithm_min_bytes_to_activate, 0, R"(
    Minimal (approximate) uncompressed size in bytes in merging parts to activate
    Vertical merge algorithm.
    )", 0) \
    DECLARE(UInt64, vertical_merge_algorithm_min_columns_to_activate, 11, R"(
    Minimal amount of non-PK columns to activate Vertical merge algorithm.
    )", 0) \
    DECLARE(Bool, vertical_merge_remote_filesystem_prefetch, true, R"(
    If true prefetching of data from remote filesystem is used for the next
    column during merge
    )", 0) \
    DECLARE(UInt64, max_postpone_time_for_failed_mutations_ms, 5ULL * 60 * 1000, R"(
    The maximum postpone time for failed mutations.
    )", 0) \
    \
    /** Compatibility settings */ \
    DECLARE(Bool, allow_suspicious_indices, false, R"(
    Reject primary/secondary indexes and sorting keys with identical expressions
    )", 0) \
    DECLARE(Bool, compatibility_allow_sampling_expression_not_in_primary_key, false, R"(
    Allow to create a table with sampling expression not in primary key. This is
    needed only to temporarily allow to run the server with wrong tables for
    backward compatibility.
    )", 0) \
    DECLARE(Bool, use_minimalistic_checksums_in_zookeeper, true, R"(
    Use small format (dozens bytes) for part checksums in ZooKeeper instead of
    ordinary ones (dozens KB). Before enabling check that all replicas support
    new format.
    )", 0) \
    DECLARE(Bool, use_minimalistic_part_header_in_zookeeper, true, R"(
    Storage method of the data parts headers in ZooKeeper. If enabled, ZooKeeper
    stores less data. For details, see [here](/operations/server-configuration-parameters/settings#use_minimalistic_part_header_in_zookeeper).
    )", 0) \
    DECLARE(UInt64, finished_mutations_to_keep, 100, R"(
    How many records about mutations that are done to keep. If zero, then keep
    all of them.
    )", 0) \
    DECLARE(UInt64, min_merge_bytes_to_use_direct_io, 10ULL * 1024 * 1024 * 1024, R"(
    The minimum data volume for merge operation that is required for using direct
    I/O access to the storage disk. When merging data parts, ClickHouse calculates
    the total storage volume of all the data to be merged. If the volume exceeds
    `min_merge_bytes_to_use_direct_io` bytes, ClickHouse reads and writes the
    data to the storage disk using the direct I/O interface (`O_DIRECT` option).
    If `min_merge_bytes_to_use_direct_io = 0`, then direct I/O is disabled.
    )", 0) \
    DECLARE(UInt64, index_granularity_bytes, 10 * 1024 * 1024, R"(
    Maximum size of data granules in bytes.

    To restrict the granule size only by number of rows, set to `0` (not recommended).
    )", 0) \
    DECLARE(UInt64, min_index_granularity_bytes, 1024, R"(
    Min allowed size of data granules in bytes.

    To provide a safeguard against accidentally creating tables with very low
    `index_granularity_bytes`.
    )", 1024) \
    DECLARE(Bool, use_const_adaptive_granularity, false, R"(
    Always use constant granularity for whole part. It allows to compress in
    memory values of index granularity. It can be useful in extremely large
    workloads with thin tables.
    )", 0) \
    DECLARE(Bool, enable_index_granularity_compression, true, R"(
    Compress in memory values of index granularity if it is possible
    )", 0) \
    DECLARE(Int64, merge_with_ttl_timeout, 3600 * 4, R"(
    Minimum delay in seconds before repeating a merge with delete TTL.
    )", 0) \
    DECLARE(Int64, merge_with_recompression_ttl_timeout, 3600 * 4, R"(
    Minimum delay in seconds before repeating a merge with recompression TTL.
    )", 0) \
    DECLARE(Bool, ttl_only_drop_parts, false, R"(
    Controls whether data parts are fully dropped in MergeTree tables when all
    rows in that part have expired according to their `TTL` settings.

    When `ttl_only_drop_parts` is disabled (by default), only the rows that have
    expired based on their TTL settings are removed.

    When `ttl_only_drop_parts` is enabled, the entire part is dropped if all
    rows in that part have expired according to their `TTL` settings.
    )", 0) \
    DECLARE(Bool, materialize_ttl_recalculate_only, false, R"(
    Only recalculate ttl info when MATERIALIZE TTL
    )", 0) \
    DECLARE(Bool, enable_mixed_granularity_parts, true, R"(
    Enables or disables transitioning to control the granule size with the
    `index_granularity_bytes` setting. Before version 19.11, there was only the
    `index_granularity` setting for restricting granule size. The
    `index_granularity_bytes` setting improves ClickHouse performance when
    selecting data from tables with big rows (tens and hundreds of megabytes).
    If you have tables with big rows, you can enable this setting for the tables
    to improve the efficiency of `SELECT` queries.
    )", 0) \
    DECLARE(UInt64, concurrent_part_removal_threshold, 100, R"(
    Activate concurrent part removal (see 'max_part_removal_threads') only if
    the number of inactive data parts is at least this.
    )", 0) \
    DECLARE(UInt64, zero_copy_concurrent_part_removal_max_split_times, 5, R"(
    Max recursion depth for splitting independent Outdated parts ranges into
    smaller subranges. Recommended not to change.
    )", 0) \
    DECLARE(Float, zero_copy_concurrent_part_removal_max_postpone_ratio, static_cast<Float32>(0.05), R"(
    Max percentage of top level parts to postpone removal in order to get
    smaller independent ranges. Recommended not to change.
    )", 0) \
    DECLARE(String, storage_policy, "default", R"(
    Name of storage disk policy
    )", 0) \
    DECLARE(String, disk, "", R"(
    Name of storage disk. Can be specified instead of storage policy.
    )", 0) \
    DECLARE(Bool, table_disk, false, R"(
    This is table disk, the path/endpoint should point to the table data, not to
    the database data. Can be set only for s3_plain/s3_plain_rewritable/web.
    )", 0) \
    DECLARE(Bool, allow_nullable_key, false, R"(
    Allow Nullable types as primary keys.
    )", 0) \
    DECLARE(Bool, remove_empty_parts, true, R"(
    Remove empty parts after they were pruned by TTL, mutation, or collapsing
    merge algorithm.
    )", 0) \
    DECLARE(Bool, assign_part_uuids, false, R"(
    When enabled, a unique part identifier will be assigned for every new part.
    Before enabling, check that all replicas support UUID version 4.
    )", 0) \
    DECLARE(Int64, max_partitions_to_read, -1, R"(
    Limits the maximum number of partitions that can be accessed in one query.

    The setting value specified when the table is created can be overridden via
    query-level setting.

    Possible values:
    - Any positive integer.

    You can also specify a query complexity setting [max_partitions_to_read](query-complexity#max_partitions_to_read)
    at a query / session / profile level.
    )", 0) \
    DECLARE(UInt64, max_concurrent_queries, 0, R"(
    Max number of concurrently executed queries related to the MergeTree table.
    Queries will still be limited by other `max_concurrent_queries` settings.

    Possible values:
    - Positive integer.
    - `0` — No limit.

    Default value: `0` (no limit).

    **Example**

    ```xml
    <max_concurrent_queries>50</max_concurrent_queries>
    ```
    )", 0) \
    DECLARE(UInt64, min_marks_to_honor_max_concurrent_queries, 0, R"(
    The minimal number of marks read by the query for applying the [max_concurrent_queries](#max_concurrent_queries)
    setting.

    :::note
    Queries will still be limited by other `max_concurrent_queries` settings.
    :::

    Possible values:
    - Positive integer.
    - `0` — Disabled (`max_concurrent_queries` limit applied to no queries).

    **Example**

    ```xml
    <min_marks_to_honor_max_concurrent_queries>10</min_marks_to_honor_max_concurrent_queries>
    ```
    )", 0) \
    DECLARE(UInt64, min_bytes_to_rebalance_partition_over_jbod, 0, R"(
    Sets minimal amount of bytes to enable balancing when distributing new big
    parts over volume disks [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures).

    Possible values:

    - Positive integer.
    - `0` — Balancing is disabled.

    **Usage**

    The value of the `min_bytes_to_rebalance_partition_over_jbod` setting should
    not be less than the value of the
    [max_bytes_to_merge_at_max_space_in_pool](/operations/settings/merge-tree-settings#max_bytes_to_merge_at_max_space_in_pool)
    / 1024. Otherwise, ClickHouse throws an exception.
    )", 0) \
    DECLARE(Bool, check_sample_column_is_correct, true, R"(
    Enables the check at table creation, that the data type of a column for s
    ampling or sampling expression is correct. The data type must be one of unsigned
    [integer types](/sql-reference/data-types/int-uint): `UInt8`, `UInt16`,
    `UInt32`, `UInt64`.

    Possible values:
    - `true`  — The check is enabled.
    - `false` — The check is disabled at table creation.

    Default value: `true`.

    By default, the ClickHouse server checks at table creation the data type of
    a column for sampling or sampling expression. If you already have tables with
    incorrect sampling expression and do not want the server to raise an exception
    during startup, set `check_sample_column_is_correct` to `false`.
    )", 0) \
    DECLARE(Bool, allow_vertical_merges_from_compact_to_wide_parts, true, R"(
    Allows vertical merges from compact to wide parts. This settings must have
    the same value on all replicas.
    )", 0) \
    DECLARE(Bool, enable_the_endpoint_id_with_zookeeper_name_prefix, false, R"(
    Enable the endpoint id with zookeeper name prefix for the replicated merge
    tree table.
    )", 0) \
    DECLARE(UInt64, zero_copy_merge_mutation_min_parts_size_sleep_no_scale_before_lock, 0, R"(
    If zero copy replication is enabled sleep random amount of time up to 500ms
    before trying to lock for merge or mutation.
    )", 0) \
    DECLARE(UInt64, zero_copy_merge_mutation_min_parts_size_sleep_before_lock, 1ULL * 1024 * 1024 * 1024, R"(
    If zero copy replication is enabled sleep random amount of time before trying
    to lock depending on parts size for merge or mutation
    )", 0) \
    DECLARE(Bool, allow_floating_point_partition_key, false, R"(
    Enables to allow floating-point number as a partition key.

    Possible values:
    - `0` — Floating-point partition key not allowed.
    - `1` — Floating-point partition key allowed.
    )", 0) \
    DECLARE(UInt64, sleep_before_loading_outdated_parts_ms, 0, R"(
    For testing. Do not change it.
    )", 0) \
    DECLARE(Bool, always_use_copy_instead_of_hardlinks, false, R"(
    Always copy data instead of hardlinking during mutations/replaces/detaches
    and so on.
    )", 0) \
    DECLARE(Bool, disable_freeze_partition_for_zero_copy_replication, true, R"(
    Disable FREEZE PARTITION query for zero copy replication.
    )", 0) \
    DECLARE(Bool, disable_detach_partition_for_zero_copy_replication, true, R"(
    Disable DETACH PARTITION query for zero copy replication.
    )", 0) \
    DECLARE(Bool, disable_fetch_partition_for_zero_copy_replication, true, R"(
    Disable FETCH PARTITION query for zero copy replication.
    )", 0) \
    DECLARE(Bool, enable_block_number_column, false, R"(
    Enable persisting column _block_number for each row.
    )", 0) ALIAS(allow_experimental_block_number_column) \
    DECLARE(Bool, enable_block_offset_column, false, R"(
    Persists virtual column `_block_number` on merges.
    )", 0) \
    DECLARE(Bool, add_minmax_index_for_numeric_columns, false, R"(
    When enabled, min-max (skipping) indices are added for all numeric columns
    of the table.
    )", 0) \
    DECLARE(Bool, add_minmax_index_for_string_columns, false, R"(
    When enabled, min-max (skipping) indices are added for all string columns of
    the table.
    )", 0) \
    \
    /** Experimental/work in progress feature. Unsafe for production. */ \
    DECLARE(UInt64, part_moves_between_shards_enable, 0, R"(
    Experimental/Incomplete feature to move parts between shards. Does not take
    into account sharding expressions.
    )", EXPERIMENTAL) \
    DECLARE(UInt64, part_moves_between_shards_delay_seconds, 30, R"(
    Time to wait before/after moving parts between shards.
    )", EXPERIMENTAL) \
    DECLARE(Bool, allow_remote_fs_zero_copy_replication, default_allow_remote_fs_zero_copy_replication, R"(
    Don't use this setting in production, because it is not ready.
    )", BETA) \
    DECLARE(String, remote_fs_zero_copy_zookeeper_path, "/clickhouse/zero_copy", R"(
    ZooKeeper path for zero-copy table-independent info.
    )", EXPERIMENTAL) \
    DECLARE(Bool, remote_fs_zero_copy_path_compatible_mode, false, R"(
    Run zero-copy in compatible mode during conversion process.
    )", EXPERIMENTAL) \
    DECLARE(Bool, force_read_through_cache_for_merges, false, R"(
    Force read-through filesystem cache for merges
    )", EXPERIMENTAL) \
    DECLARE(Bool, cache_populated_by_fetch, false, R"(
    :::note
    This setting applies only to ClickHouse Cloud.
    :::

    When `cache_populated_by_fetch` is disabled (the default setting), new data
    parts are loaded into the cache only when a query is run that requires those
    parts.

    If enabled, `cache_populated_by_fetch` will instead cause all nodes to load
    new data parts from storage into their cache without requiring a query to
    trigger such an action.

    **See Also**

    - [ignore_cold_parts_seconds](/operations/settings/settings#ignore_cold_parts_seconds)
    - [prefer_warmed_unmerged_parts_seconds](/operations/settings/settings#prefer_warmed_unmerged_parts_seconds)
    - [cache_warmer_threads](/operations/settings/settings#cache_warmer_threads)
    )", 0) \
    DECLARE(Bool, allow_experimental_replacing_merge_with_cleanup, false, R"(
    Allow experimental CLEANUP merges for ReplacingMergeTree with `is_deleted`
    column. When enabled, allows using `OPTIMIZE ... FINAL CLEANUP` to manually
    merge all parts in a partition down to a single part and removing any
    deleted rows.

    Also allows enabling such merges to happen automatically in the background
    with settings `min_age_to_force_merge_seconds`,
    `min_age_to_force_merge_on_partition_only` and
    `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge`.
    )", EXPERIMENTAL) \
    DECLARE(Bool, enable_replacing_merge_with_cleanup_for_min_age_to_force_merge, false, R"(
    Whether to use CLEANUP merges for ReplacingMergeTree when merging partitions
    down to a single part. Requires `allow_experimental_replacing_merge_with_cleanup`,
    `min_age_to_force_merge_seconds` and `min_age_to_force_merge_on_partition_only`
    to be enabled.

    Possible values:
    - `true`
    - `false`
    )", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_reverse_key, false, R"(
    Enables support for descending sort order in MergeTree sorting keys. This
    setting is particularly useful for time series analysis and Top-N queries,
    allowing data to be stored in reverse chronological order to optimize query
    performance.

    With `allow_experimental_reverse_key` enabled, you can define descending sort
    orders within the `ORDER BY` clause of a MergeTree table. This enables the
    use of more efficient `ReadInOrder` optimizations instead of `ReadInReverseOrder`
    for descending queries.

    **Example**

    ```sql
    CREATE TABLE example
    (
    time DateTime,
    key Int32,
    value String
    ) ENGINE = MergeTree
    ORDER BY (time DESC, key)  -- Descending order on 'time' field
    SETTINGS allow_experimental_reverse_key = 1;

    SELECT * FROM example WHERE key = 'xxx' ORDER BY time DESC LIMIT 10;
    ```

    By using `ORDER BY time DESC` in the query, `ReadInOrder` is applied.

    **Default Value:** false
    )", EXPERIMENTAL) \
    DECLARE(Bool, notify_newest_block_number, false, R"(
    Notify newest block number to SharedJoin or SharedSet. Only in ClickHouse Cloud.
    )", EXPERIMENTAL) \
    DECLARE(Bool, shared_merge_tree_enable_keeper_parts_extra_data, false, R"(
    Enables writing attributes into virtual parts and committing blocks in keeper
    )", EXPERIMENTAL) \
    \
    /** Compress marks and primary key. */ \
    DECLARE(Bool, compress_marks, true, R"(
    Marks support compression, reduce mark file size and speed up network
    transmission.
    )", 0) \
    DECLARE(Bool, compress_primary_key, true, R"(
    Primary key support compression, reduce primary key file size and speed up
    network transmission.
    )", 0) \
    DECLARE(String, marks_compression_codec, "ZSTD(3)", R"(
    Compression encoding used by marks, marks are small enough and cached, so
    the default compression is ZSTD(3).
    )", 0) \
    DECLARE(String, primary_key_compression_codec, "ZSTD(3)", R"(
    Compression encoding used by primary, primary key is small enough and cached,
    so the default compression is ZSTD(3).
    )", 0) \
    DECLARE(UInt64, marks_compress_block_size, 65536, R"(
    Mark compress block size, the actual size of the block to compress.
    )", 0) \
    DECLARE(UInt64, primary_key_compress_block_size, 65536, R"(
    Primary compress block size, the actual size of the block to compress.
    )", 0) \
    DECLARE(Bool, primary_key_lazy_load, true, R"(Load primary key in memory on
    first use instead of on table initialization. This can save memory in the
    presence of a large number of tables.
    )", 0) \
    DECLARE(Float, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns, 0.9f, R"(
    If the value of a column of the primary key in data part changes at least in
    this ratio of times, skip loading next columns in memory. This allows to save
    memory usage by not loading useless columns of the primary key.
    )", 0) \
    DECLARE(Bool, use_primary_key_cache, false, R"(Use cache for primary index
    instead of saving all indexes in memory. Can be useful for very large tables
    )", 0) \
    DECLARE(Bool, prewarm_primary_key_cache, false, R"(If true primary index
    cache will be prewarmed by saving marks to mark cache on inserts, merges,
    fetches and on startup of server
    )", 0) \
    DECLARE(Bool, prewarm_mark_cache, false, R"(If true mark cache will be
    prewarmed by saving marks to mark cache on inserts, merges, fetches and on
    startup of server
    )", 0) \
    DECLARE(String, columns_to_prewarm_mark_cache, "", R"(
    List of columns to prewarm mark cache for (if enabled). Empty means all columns
    )", 0) \
    DECLARE(UInt64, min_bytes_to_prewarm_caches, 0, R"(
    Minimal size (uncompressed bytes) to prewarm mark cache and primary index cache
    for new parts
    )", 0) \
    /** Projection settings. */ \
    DECLARE(UInt64, max_projections, 25, R"(
    The maximum number of merge tree projections.
    )", 0) \
    DECLARE(LightweightMutationProjectionMode, lightweight_mutation_projection_mode, LightweightMutationProjectionMode::THROW, R"(
    By default, lightweight delete `DELETE` does not work for tables with
    projections. This is because rows in a projection may be affected by a
    `DELETE` operation. So the default value would be `throw`. However, this
    option can change the behavior. With the value either `drop` or `rebuild`,
    deletes will work with projections. `drop` would delete the projection so it
    might be fast in the current query as projection gets deleted but slow in
    future queries as no projection attached. `rebuild` would rebuild the
    projection which might affect the performance of the current query, but
    might speedup for future queries. A good thing is that these options would
    only work in the part level, which means projections in the part that don't
    get touched would stay intact instead of triggering any action like
    drop or rebuild.

    Possible values:
    - `throw`
    - `drop`
    - `rebuild`
    )", 0) \
    DECLARE(DeduplicateMergeProjectionMode, deduplicate_merge_projection_mode, DeduplicateMergeProjectionMode::THROW, R"(
    Whether to allow create projection for the table with non-classic MergeTree,
    that is not (Replicated, Shared) MergeTree. Ignore option is purely for
    compatibility which might result in incorrect answer. Otherwise, if allowed,
    what is the action when merge projections, either drop or rebuild. So classic
    MergeTree would ignore this setting. It also controls `OPTIMIZE DEDUPLICATE`
    as well, but has effect on all MergeTree family members. Similar to the
    option `lightweight_mutation_projection_mode`, it is also part level.

    Possible values:
    - `ignore`
    - `throw`
    - `drop`
    - `rebuild`
    )", 0) \
    /** Part loading settings. */           \
    DECLARE(Bool, columns_and_secondary_indices_sizes_lazy_calculation, true, R"(
    Calculate columns and secondary indices sizes lazily on first request instead
    of on table initialization.
    )", 0) \

#define MAKE_OBSOLETE_MERGE_TREE_SETTING(M, TYPE, NAME, DEFAULT) \
    M(TYPE, NAME, DEFAULT, "Obsolete setting, does nothing.", SettingsTierType::OBSOLETE)

#define OBSOLETE_MERGE_TREE_SETTINGS(M, ALIAS) \
    /** Obsolete settings that do nothing but left for compatibility reasons. */ \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, min_relative_delay_to_yield_leadership, 120) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, check_delay_period, 60) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, replicated_max_parallel_sends, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, replicated_max_parallel_sends_for_table, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, replicated_max_parallel_fetches, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, replicated_max_parallel_fetches_for_table, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Bool, write_final_mark, true) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, min_bytes_for_compact_part, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, min_rows_for_compact_part, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Bool, in_memory_parts_enable_wal, true) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, write_ahead_log_max_bytes, 1024 * 1024 * 1024) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, write_ahead_log_bytes_to_fsync, 100ULL * 1024 * 1024) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, write_ahead_log_interval_ms_to_fsync, 100) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Bool, in_memory_parts_insert_sync, false) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, MaxThreads, max_part_loading_threads, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, MaxThreads, max_part_removal_threads, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Bool, use_metadata_cache, false) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, merge_tree_enable_clear_old_broken_detached, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds, 1ULL * 3600 * 24 * 30) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Seconds, replicated_fetches_http_connection_timeout, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Seconds, replicated_fetches_http_send_timeout, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, Seconds, replicated_fetches_http_receive_timeout, 0) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, replicated_max_parallel_fetches_for_host, DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, CleanDeletedRows, clean_deleted_rows, CleanDeletedRows::Never) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, kill_delay_period, 30) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, kill_delay_period_random_add, 10) \
    MAKE_OBSOLETE_MERGE_TREE_SETTING(M, UInt64, kill_threads, 128) \

    /// Settings that should not change after the creation of a table.
    /// NOLINTNEXTLINE
#define APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(MACRO) \
    MACRO(index_granularity)                           \

#define LIST_OF_MERGE_TREE_SETTINGS(M, ALIAS) \
    MERGE_TREE_SETTINGS(M, ALIAS)             \
    OBSOLETE_MERGE_TREE_SETTINGS(M, ALIAS)

// clang-format on

DECLARE_SETTINGS_TRAITS(MergeTreeSettingsTraits, LIST_OF_MERGE_TREE_SETTINGS)

/** Settings for the MergeTree family of engines.
  * Could be loaded from config or from a CREATE TABLE query (SETTINGS clause).
  */
struct MergeTreeSettingsImpl : public BaseSettings<MergeTreeSettingsTraits>
{
    /// NOTE: will rewrite the AST to add immutable settings.
    void loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach);

    /// Check that the values are sane taking also query-level settings into account.
    void sanityCheck(size_t background_pool_tasks, bool allow_experimental, bool allow_beta) const;
};

static void validateTableDisk(const DiskPtr & disk)
{
    if (!disk)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MergeTree settings `table_disk` requires `disk` setting.");
    const auto * disk_object_storage = dynamic_cast<const DiskObjectStorage *>(disk.get());
    if (!disk_object_storage)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MergeTree settings `table_disk` is not supported for non-ObjectStorage disks");
    if (!(disk_object_storage->isReadOnly() || disk_object_storage->isPlain()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MergeTree settings `table_disk` is not supported for {}", disk_object_storage->getStructure());
}

IMPLEMENT_SETTINGS_TRAITS(MergeTreeSettingsTraits, LIST_OF_MERGE_TREE_SETTINGS)

void MergeTreeSettingsImpl::loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach)
{
    if (storage_def.settings)
    {
        try
        {
            bool found_disk_setting = false;
            bool found_storage_policy_setting = false;
            bool table_disk = false;
            DiskPtr disk;

            auto changes = storage_def.settings->changes;
            for (auto & [name, value] : changes)
            {
                CustomType custom;
                if (name == "disk")
                {
                    ASTPtr value_as_custom_ast = nullptr;
                    if (value.tryGet<CustomType>(custom) && 0 == strcmp(custom.getTypeName(), "AST"))
                        value_as_custom_ast = dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast;

                    if (value_as_custom_ast && isDiskFunction(value_as_custom_ast))
                    {
                        auto disk_name = DiskFromAST::createCustomDisk(value_as_custom_ast, context, is_attach);
                        LOG_DEBUG(getLogger("MergeTreeSettings"), "Created custom disk {}", disk_name);
                        value = disk_name;
                    }
                    else
                    {
                        DiskFromAST::ensureDiskIsNotCustom(value.safeGet<String>(), context);
                    }
                    disk = context->getDisk(value.safeGet<String>());

                    if (has("storage_policy"))
                        resetToDefault("storage_policy");

                    found_disk_setting = true;
                }
                else if (name == "storage_policy")
                    found_storage_policy_setting = true;
                else if (name == "table_disk")
                    table_disk = value.safeGet<bool>();

                if (!is_attach && found_disk_setting && found_storage_policy_setting)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "MergeTree settings `storage_policy` and `disk` cannot be specified at the same time");
                }

            }

            if (table_disk)
                validateTableDisk(disk);

            applyChanges(changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    SettingsChanges & changes = storage_def.settings->changes;

#define ADD_IF_ABSENT(NAME)                                                                                   \
    if (std::find_if(changes.begin(), changes.end(),                                                          \
                  [](const SettingChange & c) { return c.name == #NAME; })                                    \
            == changes.end())                                                                                 \
        changes.push_back(SettingChange{#NAME, (NAME).value});

    APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(ADD_IF_ABSENT)
#undef ADD_IF_ABSENT
}

void MergeTreeSettingsImpl::sanityCheck(size_t background_pool_tasks, bool allow_experimental, bool allow_beta) const
{
    if (!allow_experimental || !allow_beta)
    {
        for (const auto & setting : all())
        {
            if (!setting.isValueChanged())
                continue;

            auto tier = setting.getTier();
            if (!allow_experimental && tier == EXPERIMENTAL)
            {
                throw Exception(
                    ErrorCodes::READONLY,
                    "Cannot modify setting '{}'. Changes to EXPERIMENTAL settings are disabled in the server config "
                    "('allow_feature_tier')",
                    setting.getName());
            }
            if (!allow_beta && tier == BETA)
            {
                throw Exception(
                    ErrorCodes::READONLY,
                    "Cannot modify setting '{}'. Changes to BETA settings are disabled in the server config ('allow_feature_tier')",
                    setting.getName());
            }
        }
    }


    if (number_of_free_entries_in_pool_to_execute_mutation > background_pool_tasks)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_execute_mutation' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'*'background_merges_mutations_concurrency_ratio'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because mutations cannot work with these settings.",
            number_of_free_entries_in_pool_to_execute_mutation.value,
            background_pool_tasks);
    }

    if (number_of_free_entries_in_pool_to_lower_max_size_of_merge > background_pool_tasks)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_lower_max_size_of_merge' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'*'background_merges_mutations_concurrency_ratio'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because the maximum size of merge will be always lowered.",
            number_of_free_entries_in_pool_to_lower_max_size_of_merge.value,
            background_pool_tasks);
    }

    if (number_of_free_entries_in_pool_to_execute_optimize_entire_partition > background_pool_tasks)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_execute_optimize_entire_partition' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'*'background_merges_mutations_concurrency_ratio'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because the maximum size of merge will be always lowered.",
            number_of_free_entries_in_pool_to_execute_optimize_entire_partition.value,
            background_pool_tasks);
    }

    // Zero index_granularity is nonsensical.
    if (index_granularity < 1)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "index_granularity: value {} makes no sense",
            index_granularity.value);
    }

    // The min_index_granularity_bytes value is 1024 b and index_granularity_bytes is 10 mb by default.
    // If index_granularity_bytes is not disabled i.e > 0 b, then always ensure that it's greater than
    // min_index_granularity_bytes. This is mainly a safeguard against accidents whereby a really low
    // index_granularity_bytes SETTING of 1b can create really large parts with large marks.
    if (index_granularity_bytes > 0 && index_granularity_bytes < min_index_granularity_bytes)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "index_granularity_bytes: {} is lower than specified min_index_granularity_bytes: {}",
            index_granularity_bytes.value,
            min_index_granularity_bytes.value);
    }

    // If min_bytes_to_rebalance_partition_over_jbod is not disabled i.e > 0 b, then always ensure that
    // it's not less than min_bytes_to_rebalance_partition_over_jbod. This is a safeguard to avoid tiny
    // parts to participate JBOD balancer which will slow down the merge process.
    if (min_bytes_to_rebalance_partition_over_jbod > 0
        && min_bytes_to_rebalance_partition_over_jbod < max_bytes_to_merge_at_max_space_in_pool / 1024)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "min_bytes_to_rebalance_partition_over_jbod: {} is lower than specified max_bytes_to_merge_at_max_space_in_pool / 1024: {}",
            min_bytes_to_rebalance_partition_over_jbod.value,
            max_bytes_to_merge_at_max_space_in_pool / 1024);
    }

    if (max_cleanup_delay_period < cleanup_delay_period)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of max_cleanup_delay_period setting ({}) must be greater than the value of cleanup_delay_period setting ({})",
            max_cleanup_delay_period.value, cleanup_delay_period.value);
    }

    if (max_merge_selecting_sleep_ms < merge_selecting_sleep_ms)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of max_merge_selecting_sleep_ms setting ({}) must be greater than the value of merge_selecting_sleep_ms setting ({})",
            max_merge_selecting_sleep_ms.value, merge_selecting_sleep_ms.value);
    }

    if (merge_selecting_sleep_slowdown_factor < 1.f)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of merge_selecting_sleep_slowdown_factor setting ({}) cannot be less than 1.0",
            merge_selecting_sleep_slowdown_factor.value);
    }

    if (zero_copy_merge_mutation_min_parts_size_sleep_before_lock != 0
        && zero_copy_merge_mutation_min_parts_size_sleep_before_lock < zero_copy_merge_mutation_min_parts_size_sleep_no_scale_before_lock)
    {
        throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The value of zero_copy_merge_mutation_min_parts_size_sleep_before_lock setting ({}) cannot be less than"
                " the value of zero_copy_merge_mutation_min_parts_size_sleep_no_scale_before_lock ({})",
                zero_copy_merge_mutation_min_parts_size_sleep_before_lock.value,
                zero_copy_merge_mutation_min_parts_size_sleep_no_scale_before_lock.value);
    }
}

void MergeTreeColumnSettings::validate(const SettingsChanges & changes)
{
    static const MergeTreeSettings merge_tree_settings;
    static const std::set<String> allowed_column_level_settings =
    {
        "min_compress_block_size",
        "max_compress_block_size"
    };

    for (const auto & change : changes)
    {
        if (!allowed_column_level_settings.contains(change.name))
            throw Exception(
                ErrorCodes::UNKNOWN_SETTING,
                "Setting {} is unknown or not supported at column level, supported settings: {}",
                change.name,
                fmt::join(allowed_column_level_settings, ", "));
        MergeTreeSettingsImpl::checkCanSet(change.name, change.value);
    }
}

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) MergeTreeSettings##TYPE NAME = &MergeTreeSettingsImpl ::NAME;

namespace MergeTreeSetting
{
    LIST_OF_MERGE_TREE_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)  /// NOLINT(misc-use-internal-linkage)
}

#undef INITIALIZE_SETTING_EXTERN

MergeTreeSettings::MergeTreeSettings() : impl(std::make_unique<MergeTreeSettingsImpl>())
{
}

MergeTreeSettings::MergeTreeSettings(const MergeTreeSettings & settings) : impl(std::make_unique<MergeTreeSettingsImpl>(*settings.impl))
{
}

MergeTreeSettings::MergeTreeSettings(MergeTreeSettings && settings) noexcept
    : impl(std::make_unique<MergeTreeSettingsImpl>(std::move(*settings.impl)))
{
}

MergeTreeSettings::~MergeTreeSettings() = default;

MERGETREE_SETTINGS_SUPPORTED_TYPES(MergeTreeSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

bool MergeTreeSettings::has(std::string_view name) const
{
    return impl->has(name);
}

bool MergeTreeSettings::tryGet(std::string_view name, Field & value) const
{
    return impl->tryGet(name, value);
}

Field MergeTreeSettings::get(std::string_view name) const
{
    return impl->get(name);
}

void MergeTreeSettings::set(std::string_view name, const Field & value)
{
    impl->set(name, value);
}

SettingsChanges MergeTreeSettings::changes() const
{
    return impl->changes();
}

void MergeTreeSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

void MergeTreeSettings::applyChange(const SettingChange & change)
{
    impl->applyChange(change);
}

void MergeTreeSettings::applyCompatibilitySetting(const String & compatibility_value)
{
    /// If setting value is empty, we don't need to change settings
    if (compatibility_value.empty())
        return;

    ClickHouseVersion version(compatibility_value);
    const auto & settings_changes_history = getMergeTreeSettingsChangesHistory();
    /// Iterate through ClickHouse version in descending order and apply reversed
    /// changes for each version that is higher that version from compatibility setting
    for (auto it = settings_changes_history.rbegin(); it != settings_changes_history.rend(); ++it)
    {
        if (version >= it->first)
            break;

        /// Apply reversed changes from this version.
        for (const auto & change : it->second)
        {
            /// In case the alias is being used (e.g. use enable_analyzer) we must change the original setting
            auto final_name = MergeTreeSettingsTraits::resolveName(change.name);
            auto setting_index = MergeTreeSettingsTraits::Accessor::instance().find(final_name);
            auto previous_value = MergeTreeSettingsTraits::Accessor::instance().castValueUtil(setting_index, change.previous_value);

            if (get(final_name) != previous_value)
                set(final_name, previous_value);
        }
    }
}

std::vector<std::string_view> MergeTreeSettings::getAllRegisteredNames() const
{
    std::vector<std::string_view> setting_names;
    for (const auto & setting : impl->all())
    {
        setting_names.emplace_back(setting.getName());
    }
    return setting_names;
}

void MergeTreeSettings::loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach)
{
    impl->loadFromQuery(storage_def, context, is_attach);
}

void MergeTreeSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
            impl->set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in MergeTree config");
        throw;
    }
}

bool MergeTreeSettings::needSyncPart(size_t input_rows, size_t input_bytes) const
{
    return (
        (impl->min_rows_to_fsync_after_merge && input_rows >= impl->min_rows_to_fsync_after_merge)
        || (impl->min_compressed_bytes_to_fsync_after_merge && input_bytes >= impl->min_compressed_bytes_to_fsync_after_merge));
}

void MergeTreeSettings::sanityCheck(size_t background_pool_tasks, bool allow_experimental, bool allow_beta) const
{
    impl->sanityCheck(background_pool_tasks, allow_experimental, allow_beta);
}

void MergeTreeSettings::dumpToSystemMergeTreeSettingsColumns(MutableColumnsAndConstraints & params) const
{
    const auto & constraints = params.constraints;
    MutableColumns & res_columns = params.res_columns;

    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        res_columns[0]->insert(setting_name);
        res_columns[1]->insert(setting.getValueString());
        res_columns[2]->insert(setting.getDefaultValueString());
        res_columns[3]->insert(setting.isValueChanged());
        res_columns[4]->insert(setting.getDescription());

        Field min;
        Field max;
        SettingConstraintWritability writability = SettingConstraintWritability::WRITABLE;
        constraints.get(*this, setting_name, min, max, writability);

        /// These two columns can accept strings only.
        if (!min.isNull())
            min = MergeTreeSettings::valueToStringUtil(setting_name, min);
        if (!max.isNull())
            max = MergeTreeSettings::valueToStringUtil(setting_name, max);

        res_columns[5]->insert(min);
        res_columns[6]->insert(max);
        res_columns[7]->insert(writability == SettingConstraintWritability::CONST);
        res_columns[8]->insert(setting.getTypeName());
        res_columns[9]->insert(setting.getTier() == SettingsTierType::OBSOLETE);
        res_columns[10]->insert(setting.getTier());
    }
}


namespace
{
/// Define transparent hash to we can use
/// std::string_view with the containers
struct TransparentStringHash
{
    using is_transparent = void;
    size_t operator()(std::string_view txt) const { return std::hash<std::string_view>{}(txt); }
};
}

void MergeTreeSettings::addToProgramOptionsIfNotPresent(
    boost::program_options::options_description & main_options, bool allow_repeated_settings)
{
    /// Add merge tree settings manually, because names of some settings
    /// may clash. Query settings have higher priority and we just
    /// skip ambiguous merge tree settings.

    std::unordered_set<std::string, TransparentStringHash, std::equal_to<>> main_option_names;
    for (const auto & option : main_options.options())
        main_option_names.insert(option->long_name());

    const auto & settings_to_aliases = MergeTreeSettingsImpl::Traits::settingsToAliases();
    for (const auto & setting : impl->all())
    {
        const auto add_setting = [&](const std::string_view name)
        {
            if (auto it = main_option_names.find(name); it != main_option_names.end())
                return;

            if (allow_repeated_settings)
                addProgramOptionAsMultitoken(*impl, main_options, name, setting);
            else
                addProgramOption(*impl, main_options, name, setting);
        };

        const auto & setting_name = setting.getName();
        add_setting(setting_name);

        if (auto it = settings_to_aliases.find(setting_name); it != settings_to_aliases.end())
        {
            for (const auto alias : it->second)
            {
                add_setting(alias);
            }
        }
    }
}

Field MergeTreeSettings::castValueUtil(std::string_view name, const Field & value)
{
    return MergeTreeSettingsImpl::castValueUtil(name, value);
}

String MergeTreeSettings::valueToStringUtil(std::string_view name, const Field & value)
{
    return MergeTreeSettingsImpl::valueToStringUtil(name, value);
}

Field MergeTreeSettings::stringToValueUtil(std::string_view name, const String & str)
{
    return MergeTreeSettingsImpl::stringToValueUtil(name, str);
}

bool MergeTreeSettings::hasBuiltin(std::string_view name)
{
    return MergeTreeSettingsImpl::hasBuiltin(name);
}

std::string_view MergeTreeSettings::resolveName(std::string_view name)
{
    return MergeTreeSettingsImpl::Traits::resolveName(name);
}

bool MergeTreeSettings::isReadonlySetting(const String & name)
{
    return name == "index_granularity"
        || name == "index_granularity_bytes"
        || name == "enable_mixed_granularity_parts"
        || name == "add_minmax_index_for_numeric_columns"
        || name == "add_minmax_index_for_string_columns"
        || name == "table_disk"
    ;
}

/// Cloud only
bool MergeTreeSettings::isSMTReadonlySetting(const String & name)
{
    return name == "enable_mixed_granularity_parts";
}

void MergeTreeSettings::checkCanSet(std::string_view name, const Field & value)
{
    MergeTreeSettingsImpl::checkCanSet(name, value);
}

bool MergeTreeSettings::isPartFormatSetting(const String & name)
{
    return name == "min_bytes_for_wide_part" || name == "min_rows_for_wide_part";
}
}
