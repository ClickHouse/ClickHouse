#include <Storages/System/TemplateQueriesBuiltin.h>


namespace DB
{

const std::vector<TemplateQuery> & getBuiltinTemplateQueries()
{
    static const std::vector<TemplateQuery> queries =
    {
        /// ---------------------------------------------------------------- Storage
        {
            "Biggest tables",
            "Storage",
            "Active data size, rows and compression ratio per table, largest first.",
            R"(SELECT
    database,
    `table`,
    sum(rows) AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    round(sum(data_uncompressed_bytes) / nullIf(sum(data_compressed_bytes), 0), 2) AS compression_ratio
FROM system.parts
WHERE active
GROUP BY database, `table`
ORDER BY sum(bytes_on_disk) DESC
LIMIT 50)"
        },
        {
            "Database sizes",
            "Storage",
            "Total active data size, rows and number of tables per database.",
            R"(SELECT
    database,
    uniqExact(`table`) AS tables,
    sum(rows) AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk
FROM system.parts
WHERE active
GROUP BY database
ORDER BY sum(bytes_on_disk) DESC)"
        },
        {
            "Compression ratio per column",
            "Storage",
            "Compressed and uncompressed size of every column in non-system tables, largest first.",
            R"(SELECT
    database,
    `table`,
    name AS column_name,
    type,
    compression_codec,
    formatReadableSize(data_compressed_bytes) AS compressed,
    formatReadableSize(data_uncompressed_bytes) AS uncompressed,
    round(data_uncompressed_bytes / nullIf(data_compressed_bytes, 0), 2) AS compression_ratio
FROM system.columns
WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
ORDER BY data_compressed_bytes DESC
LIMIT 100)"
        },
        {
            "Parts count per partition",
            "Storage",
            "Partitions with the highest number of active parts; a high number means merges are not keeping up.",
            R"(SELECT
    database,
    `table`,
    partition,
    count() AS active_parts,
    sum(rows) AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk,
    max(level) AS max_level
FROM system.parts
WHERE active
GROUP BY database, `table`, partition
ORDER BY active_parts DESC
LIMIT 50)"
        },
        {
            "Inactive parts",
            "Storage",
            "Parts that were merged or mutated away but are not yet removed from disk.",
            R"(SELECT
    database,
    `table`,
    name,
    partition,
    rows,
    formatReadableSize(bytes_on_disk) AS size_on_disk,
    modification_time,
    remove_time,
    refcount,
    disk_name
FROM system.parts
WHERE NOT active
ORDER BY modification_time DESC
LIMIT 100)"
        },
        {
            "Detached parts",
            "Storage",
            "Detached parts with the reason they were detached.",
            R"(SELECT
    database,
    `table`,
    name,
    partition_id,
    reason,
    formatReadableSize(bytes_on_disk) AS size_on_disk,
    modification_time,
    disk
FROM system.detached_parts
ORDER BY modification_time DESC
LIMIT 100)"
        },
        {
            "Disk space",
            "Storage",
            "Free and total space of every configured disk.",
            R"(SELECT
    name,
    path,
    type,
    formatReadableSize(free_space) AS free,
    formatReadableSize(unreserved_space) AS unreserved,
    formatReadableSize(total_space) AS total,
    round(100 * (1 - free_space / nullIf(total_space, 0)), 1) AS used_percent,
    is_remote,
    is_read_only,
    is_broken
FROM system.disks
ORDER BY name)"
        },

        /// ------------------------------------------------------ Merges & Mutations
        {
            "Running merges",
            "Merges & Mutations",
            "Merges and mutations in progress, with progress, size and memory usage.",
            R"(SELECT
    database,
    `table`,
    is_mutation,
    merge_type,
    merge_algorithm,
    round(elapsed, 1) AS elapsed_sec,
    round(progress * 100, 1) AS progress_percent,
    num_parts,
    result_part_name,
    formatReadableSize(total_size_bytes_compressed) AS size_compressed,
    formatReadableSize(memory_usage) AS memory
FROM system.merges
ORDER BY elapsed DESC)"
        },
        {
            "Unfinished mutations",
            "Merges & Mutations",
            "Mutations that are not done yet, with the parts still waiting to be mutated.",
            R"(SELECT
    database,
    `table`,
    mutation_id,
    command,
    create_time,
    parts_to_do,
    parts_to_do_names,
    is_killed,
    latest_fail_reason
FROM system.mutations
WHERE NOT is_done
ORDER BY create_time ASC)"
        },
        {
            "Failed mutations",
            "Merges & Mutations",
            "Unfinished mutations that have a recorded failure, with the failing part and the reason.",
            R"(SELECT
    database,
    `table`,
    mutation_id,
    command,
    create_time,
    parts_to_do,
    latest_failed_part,
    latest_fail_time,
    latest_fail_error_code_name,
    latest_fail_reason
FROM system.mutations
WHERE NOT is_done AND latest_fail_reason != ''
ORDER BY latest_fail_time DESC)"
        },
        {
            "Slowest merges in part log",
            "Merges & Mutations",
            "Longest merges and mutations finished in the last day according to system.part_log.",
            R"(SELECT
    event_time,
    database,
    `table`,
    event_type,
    merge_reason,
    part_name,
    length(merged_from) AS source_parts,
    rows,
    formatReadableSize(size_in_bytes) AS result_size,
    formatReadableSize(peak_memory_usage) AS peak_memory,
    duration_ms
FROM system.part_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 DAY
    AND event_type IN ('MergeParts', 'MutatePart')
    AND error = 0
ORDER BY duration_ms DESC
LIMIT 50)"
        },
        {
            "Failed part operations",
            "Merges & Mutations",
            "Failed merges, mutations, downloads and moves in the last day from system.part_log.",
            R"(SELECT
    event_time,
    database,
    `table`,
    event_type,
    part_name,
    error,
    exception
FROM system.part_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 DAY
    AND error != 0
ORDER BY event_time DESC
LIMIT 100)"
        },

        /// ------------------------------------------------------------ Replication
        {
            "Replicas status",
            "Replication",
            "Health, delay and queue size of every replicated table on this server.",
            R"(SELECT
    database,
    `table`,
    replica_name,
    is_leader,
    is_readonly,
    is_session_expired,
    absolute_delay,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    part_mutations_in_queue,
    active_replicas,
    total_replicas
FROM system.replicas
ORDER BY absolute_delay DESC, queue_size DESC)"
        },
        {
            "Readonly replicas",
            "Replication",
            "Replicas that are in readonly mode or have an expired Keeper session, with the last Keeper error.",
            R"(SELECT
    database,
    `table`,
    replica_name,
    is_readonly,
    is_session_expired,
    readonly_start_time,
    zookeeper_exception,
    last_queue_update_exception
FROM system.replicas
WHERE is_readonly OR is_session_expired
ORDER BY database, `table`)"
        },
        {
            "Replication delay",
            "Replication",
            "Replicas lagging behind, with the age of the oldest queue entries.",
            R"(SELECT
    database,
    `table`,
    absolute_delay,
    queue_size,
    queue_oldest_time,
    inserts_oldest_time,
    merges_oldest_time,
    part_mutations_oldest_time,
    oldest_part_to_get,
    oldest_part_to_merge_to,
    log_max_index - log_pointer AS log_entries_behind
FROM system.replicas
WHERE absolute_delay > 0 OR queue_size > 0
ORDER BY absolute_delay DESC)"
        },
        {
            "Replication queue summary",
            "Replication",
            "Replication queue entries grouped by table and entry type, with retry statistics.",
            R"(SELECT
    database,
    `table`,
    type,
    count() AS entries,
    countIf(is_currently_executing) AS executing,
    max(num_tries) AS max_tries,
    max(num_postponed) AS max_postponed,
    min(create_time) AS oldest_entry
FROM system.replication_queue
GROUP BY database, `table`, type
ORDER BY entries DESC)"
        },
        {
            "Replication queue errors",
            "Replication",
            "Replication queue entries that failed or are being postponed, with the last exception.",
            R"(SELECT
    database,
    `table`,
    type,
    new_part_name,
    create_time,
    num_tries,
    last_attempt_time,
    last_exception,
    postpone_reason
FROM system.replication_queue
WHERE last_exception != '' OR postpone_reason != ''
ORDER BY num_tries DESC, create_time ASC
LIMIT 100)"
        },
        {
            "Lost and suspicious parts",
            "Replication",
            "Replicated tables that have lost parts or parts waiting for a consistency check.",
            R"(SELECT
    database,
    `table`,
    replica_name,
    lost_part_count,
    parts_to_check,
    future_parts
FROM system.replicas
WHERE lost_part_count > 0 OR parts_to_check > 0
ORDER BY lost_part_count DESC, parts_to_check DESC)"
        },
        {
            "Keeper paths of replicated tables",
            "Replication",
            "ZooKeeper/Keeper path and replica path of every replicated table.",
            R"(SELECT
    database,
    `table`,
    engine,
    zookeeper_name,
    zookeeper_path,
    replica_name,
    replica_path
FROM system.replicas
ORDER BY database, `table`)"
        },
        {
            "Keeper connection",
            "Replication",
            "Current ZooKeeper/Keeper connections and session state.",
            R"(SELECT
    name,
    host,
    port,
    index,
    is_expired,
    connected_time,
    session_uptime_elapsed_seconds,
    session_timeout_ms,
    availability_zone
FROM system.zookeeper_connection)"
        },

        /// ---------------------------------------------------- Queries & Performance
        {
            "Running queries",
            "Queries & Performance",
            "Queries currently executing, longest first.",
            R"(SELECT
    query_id,
    user,
    round(elapsed, 1) AS elapsed_sec,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    formatReadableSize(memory_usage) AS memory,
    formatReadableSize(peak_memory_usage) AS peak_memory,
    query_kind,
    substring(query, 1, 200) AS query
FROM system.processes
WHERE query_id != currentQueryID()
ORDER BY elapsed DESC)"
        },
        {
            "Slowest queries",
            "Queries & Performance",
            "Slowest finished queries of the last day from system.query_log.",
            R"(SELECT
    event_time,
    query_id,
    user,
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    result_rows,
    formatReadableSize(memory_usage) AS memory,
    substring(query, 1, 200) AS query
FROM system.query_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 DAY
    AND type = 'QueryFinish'
    AND is_initial_query
ORDER BY query_duration_ms DESC
LIMIT 50)"
        },
        {
            "Top queries by memory",
            "Queries & Performance",
            "Finished queries of the last day with the highest peak memory usage.",
            R"(SELECT
    event_time,
    query_id,
    user,
    formatReadableSize(memory_usage) AS memory,
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    substring(query, 1, 200) AS query
FROM system.query_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 DAY
    AND type = 'QueryFinish'
    AND is_initial_query
ORDER BY memory_usage DESC
LIMIT 50)"
        },
        {
            "Top queries by rows read",
            "Queries & Performance",
            "Finished queries of the last day that read the most rows.",
            R"(SELECT
    event_time,
    query_id,
    user,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    query_duration_ms,
    formatReadableSize(memory_usage) AS memory,
    substring(query, 1, 200) AS query
FROM system.query_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 DAY
    AND type = 'QueryFinish'
    AND is_initial_query
ORDER BY read_rows DESC
LIMIT 50)"
        },
        {
            "Query patterns by total time",
            "Queries & Performance",
            "Queries of the last day grouped by normalized_query_hash, ordered by total execution time.",
            R"(SELECT
    normalized_query_hash,
    count() AS executions,
    round(sum(query_duration_ms) / 1000, 1) AS total_sec,
    round(avg(query_duration_ms)) AS avg_ms,
    round(quantile(0.95)(query_duration_ms)) AS p95_ms,
    sum(read_rows) AS read_rows,
    formatReadableSize(max(memory_usage)) AS max_memory,
    any(substring(query, 1, 200)) AS example_query
FROM system.query_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 DAY
    AND type = 'QueryFinish'
    AND is_initial_query
GROUP BY normalized_query_hash
ORDER BY sum(query_duration_ms) DESC
LIMIT 50)"
        },
        {
            "Failed queries",
            "Queries & Performance",
            "Queries of the last day that failed, with the exception code and message.",
            R"(SELECT
    event_time,
    query_id,
    user,
    exception_code,
    substring(exception, 1, 300) AS exception,
    query_duration_ms,
    substring(query, 1, 200) AS query
FROM system.query_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 DAY
    AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
ORDER BY event_time DESC
LIMIT 100)"
        },
        {
            "Threads of slowest queries",
            "Queries & Performance",
            "Thread count and totals of the slowest queries of the last day. Empty unless `log_query_threads` is enabled.",
            R"(SELECT
    query_id,
    any(user) AS user,
    count() AS threads,
    max(query_duration_ms) AS query_duration_ms,
    formatReadableSize(max(peak_memory_usage)) AS max_thread_peak_memory,
    sum(read_rows) AS read_rows,
    any(substring(query, 1, 200)) AS query
FROM system.query_thread_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 DAY
GROUP BY query_id
ORDER BY query_duration_ms DESC
LIMIT 50)"
        },
        {
            "Slow materialized views",
            "Queries & Performance",
            "Views that took the longest to process inserts in the last day, from system.query_views_log.",
            R"(SELECT
    view_name,
    view_type,
    count() AS runs,
    countIf(status NOT IN ('QueryFinish', 'QueryStart')) AS failed,
    round(avg(view_duration_ms)) AS avg_ms,
    max(view_duration_ms) AS max_ms,
    sum(written_rows) AS written_rows,
    formatReadableSize(max(peak_memory_usage)) AS max_memory
FROM system.query_views_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 DAY
GROUP BY view_name, view_type
ORDER BY sum(view_duration_ms) DESC
LIMIT 50)"
        },

        /// ------------------------------------------------------------ Errors & Logs
        {
            "Error counters",
            "Errors & Logs",
            "Errors since server start grouped by error code, with the latest message.",
            R"(SELECT
    name,
    code,
    value AS occurrences,
    last_error_time,
    remote,
    last_error_message
FROM system.errors
ORDER BY value DESC
LIMIT 50)"
        },
        {
            "Recent server errors",
            "Errors & Logs",
            "Error, critical and fatal messages from system.text_log in the last hour.",
            R"(SELECT
    event_time,
    level,
    logger_name,
    query_id,
    substring(message, 1, 400) AS message
FROM system.text_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 HOUR
    AND level IN ('Fatal', 'Critical', 'Error')
ORDER BY event_time DESC
LIMIT 200)"
        },
        {
            "Server warnings",
            "Errors & Logs",
            "Active warnings the server reports about its own configuration or state.",
            R"(SELECT message
FROM system.warnings)"
        },

        /// ---------------------------------------------------------------- Cluster
        {
            "Clusters and shards",
            "Cluster",
            "Configured clusters with shards, replicas and connection error counters.",
            R"(SELECT
    cluster,
    shard_num,
    replica_num,
    host_name,
    port,
    is_local,
    errors_count,
    slowdowns_count,
    estimated_recovery_time
FROM system.clusters
ORDER BY cluster, shard_num, replica_num)"
        },
        {
            "Distribution queue",
            "Cluster",
            "Pending and broken asynchronous inserts of Distributed tables.",
            R"(SELECT
    database,
    `table`,
    data_path,
    is_blocked,
    error_count,
    data_files,
    formatReadableSize(data_compressed_bytes) AS pending_size,
    broken_data_files,
    formatReadableSize(broken_data_compressed_bytes) AS broken_size,
    last_exception_time,
    last_exception
FROM system.distribution_queue
ORDER BY data_files DESC)"
        },
        {
            "Pending async inserts",
            "Cluster",
            "Asynchronous inserts currently buffered in memory waiting to be flushed.",
            R"(SELECT
    database,
    `table`,
    format,
    sum(length(entries.query_id)) AS pending_queries,
    formatReadableSize(sum(total_bytes)) AS pending_size,
    min(first_update) AS oldest_entry
FROM system.asynchronous_inserts
GROUP BY database, `table`, format
ORDER BY sum(total_bytes) DESC)"
        },
        {
            "Cluster-wide parts per replica",
            "Cluster",
            "Cluster-wide: number of active parts and data size per table on every replica of the '{cluster}' cluster. Requires a `cluster` macro to be defined in the server configuration.",
            R"(SELECT
    hostName() AS host,
    database,
    `table`,
    count() AS active_parts,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk
FROM clusterAllReplicas('{cluster}', system.parts)
WHERE active
GROUP BY host, database, `table`
ORDER BY active_parts DESC
LIMIT 100)"
        },

        /// -------------------------------------------------------- Users & Access
        {
            "Users",
            "Users & Access",
            "Users with their storage, authentication type and allowed hosts.",
            R"(SELECT
    name,
    storage,
    auth_type,
    host_ip,
    host_names,
    host_names_regexp,
    default_database,
    default_roles_all,
    default_roles_list
FROM system.users
ORDER BY name)"
        },
        {
            "Grants",
            "Users & Access",
            "Privileges granted to users and roles.",
            R"(SELECT
    user_name,
    role_name,
    access_type,
    database,
    `table`,
    `column`,
    is_partial_revoke,
    grant_option
FROM system.grants
ORDER BY user_name, role_name, access_type)"
        },
        {
            "Failed logins",
            "Users & Access",
            "Failed login attempts in the last day from system.session_log.",
            R"(SELECT
    event_time,
    user,
    auth_type,
    interface,
    client_address,
    client_name,
    failure_reason
FROM system.session_log
WHERE event_date >= today() - 1
    AND event_time > now() - INTERVAL 1 DAY
    AND type = 'LoginFailure'
ORDER BY event_time DESC
LIMIT 200)"
        },

        /// --------------------------------------------------------------- Settings
        {
            "Changed settings",
            "Settings",
            "Session settings whose value differs from the default.",
            R"(SELECT
    name,
    value,
    default,
    type,
    description
FROM system.settings
WHERE changed
ORDER BY name)"
        },
        {
            "Changed server settings",
            "Settings",
            "Server settings whose value differs from the default, and whether they need a restart to change.",
            R"(SELECT
    name,
    value,
    default,
    changeable_without_restart,
    description
FROM system.server_settings
WHERE changed
ORDER BY name)"
        },

        /// ---------------------------------------------------------------- Metrics
        {
            "Current metrics",
            "Metrics",
            "Non-zero current metrics from system.metrics, largest values first.",
            R"(SELECT
    metric,
    value,
    description
FROM system.metrics
WHERE value != 0
ORDER BY value DESC
LIMIT 100)"
        },
        {
            "Top events since start",
            "Metrics",
            "Cumulative event counters since server start, largest first.",
            R"(SELECT
    event,
    value,
    description
FROM system.events
ORDER BY value DESC
LIMIT 100)"
        },
        {
            "Server resource highlights",
            "Metrics",
            "Selected asynchronous metrics: uptime, memory, load average and connection-related gauges.",
            R"(SELECT
    metric,
    value,
    description
FROM system.asynchronous_metrics
WHERE metric IN (
        'Uptime',
        'jemalloc.resident',
        'MemoryResident',
        'MemoryVirtual',
        'OSMemoryTotal',
        'OSMemoryAvailable',
        'LoadAverage1',
        'LoadAverage5',
        'LoadAverage15',
        'NumberOfDatabases',
        'NumberOfTables',
        'TotalPartsOfMergeTreeTables',
        'MaxPartCountForPartition',
        'ReplicasMaxAbsoluteDelay',
        'ReplicasMaxQueueSize')
ORDER BY metric)"
        },

        /// ----------------------------------------------------------- Dictionaries
        {
            "Dictionaries status",
            "Dictionaries",
            "Loading status, memory usage, hit rate and last error of every dictionary.",
            R"(SELECT
    database,
    name,
    status,
    type,
    origin,
    element_count,
    formatReadableSize(bytes_allocated) AS memory,
    round(hit_rate, 3) AS hit_rate,
    last_successful_update_time,
    loading_duration,
    last_exception
FROM system.dictionaries
ORDER BY bytes_allocated DESC)"
        },

        /// ------------------------------------------------------------------ Kafka
        {
            "Kafka consumers",
            "Kafka",
            "State of Kafka engine consumers: last poll, commits, rebalances and missing dependencies.",
            R"(SELECT
    database,
    `table`,
    consumer_id,
    is_currently_used,
    num_messages_read,
    last_poll_time,
    num_commits,
    last_commit_time,
    num_rebalance_assignments,
    last_rebalance_time,
    missing_dependencies
FROM system.kafka_consumers
ORDER BY database, `table`)"
        },
    };

    return queries;
}

}
