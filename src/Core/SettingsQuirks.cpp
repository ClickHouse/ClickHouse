#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/SettingsQuirks.h>
#include <IO/preadNoWait.h>
#include <Poco/Environment.h>
#include <Poco/Platform.h>
#include <Common/VersionNumber.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>

#include <mutex>

#include <fmt/ranges.h>


namespace
{

/// Detect does epoll_wait with nested epoll fds works correctly.
/// Polling nested epoll fds from epoll_wait is required for async_socket_for_remote and use_hedged_requests.
///
/// It may not be reliable in 5.5+ [1], that has been fixed in 5.7+ [2] or 5.6.13+.
///
///   [1]: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=339ddb53d373
///   [2]: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=0c54a6a44bf3
bool nestedEpollWorks(LoggerPtr log)
{
    if (Poco::Environment::os() != POCO_OS_LINUX)
        return true;

    DB::VersionNumber linux_version(Poco::Environment::osVersion());

    /// the check is correct since there will be no more 5.5.x releases.
    if (linux_version >= DB::VersionNumber{5, 5, 0} && linux_version < DB::VersionNumber{5, 6, 13})
    {
        if (log)
            LOG_WARNING(log, "Nested epoll_wait has some issues on kernels [5.5.0, 5.6.13). You should upgrade it to avoid possible issues.");
        return false;
    }

    return true;
}

}

namespace DB
{

namespace Setting
{
    extern const SettingsBool async_query_sending_for_remote;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsBool make_distributed_plan;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsUInt64 automatic_parallel_replicas_mode;
    extern const SettingsBool correlated_subqueries_use_in_memory_buffer;
    extern const SettingsBool use_skip_indexes_on_data_read;
    extern const SettingsBool compile_expressions;
    extern const SettingsBool query_plan_direct_read_from_text_index;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsNonZeroUInt64 format_avro_schema_registry_connection_timeout;
    extern const SettingsNonZeroUInt64 format_avro_schema_registry_receive_timeout;
    extern const SettingsNonZeroUInt64 format_avro_schema_registry_retry_initial_backoff_ms;
    extern const SettingsNonZeroUInt64 format_avro_schema_registry_send_timeout;
    extern const SettingsUInt64 format_avro_schema_registry_max_retries;
    extern const SettingsNonZeroUInt64 input_format_parquet_max_block_size;
    extern const SettingsString local_filesystem_read_method;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsNonZeroUInt64 max_read_buffer_size;
    extern const SettingsUInt64 max_read_buffer_size_local_fs;
    extern const SettingsUInt64 max_read_buffer_size_remote_fs;
    extern const SettingsUInt64 prefetch_buffer_size;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 min_insert_block_size_bytes_for_materialized_views;
    extern const SettingsUInt64 min_external_table_block_size_rows;
    extern const SettingsUInt64 max_joined_block_size_rows;
    extern const SettingsUInt64 max_streams_for_merge_tree_reading;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
    extern const SettingsBool use_hedged_requests;
}

/// Update some settings defaults to avoid some known issues.
void applySettingsQuirks(Settings & settings, LoggerPtr log)
{
    if (!nestedEpollWorks(log))
    {
        if (!settings[Setting::async_socket_for_remote].changed && settings[Setting::async_socket_for_remote])
        {
            settings[Setting::async_socket_for_remote] = false;
            if (log)
                LOG_WARNING(log, "async_socket_for_remote has been disabled (you can explicitly enable it still)");
        }
        if (!settings[Setting::async_query_sending_for_remote].changed && settings[Setting::async_query_sending_for_remote])
        {
            settings[Setting::async_query_sending_for_remote] = false;
            if (log)
                LOG_WARNING(log, "async_query_sending_for_remote has been disabled (you can explicitly enable it still)");
        }
        if (!settings[Setting::use_hedged_requests].changed && settings[Setting::use_hedged_requests])
        {
            settings[Setting::use_hedged_requests] = false;
            if (log)
                LOG_WARNING(log, "use_hedged_requests has been disabled (you can explicitly enable it still)");
        }
    }

    /// The 'pread_threadpool' read method hands a read off to a thread pool, unless the data is
    /// already in the page cache, which it checks with the `preadv2` system call and the `RWF_NOWAIT`
    /// flag. Without that check the hand-off is paid for every read, including the reads that only
    /// have to copy the data from the page cache, and reading in the calling thread is cheaper.
    /// The support is probed with a raw system call, so it is only probed when this read method
    /// is actually requested.
    if (!settings[Setting::local_filesystem_read_method].changed
        && settings[Setting::local_filesystem_read_method].value == "pread_threadpool"
        && !preadNoWaitUnavailableReason().empty())
    {
        settings[Setting::local_filesystem_read_method] = "pread";

        /// This is a property of this host, not of the query. A setting marked as changed goes into
        /// `Settings::changes()`, and `Connection::sendQuery` forwards those to the remote shards -
        /// which would downgrade the read method on hosts where the system call works. Leave it
        /// unchanged instead, so it is only the effective default here.
        settings[Setting::local_filesystem_read_method].setChanged(false);

        /// `applySettingsQuirks` is called for every settings change as well, in every program:
        /// `clickhouse-client` writes its log to stderr, and an unconditional warning here fails
        /// every test that checks the client's stderr on a host without the system call. Report
        /// the reason like the other quirks do - only to the caller that passes a logger, which
        /// is `Context::setDefaultProfiles` at server startup - and once per process, because
        /// that call runs again for every copy of the server context.
        if (log)
        {
            static std::once_flag reported;
            std::call_once(
                reported,
                [&]
                {
                    LOG_WARNING(
                        log,
                        "The default value of local_filesystem_read_method has been switched from 'pread_threadpool' "
                        "to 'pread' (you can explicitly set it back still), because {}. That system call is what "
                        "'pread_threadpool' needs to read the data that is already in the page cache "
                        "without handing the read off to a thread pool",
                        preadNoWaitUnavailableReason());
                });
        }
    }
}

/// TODO: This is a temporary workaround (issues #109476, #109329). Remove each override once
/// distributed plans support the corresponding feature - e.g. for the text index direct read,
/// let the worker re-run the rewrite over its pinned part list instead of disabling it.
void adjustSettingsForMakeDistributedPlan(Settings & settings)
{
    if (!settings[Setting::make_distributed_plan])
        return;

    Strings adjusted;

    if (settings[Setting::allow_experimental_parallel_reading_from_replicas] > 0)
    {
        settings[Setting::allow_experimental_parallel_reading_from_replicas] = 0;
        adjusted.emplace_back("enable_parallel_replicas = 0");
    }
    if (settings[Setting::automatic_parallel_replicas_mode] != 0)
    {
        settings[Setting::automatic_parallel_replicas_mode] = 0;
        adjusted.emplace_back("automatic_parallel_replicas_mode = 0");
    }
    if (settings[Setting::correlated_subqueries_use_in_memory_buffer])
    {
        settings[Setting::correlated_subqueries_use_in_memory_buffer] = false;
        adjusted.emplace_back("correlated_subqueries_use_in_memory_buffer = 0");
    }
    if (settings[Setting::use_skip_indexes_on_data_read])
    {
        settings[Setting::use_skip_indexes_on_data_read] = false;
        adjusted.emplace_back("use_skip_indexes_on_data_read = 0");
    }
    if (settings[Setting::compile_expressions])
    {
        settings[Setting::compile_expressions] = false;
        adjusted.emplace_back("compile_expressions = 0");
    }
    if (settings[Setting::query_plan_direct_read_from_text_index])
    {
        settings[Setting::query_plan_direct_read_from_text_index] = false;
        adjusted.emplace_back("query_plan_direct_read_from_text_index = 0");
    }
    /// The concurrency control currently can cause starvation for cases when multiple tasks from one
    /// query are executed on the same node and periodically wait on reads and writes to exchange sockets
    /// while holding CPU slots.
    if (settings[Setting::use_concurrency_control])
    {
        settings[Setting::use_concurrency_control] = false;
        adjusted.emplace_back("use_concurrency_control = 0");
    }

    if (!adjusted.empty())
        LOG_DEBUG(
            getLogger("adjustSettingsForMakeDistributedPlan"),
            "Adjusted settings not supported by distributed query plans (make_distributed_plan is enabled): {}",
            fmt::join(adjusted, ", "));
}

void doSettingsSanityCheckClamp(Settings & current_settings, LoggerPtr log)
{
    UInt64 max_threads = current_settings[Setting::max_threads];
    UInt64 max_threads_max_value = 256 * getNumberOfCPUCoresToUse();
    if (max_threads > max_threads_max_value)
    {
        if (log)
            LOG_WARNING(log, "Sanity check: Too many threads requested ({}). Reduced to {}", max_threads, max_threads_max_value);
        current_settings[Setting::max_threads] = max_threads_max_value;
    }

    /// Same ceiling as max_threads: an unbounded value drives pipes.reserve()/resize() in
    /// ReadFromMergeTree, where reserve() throws std::length_error when the requested size exceeds
    /// the vector max_size.
    if (UInt64 max_streams_for_merge_tree_reading = current_settings[Setting::max_streams_for_merge_tree_reading];
        max_streams_for_merge_tree_reading > max_threads_max_value)
    {
        if (log)
            LOG_WARNING(
                log,
                "Sanity check: 'max_streams_for_merge_tree_reading' value is too high ({}). Reduced to {}",
                max_streams_for_merge_tree_reading,
                max_threads_max_value);
        current_settings[Setting::max_streams_for_merge_tree_reading] = max_threads_max_value;
    }

    static constexpr UInt64 max_sane_block_rows_size = 4294967296; // 2^32

    using namespace std::literals;
#define CHECK_MAX_VALUE(SETTING_VALUE) \
    if (UInt64 block_size = current_settings[Setting::SETTING_VALUE]; block_size > max_sane_block_rows_size) \
    { \
        if (log) \
            LOG_WARNING( \
                log, "Sanity check: '{}' value is too high ({}). Reduced to {}", #SETTING_VALUE, block_size, max_sane_block_rows_size); \
        current_settings[Setting::SETTING_VALUE] = max_sane_block_rows_size; \
    }

    CHECK_MAX_VALUE(max_block_size)
    CHECK_MAX_VALUE(max_insert_block_size)
    CHECK_MAX_VALUE(min_insert_block_size_rows)
    CHECK_MAX_VALUE(min_insert_block_size_bytes_for_materialized_views)
    CHECK_MAX_VALUE(min_external_table_block_size_rows)
    CHECK_MAX_VALUE(max_joined_block_size_rows)
    CHECK_MAX_VALUE(input_format_parquet_max_block_size)

#undef CHECK_MAX_VALUE

#define CHECK_READ_BUFFER_SIZE(SETTING_VALUE) \
    if (UInt64 buffer_size = current_settings[Setting::SETTING_VALUE]; buffer_size > MAX_SANE_READ_BUFFER_SIZE) \
    { \
        if (log) \
            LOG_WARNING( \
                log, "Sanity check: '{}' value is too high ({}). Reduced to {}", #SETTING_VALUE, buffer_size, MAX_SANE_READ_BUFFER_SIZE); \
        current_settings[Setting::SETTING_VALUE] = MAX_SANE_READ_BUFFER_SIZE; \
    }

    CHECK_READ_BUFFER_SIZE(max_read_buffer_size)
    CHECK_READ_BUFFER_SIZE(max_read_buffer_size_local_fs)
    CHECK_READ_BUFFER_SIZE(max_read_buffer_size_remote_fs)
    CHECK_READ_BUFFER_SIZE(prefetch_buffer_size)

#undef CHECK_READ_BUFFER_SIZE

    /// These used to be rejected where they are read, on a path taken by every query, so an
    /// out-of-range value failed even the `SET` putting it back, bricking the session. Reading
    /// them clamps as well, for the application types this function does not run for.
#define CHECK_SETTING_MAX_VALUE(SETTING_VALUE, MAX_VALUE) \
    if (UInt64 setting_value = current_settings[Setting::SETTING_VALUE]; setting_value > (MAX_VALUE)) \
    { \
        if (log) \
            LOG_WARNING(log, "Sanity check: '{}' value is too high ({}). Reduced to {}", #SETTING_VALUE, setting_value, MAX_VALUE); \
        current_settings[Setting::SETTING_VALUE] = (MAX_VALUE); \
    }

    CHECK_SETTING_MAX_VALUE(format_avro_schema_registry_connection_timeout, MAX_SCHEMA_REGISTRY_TIMEOUT_SECONDS)
    CHECK_SETTING_MAX_VALUE(format_avro_schema_registry_send_timeout, MAX_SCHEMA_REGISTRY_TIMEOUT_SECONDS)
    CHECK_SETTING_MAX_VALUE(format_avro_schema_registry_receive_timeout, MAX_SCHEMA_REGISTRY_TIMEOUT_SECONDS)
    CHECK_SETTING_MAX_VALUE(format_avro_schema_registry_max_retries, MAX_SCHEMA_REGISTRY_RETRIES)
    CHECK_SETTING_MAX_VALUE(format_avro_schema_registry_retry_initial_backoff_ms, MAX_SCHEMA_REGISTRY_INITIAL_BACKOFF_MS)
    CHECK_SETTING_MAX_VALUE(temporary_files_buffer_size, MAX_TEMPORARY_FILES_BUFFER_SIZE)

#undef CHECK_SETTING_MAX_VALUE

    if (auto max_block_size = current_settings[Setting::max_block_size]; max_block_size == 0)
    {
        if (log)
            LOG_WARNING(log, "Sanity check: 'max_block_size' cannot be 0. Set to default value {}", DEFAULT_BLOCK_SIZE);
        current_settings[Setting::max_block_size] = DEFAULT_BLOCK_SIZE;
    }
}

UInt64 clampTemporaryFilesBufferSize(UInt64 buffer_size)
{
    if (buffer_size <= MAX_TEMPORARY_FILES_BUFFER_SIZE)
        return buffer_size;

    LOG_WARNING(
        getLogger("SettingsSanity"),
        "Sanity check: 'temporary_files_buffer_size' value is too high ({}). Reduced to {}",
        buffer_size,
        MAX_TEMPORARY_FILES_BUFFER_SIZE);
    return MAX_TEMPORARY_FILES_BUFFER_SIZE;
}

}
