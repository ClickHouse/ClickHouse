#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/SettingsQuirks.h>
#include <Poco/Environment.h>
#include <Poco/Platform.h>
#include <Common/VersionNumber.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>


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

/// See also QUERY_PROFILER_DEFAULT_SAMPLE_RATE_NS in Core/Defines.h
#if !defined(SANITIZER)
bool queryProfilerWorks() { return true; }
#else
bool queryProfilerWorks() { return false; }
#endif

}

namespace DB
{

namespace Setting
{
    extern const SettingsBool async_query_sending_for_remote;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsUInt64 query_profiler_cpu_time_period_ns;
    extern const SettingsUInt64 query_profiler_real_time_period_ns;
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

    if (!queryProfilerWorks())
    {
        if (settings[Setting::query_profiler_real_time_period_ns])
        {
            settings[Setting::query_profiler_real_time_period_ns] = 0;
            if (log)
                LOG_WARNING(log, "query_profiler_real_time_period_ns has been disabled (due to server had been compiled with sanitizers)");
        }
        if (settings[Setting::query_profiler_cpu_time_period_ns])
        {
            settings[Setting::query_profiler_cpu_time_period_ns] = 0;
            if (log)
                LOG_WARNING(log, "query_profiler_cpu_time_period_ns has been disabled (due to server had been compiled with sanitizers)");
        }
    }
}

void doSettingsSanityCheckClamp(Settings & current_settings, LoggerPtr log)
{
    auto get_current_value = [&current_settings](const std::string_view name) -> Field
    {
        Field current_value;
        bool has_current_value = current_settings.tryGet(name, current_value);
        chassert(has_current_value);
        return current_value;
    };

    UInt64 max_threads = get_current_value("max_threads").safeGet<UInt64>();
    UInt64 max_threads_max_value = 256 * getNumberOfCPUCoresToUse();
    if (max_threads > max_threads_max_value)
    {
        if (log)
            LOG_WARNING(log, "Sanity check: Too many threads requested ({}). Reduced to {}", max_threads, max_threads_max_value);
        current_settings.set("max_threads", max_threads_max_value);
    }

    static constexpr UInt64 max_sane_block_rows_size = 4294967296; // 2^32
    std::unordered_set<String> block_rows_settings{
        "max_block_size",
        "max_insert_block_size",
        "min_insert_block_size_rows",
        "min_insert_block_size_bytes_for_materialized_views",
        "min_external_table_block_size_rows",
        "max_joined_block_size_rows",
        "input_format_parquet_max_block_size"};
    for (auto const & setting : block_rows_settings)
    {
        if (auto block_size = get_current_value(setting).safeGet<UInt64>();
            block_size > max_sane_block_rows_size)
        {
            if (log)
                LOG_WARNING(log, "Sanity check: '{}' value is too high ({}). Reduced to {}", setting, block_size, max_sane_block_rows_size);
            current_settings.set(setting, max_sane_block_rows_size);
        }
    }

    if (auto max_block_size = get_current_value("max_block_size").safeGet<UInt64>(); max_block_size == 0)
    {
        if (log)
            LOG_WARNING(log, "Sanity check: 'max_block_size' cannot be 0. Set to default value {}", DEFAULT_BLOCK_SIZE);
        current_settings.set("max_block_size", DEFAULT_BLOCK_SIZE);
    }
}

}
