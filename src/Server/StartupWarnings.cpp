#include <Server/StartupWarnings.h>

#include <Common/Jemalloc.h>
#include <Common/JemallocMergeTreeArena.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/ThreadFuzzer.h>
#include <Common/filesystemHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <base/getAvailableMemoryAmount.h>

#include <Poco/Logger.h>
#include <Poco/Message.h>

#include <fmt/ranges.h>

#include <filesystem>
#include <optional>
#include <unordered_set>

#include "config.h"

#if defined(OS_LINUX)
#    include <glibc-rseq/rseq.h>
#endif

namespace fs = std::filesystem;

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
}

namespace
{

#if defined(OS_LINUX)
String readLine(const String & path)
{
    ReadBufferFromFile in(path);
    String contents;
    readStringUntilNewlineInto(contents, in);
    return contents;
}

int readNumber(const String & path)
{
    ReadBufferFromFile in(path);
    int result = {};
    readText(result, in);
    return result;
}
#endif

#if defined(SANITIZER)
std::vector<String> getSanitizerNames()
{
    std::vector<String> names;

#if defined(ADDRESS_SANITIZER)
    names.push_back("address");
#endif
#if defined(THREAD_SANITIZER)
    names.push_back("thread");
#endif
#if defined(MEMORY_SANITIZER)
    names.push_back("memory");
#endif
#if defined(UNDEFINED_BEHAVIOR_SANITIZER)
    names.push_back("undefined behavior");
#endif

    return names;
}
#endif

}

void addBuildWarnings(ContextPtr context)
{
#if !defined(NDEBUG) || !defined(__OPTIMIZE__)
    context->addOrUpdateWarningMessage(
        Context::WarningType::CLICKHOUSE_BUILT_IN_DEBUG_MODE,
        PreformattedMessage::create("ClickHouse was built in debug mode. It will work slowly."));
#endif

    if (ThreadFuzzer::instance().isEffective())
        context->addOrUpdateWarningMessage(
            Context::WarningType::THREAD_FUZZER_IS_ENABLED,
            PreformattedMessage::create("ThreadFuzzer is enabled. Application will run slowly and unstable."));

#if defined(SANITIZER)
    auto sanitizers = getSanitizerNames();

    String log_message;
    if (sanitizers.empty())
        log_message = "sanitizer";
    else if (sanitizers.size() == 1)
        log_message = fmt::format("{} sanitizer", sanitizers.front());
    else
        log_message = fmt::format("sanitizers ({})", fmt::join(sanitizers, ", "));

    context->addOrUpdateWarningMessage(
        Context::WarningType::CLICKHOUSE_BUILT_WITH_SANITIZERS,
        PreformattedMessage::create("ClickHouse was built with {}. It will work slowly.", log_message));
#endif

#if WITH_COVERAGE
    context->addOrUpdateWarningMessage(
        Context::WarningType::CLICKHOUSE_BUILT_WITH_COVERAGE,
        PreformattedMessage::create("ClickHouse was built with code coverage. It will work slowly."));
#endif
}

void addMergeTreeArenaPoolWarnings(ContextPtr context)
{
    const size_t created_arenas = JemallocMergeTreeArena::getArenaIndices().size();
    const size_t intended_arenas = JemallocMergeTreeArena::getIntendedArenaCount();
    if (created_arenas < intended_arenas)
    {
        context->addOrUpdateWarningMessage(
            Context::WarningType::MERGE_TREE_JEMALLOC_ARENA_POOL_DEGRADED,
            PreformattedMessage::create(
                "Could only create {} of the {} requested dedicated jemalloc arena(s) for MergeTree metadata; {}.",
                created_arenas, intended_arenas,
                created_arenas > 0 ? "the pool runs with the created arenas"
                                   : "MergeTree metadata falls back to the default arenas"));
    }
}

void addEnvironmentWarnings(ContextPtr context, const Poco::Logger & logger, const std::string & data_path, const std::string & logs_path)
{
    if (logger.is(Poco::Message::PRIO_TEST))
        context->addOrUpdateWarningMessage(
            Context::WarningType::CLICKHOUSE_LOGGING_LEVEL_TEST,
            PreformattedMessage::create(
                "ClickHouse logging level is set to 'test' and performance is degraded. This cannot be used in production."));
#if defined(OS_LINUX)
    try
    {
        const std::unordered_set<std::string> fast_clock_sources = {
            // ARM clock
            "arch_sys_counter",
            // KVM guest clock
            "kvm-clock",
            // X86 clock
            "tsc",
        };
        const char * filename = "/sys/devices/system/clocksource/clocksource0/current_clocksource";
        if (!fast_clock_sources.contains(readLine(filename)))
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_FAST_CLOCK_SOURCE_NOT_USED,
                PreformattedMessage::create("Linux is not using a fast clock source. Performance can be degraded. Check {}", filename));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    if (rseq_cpu_id() < 0)
        context->addOrUpdateWarningMessage(
            Context::WarningType::LINUX_RSEQ_UNAVAILABLE,
            PreformattedMessage::create(
                "The Linux 'restartable sequences' (rseq) feature is not enabled for this process. "
                "ClickHouse uses it to cheaply detect which CPU core a thread is running on, which keeps "
                "per-CPU performance counters (used for internal profiling and statistics) fast to update. "
                "Without it, a slower fallback is used (a real system call on some platforms, such as AArch64), "
                "making these counters more expensive and slightly degrading performance. "
                "This means the runtime C library or the kernel did not register a usable rseq area for this process. "
                "Possible causes: the kernel does not support rseq (it was introduced in Linux 4.18); "
                "the C library does not register it (glibc does so automatically since version 2.35, so upgrading glibc may help; "
                "other libraries, such as musl, do not register it); "
                "or registration was disabled or failed at startup (with glibc, see the 'glibc.pthread.rseq' tunable)."));

    try
    {
        const char * filename = "/proc/sys/vm/overcommit_memory";
        if (readNumber(filename) == 2)
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MEMORY_OVERCOMMIT_DISABLED,
                PreformattedMessage::create("Linux memory overcommit is disabled. Check {}", String(filename)));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        const char * filename = "/sys/kernel/mm/transparent_hugepage/enabled";
        if (readLine(filename).contains("[always]"))
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_TRANSPARENT_HUGEPAGES_SET_TO_ALWAYS,
                PreformattedMessage::create("Linux transparent hugepages are set to \"always\". Check {}", String(filename)));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        const char * filename = "/proc/sys/kernel/pid_max";
        if (readNumber(filename) < 30000)
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MAX_PID_TOO_LOW,
                PreformattedMessage::create("Linux max PID is too low. Check {}", String(filename)));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        const char * filename = "/proc/sys/kernel/threads-max";
        if (readNumber(filename) < 30000)
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MAX_THREADS_COUNT_TOO_LOW,
                PreformattedMessage::create("Linux threads max count is too low. Check {}", String(filename)));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        const char * filename = "/proc/sys/kernel/task_delayacct";
        if (readNumber(filename) == 0)
            context->addOrUpdateWarningMessage(
                Context::WarningType::DELAY_ACCOUNTING_DISABLED,
                PreformattedMessage::create(
                    "Delay accounting is not enabled, OSIOWaitMicroseconds will not be gathered. You can enable it "
                    "using `sudo sh -c 'echo 1 > {}'` or by using sysctl.",
                    String(filename)));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    if (!data_path.empty())
    {
        std::string dev_id = getBlockDeviceId(data_path);
        if (getBlockDeviceType(dev_id) == BlockDeviceType::ROT && getBlockDeviceReadAheadBytes(dev_id) == 0)
            context->addOrUpdateWarningMessage(
                Context::WarningType::ROTATIONAL_DISK_WITH_DISABLED_READHEAD,
                PreformattedMessage::create(
                    "Rotational disk with disabled readahead is in use. Performance can be degraded. Used for data: {}", String(data_path)));
    }

    try
    {
        /// Check if any mdraid arrays are currently being checked, repaired, or degraded.
        /// Resynchronization can significantly degrade disk I/O performance.
        /// A degraded array means one or more disks are missing or faulty.
        fs::path sys_block("/sys/block");
        if (fs::exists(sys_block))
        {
            std::optional<PreformattedMessage> resync_warning;
            std::optional<PreformattedMessage> degraded_warning;

            for (const auto & entry : fs::directory_iterator(sys_block))
            {
                const auto name = entry.path().filename().string();
                if (!name.starts_with("md"))
                    continue;

                auto sync_action_path = entry.path() / "md" / "sync_action";
                if (fs::exists(sync_action_path))
                {
                    String sync_action = readLine(sync_action_path.string());
                    if (sync_action != "idle")
                    {
                        resync_warning = PreformattedMessage::create(
                            "Linux mdraid array {} is currently performing `{}`. Disk I/O performance can be degraded. Check {}",
                            name, sync_action, sync_action_path.string());
                    }
                }

                auto array_state_path = entry.path() / "md" / "array_state";
                if (fs::exists(array_state_path))
                {
                    static const std::unordered_set<String> normal_states = {"active", "active-idle", "clean", "write-pending", "readonly", "read-auto"};
                    String array_state = readLine(array_state_path.string());
                    if (!normal_states.contains(array_state))
                    {
                        degraded_warning = PreformattedMessage::create(
                            "Linux mdraid array {} has state `{}`. Check {}",
                            name, array_state, array_state_path.string());
                    }
                }

                if (resync_warning && degraded_warning)
                    break;
            }

            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MDRAID_IS_BEING_RESYNCHRONIZED, resync_warning);
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MDRAID_IS_DEGRADED, degraded_warning);
        }
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }
#endif

#if USE_JEMALLOC && (defined(OS_LINUX) || defined(OS_DARWIN))
    {
        /// Whether disabled at runtime by jemalloc itself or overridden by the operator, per-CPU
        /// arenas are worth recommending on platforms with a working current-CPU query.
        const char * effective_mode = nullptr;
        if (Jemalloc::tryGetValue("opt.percpu_arena", effective_mode) && effective_mode == std::string_view("disabled"))
        {
            context->addOrUpdateWarningMessage(
                Context::WarningType::JEMALLOC_PERCPU_ARENA_DISABLED,
                PreformattedMessage::create(
                    "jemalloc per-CPU arenas are disabled, either via configuration or automatically by jemalloc itself "
                    "(it disables them at startup when it cannot query the current CPU). They reduce memory usage by "
                    "capping the arena count at the number of CPUs"));
        }
    }
#endif

    try
    {
        if (getAvailableMemoryAmount() < (2l << 30))
            context->addOrUpdateWarningMessage(
                Context::WarningType::AVAILABLE_MEMORY_TOO_LOW,
                PreformattedMessage::create("Available memory at startup is too low (2GiB)."));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        if (!data_path.empty() && !enoughSpaceInDirectory(data_path, 1ull << 30))
            context->addOrUpdateWarningMessage(
                Context::WarningType::AVAILABLE_DISK_SPACE_TOO_LOW_FOR_DATA,
                PreformattedMessage::create("Available disk space for data at startup is too low (1GiB): {}", String(data_path)));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        if (!logs_path.empty() && fs::is_regular_file(logs_path))
        {
            auto logs_parent = fs::path(logs_path).parent_path();
            if (!enoughSpaceInDirectory(logs_parent, 1ull << 30))
                context->addOrUpdateWarningMessage(
                    Context::WarningType::AVAILABLE_DISK_SPACE_TOO_LOW_FOR_LOGS,
                    PreformattedMessage::create("Available disk space for logs at startup is too low (1GiB): {}", String(logs_parent)));
        }
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    if (context->getMergeTreeSettings()[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        constexpr auto message_format_string
            = "The setting 'allow_remote_fs_zero_copy_replication' is enabled for MergeTree tables."
              " But the feature of 'zero-copy replication' is under development and is not ready for production."
              " The usage of this feature can lead to data corruption and loss. The setting should be disabled in production.";
        context->addOrUpdateWarningMessage(
            Context::WarningType::SETTING_ZERO_COPY_REPLICATION_ENABLED,
            PreformattedMessage::create(message_format_string));
    }
}

}
