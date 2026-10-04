#include <Server/StartupWarnings.h>

#include <Common/Jemalloc.h>
#include <Common/JemallocMergeTreeArena.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/ThreadFuzzer.h>
#include <Common/VersionNumber.h>
#include <Common/atomicRename.h>
#include <Common/filesystemHelpers.h>
#include <Common/formatReadable.h>
#include <Disks/warnIfExt4CorruptionKernelBug.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <base/getAvailableMemoryAmount.h>

#include <Poco/Environment.h>
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

    try
    {
        String first_slow_governor;
        fs::path cpu_dir("/sys/devices/system/cpu");
        if (fs::exists(cpu_dir))
        {
            for (const auto & entry : fs::directory_iterator(cpu_dir))
            {
                const auto name = entry.path().filename().string();
                if (name.size() < 4 || !name.starts_with("cpu") || name[3] < '0' || name[3] > '9')
                    continue;

                auto governor_path = entry.path() / "cpufreq" / "scaling_governor";
                if (!fs::exists(governor_path))
                    continue;

                try
                {
                    String governor = readLine(governor_path.string());
                    if (governor != "performance" && first_slow_governor.empty())
                        first_slow_governor = governor;
                }
                catch (const std::exception &) // NOLINT(bugprone-empty-catch)
                {
                    /// One unreadable CPU must not hide the governors of the others.
                }
            }
        }
        if (!first_slow_governor.empty())
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_CPU_SCALING_GOVERNOR_NOT_PERFORMANCE,
                PreformattedMessage::create(
                    "Linux CPU scaling governor is set to \"{}\" instead of \"performance\" for some CPUs."
                    " Performance can be degraded. Check /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor",
                    first_slow_governor));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        /// Ranges of Linux kernel versions with bugs known to affect ClickHouse (see #18794).
        VersionNumber linux_version(Poco::Environment::osVersion());
        std::optional<PreformattedMessage> kernel_warning;
        if (linux_version < VersionNumber{3, 2, 0})
            kernel_warning = PreformattedMessage::create(
                "Linux kernel version {} is too old: IPv6 packets can be dropped randomly. Consider upgrading the kernel.",
                linux_version.toString());
        else if (linux_version >= VersionNumber{5, 5, 0} && linux_version < VersionNumber{5, 6, 13})
            kernel_warning = PreformattedMessage::create(
                "Linux kernel version {} has broken nested epoll_wait (fixed in 5.6.13). Consider upgrading the kernel.",
                linux_version.toString());
        context->addOrUpdateWarningMessage(Context::WarningType::LINUX_KERNEL_WITH_KNOWN_ISSUES, kernel_warning);

        /// The 4.16.0-4.16.3 ext4 corruption warning lives with the disks: every local disk and
        /// filesystem cache checks its own constructor-normalized root, so here only the server's
        /// data path is probed.
        if (!data_path.empty())
            warnIfAffectedByExt4CorruptionKernelBug(data_path, "the server's data path");
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
        const char * filename = "/proc/sys/fs/file-max";
        /// The value can be as large as 2^63 - 1, so don't use the int-typed `readNumber` here.
        ReadBufferFromFile in(filename);
        UInt64 system_wide_max_open_files = 0;
        readText(system_wide_max_open_files, in);
        if (system_wide_max_open_files < 500000)
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MAX_OPEN_FILES_SYSTEM_WIDE_TOO_LOW,
                PreformattedMessage::create("Linux system-wide limit on the number of open files is too low. Check {}", String(filename)));
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
            std::optional<PreformattedMessage> stripe_cache_warning;

            for (const auto & entry : fs::directory_iterator(sys_block))
            {
                const auto name = entry.path().filename().string();
                if (!name.starts_with("md"))
                    continue;

                try
                {
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

                    auto level_path = entry.path() / "md" / "level";
                    auto stripe_cache_path = entry.path() / "md" / "stripe_cache_size";
                    if (fs::exists(level_path) && fs::exists(stripe_cache_path))
                    {
                        String level = readLine(level_path.string());
                        /// The default stripe cache size of 256 pages is known to be insufficient for good RAID 4/5/6 write performance.
                        if ((level == "raid4" || level == "raid5" || level == "raid6") && readNumber(stripe_cache_path.string()) < 1024)
                        {
                            stripe_cache_warning = PreformattedMessage::create(
                                "Linux mdraid array {} with level `{}` has a low stripe cache size. Write performance can be degraded. Check {}",
                                name, level, stripe_cache_path.string());
                        }
                    }
                }
                catch (const std::exception &) // NOLINT(bugprone-empty-catch)
                {
                    /// One unreadable /sys leaf must not hide the state of the other arrays.
                }

                if (resync_warning && degraded_warning && stripe_cache_warning)
                    break;
            }

            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MDRAID_IS_BEING_RESYNCHRONIZED, resync_warning);
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MDRAID_IS_DEGRADED, degraded_warning);
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MDRAID_INSUFFICIENT_STRIPE_CACHE, stripe_cache_warning);
        }
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        UInt64 corrected_errors = 0;
        UInt64 uncorrected_errors = 0;
        fs::path edac_dir("/sys/devices/system/edac/mc");
        if (fs::exists(edac_dir))
        {
            for (const auto & entry : fs::directory_iterator(edac_dir))
            {
                auto read_count = [&](const char * name) -> UInt64
                {
                    auto path = entry.path() / name;
                    if (!fs::exists(path))
                        return 0;
                    ReadBufferFromFile in(path.string());
                    UInt64 count = 0;
                    readText(count, in);
                    return count;
                };
                try
                {
                    corrected_errors += read_count("ce_count");
                    uncorrected_errors += read_count("ue_count");
                }
                catch (const std::exception &) // NOLINT(bugprone-empty-catch)
                {
                    /// One unreadable controller must not hide the counts of the others.
                }
            }
        }
        if (corrected_errors >= 100)
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_HIGH_CORRECTED_ECC_ERRORS_COUNT,
                PreformattedMessage::create(
                    "Memory controllers reported {} corrected ECC errors: a RAM module may be failing."
                    " Check /sys/devices/system/edac/mc/mc*/ce_count",
                    corrected_errors));
        if (uncorrected_errors > 0)
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_UNCORRECTED_ECC_ERRORS,
                PreformattedMessage::create(
                    "Memory controllers reported {} uncorrected ECC errors: memory contents were corrupted."
                    " The RAM module should be replaced. Check /sys/devices/system/edac/mc/mc*/ue_count",
                    uncorrected_errors));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        const char * filename = "/proc/sys/vm/zone_reclaim_mode";
        if (readNumber(filename) != 0)
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_ZONE_RECLAIM_MODE_ENABLED,
                PreformattedMessage::create(
                    "NUMA zone reclaim is enabled. It can cause severe latency spikes on multi-socket machines;"
                    " the recommended value is 0. Check {}", String(filename)));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        /// defrag = "always" causes allocation stalls even when THP is only enabled for madvise.
        const char * defrag_filename = "/sys/kernel/mm/transparent_hugepage/defrag";
        const char * enabled_filename = "/sys/kernel/mm/transparent_hugepage/enabled";
        if (readLine(defrag_filename).contains("[always]") && !readLine(enabled_filename).contains("[never]"))
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_TRANSPARENT_HUGEPAGES_DEFRAG_SET_TO_ALWAYS,
                PreformattedMessage::create(
                    "Linux transparent hugepage defragmentation is set to \"always\"."
                    " It can cause allocation stalls. Check {}", String(defrag_filename)));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        UInt64 throttle_events = 0;
        fs::path cpu_dir("/sys/devices/system/cpu");
        if (fs::exists(cpu_dir))
        {
            for (const auto & entry : fs::directory_iterator(cpu_dir))
            {
                const auto name = entry.path().filename().string();
                if (name.size() < 4 || !name.starts_with("cpu") || name[3] < '0' || name[3] > '9')
                    continue;
                auto throttle_path = entry.path() / "thermal_throttle" / "core_throttle_count";
                if (!fs::exists(throttle_path))
                    continue;
                try
                {
                    ReadBufferFromFile in(throttle_path.string());
                    UInt64 count = 0;
                    readText(count, in);
                    throttle_events += count;
                }
                catch (const std::exception &) // NOLINT(bugprone-empty-catch)
                {
                    /// One unreadable core must not hide the throttling of the others.
                }
            }
        }
        if (throttle_events > 0)
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_CPU_THERMAL_THROTTLING_DETECTED,
                PreformattedMessage::create(
                    "CPU cores reported {} thermal throttling events since boot. Performance can be degraded and unstable."
                    " Check the cooling of the machine.",
                    throttle_events));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        bool turbo_disabled = false;
        const char * intel_no_turbo = "/sys/devices/system/cpu/intel_pstate/no_turbo";
        const char * cpufreq_boost = "/sys/devices/system/cpu/cpufreq/boost";
        if (fs::exists(intel_no_turbo))
            turbo_disabled = readNumber(intel_no_turbo) == 1;
        else if (fs::exists(cpufreq_boost))
            turbo_disabled = readNumber(cpufreq_boost) == 0;
        if (turbo_disabled)
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_CPU_TURBO_BOOST_DISABLED,
                PreformattedMessage::create(
                    "CPU turbo frequency boost is disabled. Performance can be degraded."
                    " Check {} and {}", String(intel_no_turbo), String(cpufreq_boost)));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        /// The first line of /proc/swaps is a header; any further line is an active swap area.
        ReadBufferFromFile in("/proc/swaps");
        String header;
        readStringUntilNewlineInto(header, in);
        if (!in.eof())
            in.ignore();
        String first_swap_area;
        readStringUntilNewlineInto(first_swap_area, in);
        if (!first_swap_area.empty())
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_SWAP_IS_ENABLED,
                PreformattedMessage::create(
                    "Swap is enabled on the host. It can cause severe latency degradation under memory pressure."
                    " It is recommended to disable swap on ClickHouse servers. Check /proc/swaps"));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        std::string renameat2_message;
        if (!supportsAtomicRename(&renameat2_message))
            context->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_RENAMEAT2_UNAVAILABLE,
                PreformattedMessage::create(
                    "The kernel does not support the renameat2 system call ({})."
                    " Atomic operations such as EXCHANGE TABLES will not work."
                    " This check reads the kernel version only: EXCHANGE TABLES can also fail on a filesystem"
                    " without RENAME_EXCHANGE support, which is not detected here.", renameat2_message));
    }
    catch (const std::exception &) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        /// An empty data path has nothing to locate: `fs::canonical` then throws and the check below is skipped.
        const String canonical_data_path = fs::canonical(data_path).string();
        /// Only filesystems that are plausible durable homes for the data directory.
        const std::unordered_set<String> durable_fs_types = {"ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "f2fs"};

        String data_fs_type;
        String largest_fs_mount_point;
        UInt64 largest_fs_capacity = 0;
        size_t longest_mount_point_match = 0;
        std::unordered_set<String> seen_devices;

        ReadBufferFromFile mounts_file("/proc/self/mounts");
        while (!mounts_file.eof())
        {
            String line;
            readStringUntilNewlineInto(line, mounts_file);
            if (!mounts_file.eof())
                mounts_file.ignore();

            /// Fields: device, mount point, filesystem type. Skip mount points with escaped characters.
            size_t p1 = line.find(' ');
            size_t p2 = (p1 == String::npos) ? String::npos : line.find(' ', p1 + 1);
            size_t p3 = (p2 == String::npos) ? String::npos : line.find(' ', p2 + 1);
            if (p3 == String::npos)
                continue;
            String device = line.substr(0, p1);
            String mount_point = line.substr(p1 + 1, p2 - p1 - 1);
            String fs_type = line.substr(p2 + 1, p3 - p2 - 1);
            if (mount_point.contains('\\'))
                continue;

            bool contains_data_path = canonical_data_path == mount_point
                || (canonical_data_path.starts_with(mount_point)
                    && (mount_point == "/" || canonical_data_path[mount_point.size()] == '/'));
            if (contains_data_path && mount_point.size() >= longest_mount_point_match)
            {
                longest_mount_point_match = mount_point.size();
                data_fs_type = fs_type;
            }

            if (durable_fs_types.contains(fs_type) && seen_devices.insert(device).second)
            {
                std::error_code ec;
                auto space = fs::space(mount_point, ec);
                if (!ec && space.capacity > largest_fs_capacity)
                {
                    largest_fs_capacity = space.capacity;
                    largest_fs_mount_point = mount_point;
                }
            }
        }

        if (data_fs_type == "overlay")
            context->addOrUpdateWarningMessage(
                Context::WarningType::DATA_PATH_ON_OVERLAY_FS,
                PreformattedMessage::create(
                    "The <path> directory {} is located on an overlay filesystem, which reduces I/O performance."
                    " If this is the writable layer of a container, what is stored there will also be lost when the container is removed;"
                    " mount a volume for it instead. Tables on other configured disks are not covered by this check.",
                    String(data_path)));

        std::error_code ec;
        auto data_space = fs::space(canonical_data_path, ec);
        /// Require a 2x difference so that volumes of comparable size don't trigger the warning.
        if (!ec && largest_fs_capacity > 0 && static_cast<UInt64>(data_space.capacity) * 2 < largest_fs_capacity)
            context->addOrUpdateWarningMessage(
                Context::WarningType::DATA_PATH_NOT_ON_LARGEST_FILESYSTEM,
                PreformattedMessage::create(
                    "The <path> directory {} is on a filesystem of size {}, while a much larger filesystem ({}) is mounted at {}."
                    " Make sure it is on the intended volume. Tables on other configured disks are not covered by this check.",
                    String(data_path),
                    formatReadableSizeWithBinarySuffix(data_space.capacity),
                    formatReadableSizeWithBinarySuffix(largest_fs_capacity),
                    largest_fs_mount_point));
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
