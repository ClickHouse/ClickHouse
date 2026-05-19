#include <Common/UDFProcessSubtreeSampler.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <string>


namespace DB
{

namespace
{
    UInt64 clockTicksPerSecond() noexcept
    {
        static const UInt64 ticks = []
        {
            Int64 t = ::sysconf(_SC_CLK_TCK);
            return t > 0 ? static_cast<UInt64>(t) : UInt64{100};
        }();
        return ticks;
    }

    UInt64 ticksToMicroseconds(UInt64 ticks) noexcept
    {
        return ticks * 1000000ULL / clockTicksPerSecond();
    }
}


namespace UDFProcfs
{

std::vector<pid_t> walkSubtree(pid_t root_pid)
{
    std::vector<pid_t> result;
    if (root_pid <= 0)
        return result;

    result.push_back(root_pid);

#if defined(OS_LINUX)
    /// Iterative DFS: for every pid in `result`, append the union of
    /// `/proc/<pid>/task/<tid>/children` lists. A bounded depth limit guards
    /// against pathological /proc states; UDFs in practice are 3 levels deep
    /// (execute.sh -> nsjail -> python).
    constexpr size_t MAX_PIDS = 1024;

    for (size_t i = 0; i < result.size() && result.size() < MAX_PIDS; ++i)
    {
        const pid_t pid = result[i];
        const std::string task_dir = "/proc/" + std::to_string(pid) + "/task";

        DIR * dir = ::opendir(task_dir.c_str());
        if (!dir)
            continue;

        struct dirent * entry;
        /// NOLINTNEXTLINE(concurrency-mt-unsafe) -- `dir` is a local `DIR *` not shared across threads.
        while ((entry = ::readdir(dir)) != nullptr)
        {
            const char * name = entry->d_name;
            if (name[0] == '.')
                continue;
            /// `name` is a thread id (`tid`) under the process.
            const std::string children_path = task_dir + '/' + name + "/children";
            std::ifstream in(children_path);
            if (!in.is_open())
                continue;

            pid_t child = 0;
            while (in >> child)
            {
                if (child <= 0)
                    continue;
                if (std::find(result.begin(), result.end(), child) != result.end())
                    continue;
                result.push_back(child);
                if (result.size() >= MAX_PIDS)
                    break;
            }
            if (result.size() >= MAX_PIDS)
                break;
        }

        ::closedir(dir);
    }
#endif

    return result;
}


void clearRefs(pid_t pid) noexcept
{
    if (pid <= 0)
        return;

#if defined(OS_LINUX)
    const std::string path = "/proc/" + std::to_string(pid) + "/clear_refs";
    int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
    if (fd == -1)
        return;
    const char data[] = "5\n";
    ssize_t written = ::write(fd, data, sizeof(data) - 1);
    (void)written;
    [[maybe_unused]] int close_err = ::close(fd);
#else
    (void)pid;
#endif
}


bool readStat(pid_t pid, UInt64 & utime_us, UInt64 & stime_us) noexcept
{
    utime_us = 0;
    stime_us = 0;
    if (pid <= 0)
        return false;

#if defined(OS_LINUX)
    const std::string path = "/proc/" + std::to_string(pid) + "/stat";
    std::ifstream in(path);
    if (!in.is_open())
        return false;

    std::string line;
    if (!std::getline(in, line))
        return false;

    /// `comm` (field 2) is parenthesised and may contain spaces and even
    /// parentheses, so locate the last ')' to anchor parsing.
    auto close_paren = line.rfind(')');
    if (close_paren == std::string::npos || close_paren + 1 >= line.size())
        return false;

    const std::string rest_str = line.substr(close_paren + 1);
    ReadBufferFromString rest(rest_str);
    /// After comm, the next token is state (field 3). utime is field 14,
    /// stime is field 15 — skip 11 fields to land on utime.
    for (int i = 0; i < 11; ++i)
    {
        skipWhitespaceIfAny(rest);
        skipStringUntilWhitespace(rest);
        if (rest.eof())
            return false;
    }
    skipWhitespaceIfAny(rest);

    UInt64 utime_ticks = 0;
    UInt64 stime_ticks = 0;
    if (!tryReadIntText(utime_ticks, rest))
        return false;
    skipWhitespaceIfAny(rest);
    if (!tryReadIntText(stime_ticks, rest))
        return false;

    utime_us = ticksToMicroseconds(utime_ticks);
    stime_us = ticksToMicroseconds(stime_ticks);
    return true;
#else
    return false;
#endif
}


bool readPeakRss(pid_t pid, UInt64 & bytes) noexcept
{
    bytes = 0;
    if (pid <= 0)
        return false;

#if defined(OS_LINUX)
    const std::string path = "/proc/" + std::to_string(pid) + "/status";
    std::ifstream in(path);
    if (!in.is_open())
        return false;

    std::string line;
    while (std::getline(in, line))
    {
        if (!line.starts_with("VmHWM:"))
            continue;

        const std::string value_str = line.substr(6);
        ReadBufferFromString parser(value_str);
        skipWhitespaceIfAny(parser);
        UInt64 kib = 0;
        if (!tryReadIntText(kib, parser))
            return false;
        bytes = kib * 1024ULL;
        return true;
    }
    return false;
#else
    return false;
#endif
}

}


UDFProcessSubtreeSampler::UDFProcessSubtreeSampler() = default;


void UDFProcessSubtreeSampler::recordPoolWaitDone()
{
    pool_wait_us = entry_watch.elapsedMicroseconds();
    borrow_watch.restart();
    pool_wait_done = true;
}


void UDFProcessSubtreeSampler::recordPidAcquired(pid_t root_pid_)
{
    root_pid = root_pid_;
    borrow_acquired = true;
    pre_snapshot.clear();

    if (root_pid <= 0)
        return;

    /// Pre-snapshot: walk subtree, reset VmHWM per pid, capture utime/stime
    /// baselines. clearRefs failure is silent — peak_rss for that pid will be
    /// reported as the kernel's lifetime peak, which over-counts slightly but
    /// is correct as an upper bound.
    auto pids = UDFProcfs::walkSubtree(root_pid);
    for (pid_t pid : pids)
    {
        UDFProcfs::clearRefs(pid);
        UInt64 utime_us = 0;
        UInt64 stime_us = 0;
        if (UDFProcfs::readStat(pid, utime_us, stime_us))
            pre_snapshot[pid] = PreSnapshot{utime_us, stime_us};
    }
}


void UDFProcessSubtreeSampler::recordInputBytes(size_t bytes) noexcept
{
    if (bytes == 0)
        return;
    input_bytes.fetch_add(bytes, std::memory_order_relaxed);
}


void UDFProcessSubtreeSampler::recordOutputBytes(size_t bytes) noexcept
{
    if (bytes == 0)
        return;
    output_bytes.fetch_add(bytes, std::memory_order_relaxed);
}


void UDFProcessSubtreeSampler::recordReleased()
{
    if (!borrow_acquired)
        return;

    elapsed_us = borrow_watch.elapsedMicroseconds();

    if (root_pid <= 0)
        return;

    /// Post-snapshot: re-walk because the subtree may have grown (e.g. python
    /// launching a helper). Only pids that have a pre-snapshot contribute to
    /// the CPU delta — pids that appear after the pre-walk are ignored. This
    /// slightly under-attributes a borrow that spawns brand-new descendants,
    /// but avoids the catastrophic over-attribution that would otherwise
    /// happen if `readStat` failed during the pre-walk for a warm worker:
    /// the worker's lifetime-cumulative `utime/stime` would land in the post
    /// sum with no baseline to subtract. Conservative under-attribution is
    /// always preferable to silent over-attribution.
    ///
    /// For peak memory we take the max VmHWM observed across the subtree
    /// because pids run mostly serially inside one borrow (nsjail+exec
    /// chain) so peak resident set per pid is a better estimate than a sum.
    auto pids = UDFProcfs::walkSubtree(root_pid);

    UInt64 post_utime_sum_in_baseline = 0;
    UInt64 post_stime_sum_in_baseline = 0;
    UInt64 pre_utime_sum = 0;
    UInt64 pre_stime_sum = 0;
    UInt64 peak_rss = 0;

    for (pid_t pid : pids)
    {
        auto it = pre_snapshot.find(pid);
        if (it != pre_snapshot.end())
        {
            UInt64 utime_us = 0;
            UInt64 stime_us = 0;
            if (UDFProcfs::readStat(pid, utime_us, stime_us))
            {
                post_utime_sum_in_baseline += utime_us;
                post_stime_sum_in_baseline += stime_us;
                pre_utime_sum += it->second.utime_us;
                pre_stime_sum += it->second.stime_us;
            }
        }

        UInt64 hwm_bytes = 0;
        if (UDFProcfs::readPeakRss(pid, hwm_bytes))
            peak_rss = std::max(peak_rss, hwm_bytes);
    }

    if (post_utime_sum_in_baseline >= pre_utime_sum)
        user_time_us = post_utime_sum_in_baseline - pre_utime_sum;
    if (post_stime_sum_in_baseline >= pre_stime_sum)
        system_time_us = post_stime_sum_in_baseline - pre_stime_sum;

    /// PeakMemoryByteSeconds = peak_rss × borrow_wall_seconds.
    /// Stored as integer byte-seconds. 64-bit multiplication is fine for any
    /// realistic UDF (1 GiB × 100 s ≈ 10^14, well below 2^64); pathological
    /// inputs above ~1.8e19 byte-microseconds would overflow before the
    /// divide.
    peak_memory_byte_seconds = peak_rss * elapsed_us / 1000000ULL;
}

}
