#include <Common/UDFProcessSubtreeSampler.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <base/arithmeticOverflow.h>
#include <base/scope_guard.h>

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>


namespace DB
{

#if defined(OS_LINUX)
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
#endif


namespace UDFProcfs
{

std::vector<pid_t> walkSubtree(pid_t root_pid, bool & truncated)
{
    truncated = false;
    std::vector<pid_t> result;
    if (root_pid <= 0)
        return result;

    result.push_back(root_pid);

#if defined(OS_LINUX)
    /// Iterative DFS: for every pid in `result`, append the union of
    /// `/proc/<pid>/task/<tid>/children` lists. A bounded depth limit guards
    /// against pathological /proc states; UDFs in practice nest only a few
    /// levels deep (a wrapper script may spawn the actual interpreter).
    constexpr size_t MAX_PIDS = 1024;

    for (size_t i = 0; i < result.size() && result.size() < MAX_PIDS; ++i)
    {
        const pid_t pid = result[i];
        const std::string task_dir = "/proc/" + std::to_string(pid) + "/task";

        DIR * dir = ::opendir(task_dir.c_str());
        if (!dir)
            continue;

        /// Release the handle on every path, including an exception thrown while
        /// building `children_path` or opening the `ifstream` under a memory limit;
        /// otherwise the swallowed sample failure would leak a directory fd.
        SCOPE_EXIT(::closedir(dir));

        struct dirent * entry = nullptr;
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
                /// Check the size cap BEFORE pushing so we can distinguish
                /// "we filled to exactly MAX_PIDS without seeing more" (no
                /// truncation) from "we saw another unique candidate but had
                /// no room" (truncation).
                if (result.size() >= MAX_PIDS)
                {
                    truncated = true;
                    break;
                }
                result.push_back(child);
            }
            if (result.size() >= MAX_PIDS)
                break;
        }
    }
#endif

    return result;
}


bool clearRefs(pid_t pid)
{
    if (pid <= 0)
        return false;

#if defined(OS_LINUX)
    const std::string path = "/proc/" + std::to_string(pid) + "/clear_refs";
    int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
    if (fd == -1)
        return false;
    const char data[] = "5\n";
    ssize_t written = ::write(fd, data, sizeof(data) - 1);
    [[maybe_unused]] int close_err = ::close(fd);
    return written == sizeof(data) - 1;
#else
    (void)pid;
    return false;
#endif
}


bool readStat(pid_t pid, UInt64 & utime_us, UInt64 & stime_us)
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
    /// stime is field 15, cutime is field 16, cstime is field 17 — skip 11
    /// fields to land on utime, then read four consecutive integers.
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
    UInt64 cutime_ticks = 0;
    UInt64 cstime_ticks = 0;
    if (!tryReadIntText(utime_ticks, rest))
        return false;
    skipWhitespaceIfAny(rest);
    if (!tryReadIntText(stime_ticks, rest))
        return false;
    skipWhitespaceIfAny(rest);
    if (!tryReadIntText(cutime_ticks, rest))
        return false;
    skipWhitespaceIfAny(rest);
    if (!tryReadIntText(cstime_ticks, rest))
        return false;

    /// Sum reaped children's CPU into the parent's totals. A short-lived
    /// helper (e.g. python `subprocess.run` or a one-shot worker) finishes
    /// and is reaped well before the post-walk runs, so its own `/proc/<tid>`
    /// has already vanished; the only place its CPU still exists is in the
    /// parent's c{u,s}time.
    utime_us = ticksToMicroseconds(utime_ticks + cutime_ticks);
    stime_us = ticksToMicroseconds(stime_ticks + cstime_ticks);
    return true;
#else
    return false;
#endif
}


#if defined(OS_LINUX)
namespace
{
    /// Parse a kB-valued line (e.g. "VmHWM:    123 kB") from /proc/<pid>/status.
    bool readStatusFieldKiB(pid_t pid, std::string_view field, UInt64 & bytes)
    {
        const std::string path = "/proc/" + std::to_string(pid) + "/status";
        std::ifstream in(path);
        if (!in.is_open())
            return false;

        std::string line;
        while (std::getline(in, line))
        {
            if (!line.starts_with(field))
                continue;

            const std::string value_str = line.substr(field.size());
            ReadBufferFromString parser(value_str);
            skipWhitespaceIfAny(parser);
            UInt64 kib = 0;
            if (!tryReadIntText(kib, parser))
                return false;
            bytes = kib * 1024ULL;
            return true;
        }
        return false;
    }
}
#endif


bool readPeakRss(pid_t pid, UInt64 & bytes)
{
    bytes = 0;
    if (pid <= 0)
        return false;

#if defined(OS_LINUX)
    return readStatusFieldKiB(pid, "VmHWM:", bytes);
#else
    return false;
#endif
}


bool readCurrentRss(pid_t pid, UInt64 & bytes)
{
    bytes = 0;
    if (pid <= 0)
        return false;

#if defined(OS_LINUX)
    return readStatusFieldKiB(pid, "VmRSS:", bytes);
#else
    return false;
#endif
}


bool isZombie(pid_t pid)
{
    if (pid <= 0)
        return false;

#if defined(OS_LINUX)
    std::ifstream in("/proc/" + std::to_string(pid) + "/stat");
    std::string line;
    if (!std::getline(in, line))
        return false;

    /// `/proc/<pid>/stat` is `pid (comm) S ...`; `comm` can contain spaces and
    /// ')', so anchor on the last ") ".
    const auto pos = line.rfind(") ");
    if (pos == std::string::npos || pos + 2 >= line.size())
        return false;

    return line[pos + 2] == 'Z';
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
    pre_snapshot.clear();
    pre_walk_pids.clear();

    if (root_pid <= 0)
        return;

    /// Pre-snapshot: walk subtree, snap VmHWM down to current RSS per pid
    /// via /proc/<pid>/clear_refs (mode 5), capture utime/stime baselines.
    /// clearRefs failure is silent — VmHWM keeps the worker's lifetime peak,
    /// which may inflate the reported peak_rss by an arbitrary amount but
    /// remains a correct upper bound on this borrow's peak.
    bool walk_truncated = false;
    auto pids = UDFProcfs::walkSubtree(root_pid, walk_truncated);
    if (walk_truncated)
        subtree_truncated_any.store(true, std::memory_order_relaxed);
    for (pid_t pid : pids)
    {
        pre_walk_pids.insert(pid);
        if (!UDFProcfs::clearRefs(pid))
            clear_refs_failed_any = true;
        UInt64 utime_us = 0;
        UInt64 stime_us = 0;
        if (UDFProcfs::readStat(pid, utime_us, stime_us))
            pre_snapshot[pid] = PreSnapshot{utime_us, stime_us};
        else
            read_stat_failed_any = true;
    }

    /// Mark the borrow acquired only after the pre-snapshot is fully built.
    /// `walkSubtree` and `readStat` allocate and may throw a memory-limit
    /// `exception`; the caller catches it, but if `borrow_acquired` were set
    /// up front, `recordReleased` would then run against an empty or partial
    /// baseline and charge the worker's whole lifetime CPU to this borrow.
    borrow_acquired = true;
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

    /// For peak memory we take the max `VmHWM` observed across the subtree
    /// rather than a sum. We can't claim anything about whether descendants
    /// run serially or concurrently — that's entirely up to the UDF — so
    /// summing per-pid peaks would only be meaningful if peaks coincided in
    /// time, and we have no way to know that. The max is a real number on
    /// its own: it bounds the largest resident set the borrow ever pinned
    /// on any single pid. Trade-off: when a UDF runs descendants truly
    /// concurrently with non-overlapping peaks, this under-reports the
    /// aggregate residency.
    ///
    /// Sum pre CPU values over the FULL pre_snapshot map, not just over pids
    /// that are still alive at post-walk time. A descendant present at
    /// pre-walk and reaped during the borrow leaves its CPU in the reaper's
    /// `c{u,s}time` delta; without subtracting that descendant's pre
    /// baseline, all of its pre-window CPU would leak into the borrow's
    /// reported delta.
    UInt64 pre_utime_sum = 0;
    UInt64 pre_stime_sum = 0;
    for (const auto & [_, snap] : pre_snapshot)
    {
        pre_utime_sum += snap.utime_us;
        pre_stime_sum += snap.stime_us;
    }

    bool walk_truncated = false;
    auto pids = UDFProcfs::walkSubtree(root_pid, walk_truncated);
    if (walk_truncated)
        subtree_truncated_any.store(true, std::memory_order_relaxed);

    UInt64 post_utime_sum = 0;
    UInt64 post_stime_sum = 0;
    UInt64 peak_rss = 0;

    /// Three-bucket dispatch over post-walk pids:
    ///   1. pid ∈ pre_snapshot  →  delta = post − pre. Common case for the
    ///      root and any persistent descendants.
    ///   2. pid ∈ pre_walk_pids \ pre_snapshot  →  skip. We saw this pid at
    ///      pre-walk but `readStat` failed for it, so we have no baseline to
    ///      subtract; counting the post value would attribute the pid's
    ///      entire lifetime CPU to this borrow.
    ///   3. pid ∉ pre_walk_pids  →  truly new (spawned during the borrow,
    ///      e.g. a lazily initialised `multiprocessing.Pool` worker). The
    ///      pid's pre-borrow CPU is zero by definition, so the entire post
    ///      value counts.
    for (pid_t pid : pids)
    {
        UInt64 utime_us = 0;
        UInt64 stime_us = 0;
        bool stat_ok = UDFProcfs::readStat(pid, utime_us, stime_us);
        if (!stat_ok)
            read_stat_failed_any = true;

        if (stat_ok)
        {
            if (pre_snapshot.contains(pid))
            {
                post_utime_sum += utime_us;
                post_stime_sum += stime_us;
            }
            else if (!pre_walk_pids.contains(pid))
            {
                post_utime_sum += utime_us;
                post_stime_sum += stime_us;
            }
        }

        UInt64 hwm_bytes = 0;
        if (UDFProcfs::readPeakRss(pid, hwm_bytes))
            peak_rss = std::max(peak_rss, hwm_bytes);
        else
            read_peak_rss_failed_any.store(true, std::memory_order_relaxed);
    }

    if (post_utime_sum >= pre_utime_sum)
        user_time_us = post_utime_sum - pre_utime_sum;
    if (post_stime_sum >= pre_stime_sum)
        system_time_us = post_stime_sum - pre_stime_sum;

    /// Why byte-seconds rather than peak bytes? PeakBytes is not additive
    /// across UDF invocations within a single query — summing per-borrow
    /// peaks would be meaningless (two borrows that each hit 1 GiB at
    /// different times don't add up to 2 GiB of memory pressure).
    /// PeakByteSeconds (peak_rss × wall_time) IS additive: it measures
    /// memory-time area, which sums cleanly across borrows in the
    /// query-level ProfileEvent aggregate.
    ///
    /// PeakMemoryByteSeconds = peak_rss × borrow_wall_seconds. Stored as
    /// integer byte-seconds. 64-bit multiplication is fine for any realistic
    /// UDF (1 GiB × 100 s ≈ 10^14, well below 2^64); pathological inputs
    /// above ~1.8e19 byte-microseconds would overflow before the divide.
    /// Use `common::mulOverflow` so UBSan does not flag the expected
    /// wrap-around, and clamp to UInt64::max byte-seconds on overflow.
    UInt64 product = 0;
    if (common::mulOverflow(peak_rss, elapsed_us, product))
        peak_memory_byte_seconds = std::numeric_limits<UInt64>::max();
    else
        peak_memory_byte_seconds = product / 1000000ULL;
}


void UDFProcessSubtreeSampler::recordExecutablePid(pid_t root_pid_) noexcept
{
    executable_root_pid = root_pid_;
}


void UDFProcessSubtreeSampler::sampleExecutablePeak([[maybe_unused]] bool is_final) noexcept
{
    if (executable_root_pid <= 0)
        return;

#if defined(OS_LINUX)
    /// Called on every IO buffer fill; a 4 KiB read buffer would otherwise trigger
    /// a subtree walk per fill while streaming large output. VmHWM is monotonic, so
    /// the running max survives sparse sampling: throttle to at most one walk per
    /// ~5 ms. The first call always samples (last_sample_us == 0) so a
    /// short-lived child is measured at least once. The final (EOF) call bypasses
    /// the throttle to ensure the tail sample always executes.
    if (!is_final)
    {
        static constexpr UInt64 sample_interval_us = 5000;

        const UInt64 now_us = entry_watch.elapsedMicroseconds();
        UInt64 last = last_sample_us.load(std::memory_order_relaxed);
        if (last != 0 && now_us < last + sample_interval_us)
            return;
        if (!last_sample_us.compare_exchange_strong(last, now_us, std::memory_order_relaxed))
            return;
    }

    /// walkSubtree and readPeakRss allocate (path strings, ifstream); an exception
    /// thrown under a memory limit must not cross this noexcept boundary.
    try
    {
        bool walk_truncated = false;
        const auto pids = UDFProcfs::walkSubtree(executable_root_pid, walk_truncated);
        if (walk_truncated)
            subtree_truncated_any.store(true, std::memory_order_relaxed);

        for (pid_t pid : pids)
        {
            UInt64 hwm_bytes = 0;
            if (!UDFProcfs::readPeakRss(pid, hwm_bytes))
            {
                /// A read can fail because the pid exited since `walkSubtree` (a
                /// short-lived child, or the root after it closed stdout) or because
                /// a sandbox denies `/proc`. The executable path cannot tell these
                /// apart — the child's lifetime is not under our control — so it does
                /// not raise the degradation flag here, unlike the pool path where the
                /// borrowed worker is provably alive during sampling.
                continue;
            }

            UInt64 current = executable_peak_rss_bytes.load(std::memory_order_relaxed);
            while (hwm_bytes > current)
            {
                if (executable_peak_rss_bytes.compare_exchange_weak(
                        current, hwm_bytes,
                        std::memory_order_relaxed, std::memory_order_relaxed))
                    break;
            }
        }
    }
    catch (...)
    {
        /// Dropping a sample is ok: the peak is a best-effort monotonic high-water mark.
        return;
    }
#endif
}


void UDFProcessSubtreeSampler::recordExecutableElapsed() noexcept
{
    if (executable_finished)
        return;

    /// Wall time from sampler construction to `ShellCommandSource` cleanup (includes
    /// spawn, output parsing, and IO). The executable path spawns a fresh child per
    /// invocation, so there is no pool-wait interval to subtract.
    elapsed_us = entry_watch.elapsedMicroseconds();

    /// Peak RSS comes from /proc VmHWM sampled during IO into executable_peak_rss_bytes.
    /// The send-data threads are joined before cleanup reaches this point, and the
    /// read side ran on this same (cleanup) thread, so the relaxed load sees all
    /// sampled writes.
    const UInt64 peak_rss_bytes = executable_peak_rss_bytes.load(std::memory_order_relaxed);

    UInt64 product = 0;
    if (common::mulOverflow(peak_rss_bytes, elapsed_us, product))
        peak_memory_byte_seconds = std::numeric_limits<UInt64>::max();
    else
        peak_memory_byte_seconds = product / 1000000ULL;

    executable_finished = true;
}


void UDFProcessSubtreeSampler::recordExecutableFinished(
    UInt64 user_time_us_, UInt64 system_time_us_) noexcept
{
    if (executable_rusage_recorded)
        return;

    /// Stamp elapsed first; `recordExecutableElapsed` is idempotent so calling
    /// it here is safe even if the caller already invoked it.
    recordExecutableElapsed();

    user_time_us = user_time_us_;
    system_time_us = system_time_us_;

    executable_rusage_recorded = true;
}

}
