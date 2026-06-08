#pragma once

#include <base/types.h>
#include <Common/Stopwatch.h>

#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <sys/types.h>


namespace DB
{

/** Per-invocation resource accounting for executable UDF calls.
  *
  * Created by `UserDefinedExecutableFunctionFactory::executeImpl` and passed
  * via `ShellCommandSourceConfiguration` into the borrow/IO machinery. The
  * factory queries the accumulators after the pipeline drains and feeds them
  * into ProfileEvents.
  *
  * Two paths share this class:
  *
  * Pool path (`executable_pool`): a persistent worker process is borrowed from
  * a pool for the duration of one UDF invocation. CPU and memory are measured
  * by sampling `/proc/<pid>/{stat,status}` for the worker and all descendants.
  * The pre-snapshot zeroes `VmHWM` via `/proc/<pid>/clear_refs` (mode 5) so
  * `VmHWM` at release reflects the peak observed during this borrow, not the
  * worker's lifetime peak. Lifecycle callbacks:
  *   ctor                    -- starts the wall clock for pool-wait measurement
  *   recordPoolWaitDone      -- `tryBorrowObject` returned (success OR timeout)
  *   recordPidAcquired       -- `buildCommand` returned; pid known (success only)
  *   recordInputBytes / recordOutputBytes -- IO buffers report bytes pushed
  *   recordReleased          -- ShellCommandSource cleanup, before returnObject
  *
  * Executable path (non-pool): a fresh child is spawned per invocation. CPU
  * and peak RSS come from `wait4` rusage captured by `ShellCommand::tryWaitImpl`.
  * Elapsed wall time is always recorded; CPU and peak-RSS are recorded only when
  * the child was reaped and rusage is available. Lifecycle callbacks:
  *   ctor                    -- starts the wall clock for elapsed measurement
  *   recordInputBytes / recordOutputBytes -- IO buffers report bytes pushed
  *   recordExecutableElapsed -- ShellCommandSource cleanup; stamps elapsed_us
  *   recordExecutableFinished -- additionally records CPU and peak-RSS from rusage
  *
  * Procfs failures (ENOENT after a worker died, EACCES, malformed content) are
  * silently skipped on the pool path: CPU/memory increments are dropped but the
  * other events still fire.
  */
class UDFProcessSubtreeSampler
{
public:
    UDFProcessSubtreeSampler();

    void recordPoolWaitDone();
    void recordPidAcquired(pid_t root_pid);
    void recordInputBytes(size_t bytes) noexcept;
    void recordOutputBytes(size_t bytes) noexcept;
    void recordReleased();

    /// Executable (non-pool) path: stamp `elapsed_us` from the wall clock at
    /// `ShellCommandSource` cleanup. Safe to call even when the child was never
    /// reaped — elapsed is always meaningful. Idempotent; a second call is a no-op.
    void recordExecutableElapsed() noexcept;

    /// Executable (non-pool) path: fill `user_time_us`, `system_time_us`, and
    /// `peak_memory_byte_seconds` from the scalars returned by `ShellCommand::getChild*`.
    /// Calls `recordExecutableElapsed` if it has not been called yet. Idempotent.
    /// Coverage note: these come from `wait4` rusage, which — unlike pool mode's /proc
    /// subtree walk — counts the child plus the descendants it reaps (CPU summed, peak
    /// RSS maxed), but not descendants it never waits on.
    void recordExecutableFinished(UInt64 user_time_us, UInt64 system_time_us, UInt64 peak_rss_bytes) noexcept;

    /// Pool wait = entry → borrow acquired.
    /// Zero if the borrow never happened (caller should still report 0).
    UInt64 getPoolWaitMicroseconds() const noexcept { return pool_wait_us; }

    /// Pool mode: borrow acquired → released (excludes the pool-wait interval).
    /// Executable mode: ctor → ShellCommandSource cleanup (includes spawn, output parsing and IO).
    UInt64 getElapsedMicroseconds() const noexcept { return elapsed_us; }

    UInt64 getUserTimeMicroseconds() const noexcept { return user_time_us; }
    UInt64 getSystemTimeMicroseconds() const noexcept { return system_time_us; }
    UInt64 getPeakMemoryByteSeconds() const noexcept { return peak_memory_byte_seconds; }
    UInt64 getInputBytes() const noexcept { return input_bytes.load(std::memory_order_relaxed); }
    UInt64 getOutputBytes() const noexcept { return output_bytes.load(std::memory_order_relaxed); }

    bool poolWaitDone() const noexcept { return pool_wait_done; }
    bool borrowAcquired() const noexcept { return borrow_acquired; }
    bool executableFinished() const noexcept { return executable_finished; }

    /// Whether at least one pid in this borrow's subtree saw the
    /// corresponding procfs operation fail. Caller-side state can use these
    /// to log once per UDF on first failure so operators have a signal when
    /// a sandbox / seccomp profile silently degrades metrics to zero.
    bool clearRefsFailedAnyPid() const noexcept { return clear_refs_failed_any; }
    bool readStatFailedAnyPid() const noexcept { return read_stat_failed_any; }
    bool readPeakRssFailedAnyPid() const noexcept { return read_peak_rss_failed_any; }

    /// Whether `walkSubtree` hit the `MAX_PIDS` cap during this borrow and
    /// a candidate pid had to be dropped. When true, the subtree enumeration
    /// is incomplete, so CPU and `PeakMemoryByteSeconds` will under-count
    /// the unvisited descendants.
    bool subtreeWalkTruncated() const noexcept { return subtree_truncated_any; }

private:
    Stopwatch entry_watch;
    Stopwatch borrow_watch;

    pid_t root_pid = -1;
    bool pool_wait_done = false;
    bool borrow_acquired = false;
    bool executable_finished = false;
    bool executable_rusage_recorded = false;

    UInt64 pool_wait_us = 0;
    UInt64 elapsed_us = 0;
    UInt64 user_time_us = 0;
    UInt64 system_time_us = 0;
    UInt64 peak_memory_byte_seconds = 0;

    bool clear_refs_failed_any = false;
    bool read_stat_failed_any = false;
    bool read_peak_rss_failed_any = false;
    bool subtree_truncated_any = false;

    std::atomic<UInt64> input_bytes{0};
    std::atomic<UInt64> output_bytes{0};

    struct PreSnapshot
    {
        UInt64 utime_us = 0;
        UInt64 stime_us = 0;
    };
    std::unordered_map<pid_t, PreSnapshot> pre_snapshot;
    /// Every pid we observed during the pre-walk, regardless of whether
    /// `readStat` succeeded for it. Used at post-walk to distinguish a
    /// pid that the pre-walk failed to baseline (skip — no delta possible)
    /// from a pid spawned during the borrow (count the entire post value).
    std::unordered_set<pid_t> pre_walk_pids;
};


/// Free helpers for /proc parsing. On non-Linux platforms they are no-ops
/// that return empty / false.
///
/// They return false for *expected* procfs failures (the pid vanished, a
/// seccomp profile denies `open`, malformed contents), but they are NOT
/// `noexcept`: a memory-limit `exception` thrown while allocating a
/// `/proc/<pid>` path string is allowed to propagate. Every caller
/// (`recordPidAcquired`, `recordReleased`) runs them inside a try/catch, so
/// such an `exception` is logged and sampling is skipped, instead of hitting
/// `std::terminate` at a `noexcept` boundary.
namespace UDFProcfs
{
    /// Recursively enumerate the root pid plus every descendant by walking
    /// `/proc/<pid>/task/<tid>/children`. Returns at least {root_pid} when
    /// procfs is unavailable. `truncated` is set to true if a fresh
    /// candidate pid was seen after the internal `MAX_PIDS` cap had been
    /// reached, in which case the returned vector is incomplete.
    std::vector<pid_t> walkSubtree(pid_t root_pid, bool & truncated);

    /// Write "5\n" to /proc/<pid>/clear_refs to reset VmHWM. Returns
    /// false on open/write failure (e.g. seccomp denies `open` on
    /// `/proc`, or the pid disappeared between walkSubtree and here).
    bool clearRefs(pid_t pid);

    /// Parse user and system CPU time from /proc/<pid>/stat, converting clock
    /// ticks to microseconds. `utime_us` sums fields 14 (`utime`, this pid's
    /// own user CPU) and 16 (`cutime`, user CPU of children this pid has
    /// reaped). `stime_us` sums fields 15 (`stime`) and 17 (`cstime`). Reading
    /// cutime/cstime is necessary because a short-lived helper (e.g. python
    /// `subprocess.run`) finishes and is `waitpid`-ed before the post-walk
    /// even sees it; without those two fields the helper's CPU is invisible.
    bool readStat(pid_t pid, UInt64 & utime_us, UInt64 & stime_us);

    /// Parse VmHWM from /proc/<pid>/status, converted to bytes.
    bool readPeakRss(pid_t pid, UInt64 & bytes);
}

}
