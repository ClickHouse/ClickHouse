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
  * Executable path (non-pool): a fresh child is spawned per invocation via
  * `vfork`+`exec`. Peak RSS is measured by sampling `/proc/<pid>/VmHWM` across
  * the child's subtree during IO (not `wait4 ru_maxrss`, which reports the
  * parent's RSS for a `vfork`+`exec` child because the kernel folds the pre-exec
  * mm's RSS into `signal->maxrss` at exec). CPU still comes from `wait4` rusage.
  * Lifecycle callbacks:
  *   ctor                    -- starts the wall clock for elapsed measurement
  *   recordExecutablePid     -- process created; stores pid for subtree sampling
  *   recordInputBytes / recordOutputBytes -- IO buffers report bytes pushed
  *   sampleExecutablePeak    -- called on each IO read and input write (child
  *                              provably alive), plus one best-effort tail sample
  *                              at stdout EOF (child may have already exited);
  *                              keeps a running max of VmHWM across the subtree
  *   recordExecutableElapsed -- ShellCommandSource cleanup; stamps elapsed_us
  *                              and computes peak_memory_byte_seconds
  *   recordExecutableFinished -- records CPU from wait4 rusage
  *
  * Procfs failures (ENOENT after a worker died, EACCES, malformed content) are
  * silently skipped on both the pool and executable paths: CPU/memory increments
  * are dropped but the other events still fire. On the pool path, a failed read
  * for the root pid signals genuine degradation (e.g. seccomp blocking `/proc`)
  * because the borrowed worker is provably alive during sampling. On the
  * executable path, `readPeakRss` failures are never flagged as degradation:
  * the child's lifetime is not under our control and a root read failure is
  * indistinguishable from a legitimately exited short-lived UDF.
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

    /// Executable (non-pool) path: store the child pid for subtree sampling.
    /// Called immediately after the child process is created. Does not perform
    /// any /proc I/O — the child's VmHWM starts at zero and is sampled during IO.
    void recordExecutablePid(pid_t root_pid) noexcept;

    /// Executable (non-pool) path: walk the subtree from `executable_root_pid`,
    /// read `VmHWM` from `/proc/<pid>/status` for each pid, and keep a running
    /// max in `executable_peak_rss_bytes`. Invoked from both IO loops (stdout
    /// read and stdin write). The first call always samples; subsequent non-final
    /// calls are throttled to at most one subtree walk per ~5 ms. `VmHWM`
    /// monotonicity keeps the running max correct under sparse sampling. No-op
    /// when `executable_root_pid <= 0`. Thread-safe: may be called concurrently
    /// from the write-buffer send thread and the read-buffer pull thread.
    ///
    /// When the stdout read reaches EOF, the caller passes `is_final=true` to
    /// take one best-effort concluding ("tail") sample that bypasses the throttle.
    /// This is a single attempt at the point the child has closed stdout and is
    /// typically still resident; it is NOT continuous sampling during the
    /// post-output reap. A failed `/proc` read on the final call is expected
    /// (the child may have already exited) and does not flag degradation.
    /// Memory the child allocates strictly after closing stdout is captured only
    /// on a best-effort basis: it may or may not be observed depending on whether
    /// the child is still resident when EOF is detected.
    ///
    /// Limitation: only the UDF process and its live descendants at a sample
    /// point are captured. A short-lived child that allocates and is reaped
    /// between two consecutive throttled samples is best-effort and may be
    /// under-counted. A process that is alive at least once after reaching its
    /// peak is captured exactly, because `VmHWM` is monotonic.
    /// Additionally, a short-lived UDF that allocates, writes small output
    /// that fits entirely in the pipe buffer, and exits before ClickHouse reads
    /// its stdout may report a near-zero peak: the IO-phase samples never fire
    /// because the child is already gone when the read side runs. CPU and
    /// elapsed time are unaffected (taken from `wait4` rusage).
    void sampleExecutablePeak([[maybe_unused]] bool is_final = false) noexcept;

    /// Executable (non-pool) path: stamp `elapsed_us` from the wall clock at
    /// `ShellCommandSource` cleanup and compute `peak_memory_byte_seconds` from
    /// `executable_peak_rss_bytes` (sampled from `/proc VmHWM` during IO).
    /// Safe to call even when the child was never reaped. Idempotent; a second
    /// call is a no-op.
    void recordExecutableElapsed() noexcept;

    /// Executable (non-pool) path: fill `user_time_us` and `system_time_us`
    /// from the scalars returned by `ShellCommand::getChild*` (CPU from `wait4`
    /// rusage). Two independent idempotency guards operate here:
    ///   - `recordExecutableElapsed` is guarded by `executable_finished`: it
    ///     stamps `elapsed_us` and `peak_memory_byte_seconds` on the first call
    ///     and is a no-op thereafter.
    ///   - `recordExecutableFinished` itself is guarded by
    ///     `executable_rusage_recorded`: it stores CPU scalars on the first call
    ///     and is a no-op thereafter, even if `recordExecutableElapsed` was
    ///     already invoked by the caller.
    /// `recordExecutableElapsed` is always called from here so that elapsed and
    /// peak are stamped even when CPU is recorded first.
    /// Peak memory is taken from `/proc VmHWM` sampled during IO; this method
    /// does not receive or touch it.
    void recordExecutableFinished(UInt64 user_time_us, UInt64 system_time_us) noexcept;

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
    bool readPeakRssFailedAnyPid() const noexcept { return read_peak_rss_failed_any.load(std::memory_order_relaxed); }

    /// Whether `walkSubtree` hit the `MAX_PIDS` cap during this borrow and
    /// a candidate pid had to be dropped. When true, the subtree enumeration
    /// is incomplete, so CPU and `PeakMemoryByteSeconds` will under-count
    /// the unvisited descendants.
    bool subtreeWalkTruncated() const noexcept { return subtree_truncated_any.load(std::memory_order_relaxed); }

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

    /// Written by sampleExecutablePeak (called from two threads: the write-buffer
    /// send thread and the read-buffer pull thread) and read after both threads are
    /// joined. Must be atomic to avoid a data race.
    std::atomic<bool> read_peak_rss_failed_any{false};
    std::atomic<bool> subtree_truncated_any{false};

    std::atomic<UInt64> input_bytes{0};
    std::atomic<UInt64> output_bytes{0};

    /// Executable path only: pid set by recordExecutablePid; <= 0 means sampling
    /// has not been requested (pool path or no process yet). Guarded by sequential
    /// writes (recordExecutablePid called once before IO) and reads from IO threads.
    pid_t executable_root_pid = -1;

    /// Running max VmHWM across the executable child's subtree, updated atomically
    /// by sampleExecutablePeak from two IO threads. Read after both threads join.
    std::atomic<UInt64> executable_peak_rss_bytes{0};

    /// Wall-clock microseconds (from entry_watch) of the last subtree sample, or 0
    /// if none taken yet. Throttles sampleExecutablePeak off the per-buffer IO path;
    /// updated with a relaxed CAS from the two IO threads.
    std::atomic<UInt64> last_sample_us{0};

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
/// `/proc/<pid>` path string is allowed to propagate. The callers in this
/// file (`recordPidAcquired`, `recordReleased`, `sampleExecutablePeak`) run
/// them inside a try/catch, so such an `exception` is logged and sampling is
/// skipped, instead of hitting `std::terminate` at a `noexcept` boundary.
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

    /// Parse VmRSS from /proc/<pid>/status, converted to bytes.
    bool readCurrentRss(pid_t pid, UInt64 & bytes);

    /// Return true if /proc/<pid>/stat can be read and reports the process as a zombie.
    bool isZombie(pid_t pid);
}

}
