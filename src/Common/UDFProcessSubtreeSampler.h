#pragma once

#include <base/types.h>
#include <Common/Stopwatch.h>

#include <atomic>
#include <unordered_map>
#include <vector>

#include <sys/types.h>


namespace DB
{

/** Per-borrow resource accounting for an executable_pool UDF call.
  *
  * Created by `UserDefinedExecutableFunctionFactory::executeImpl` and passed
  * via `ShellCommandSourceConfiguration` into the borrow/IO machinery. The
  * factory queries the accumulators after the pipeline drains and feeds them
  * into ProfileEvents.
  *
  * Lifecycle (callbacks tolerate being skipped — e.g. `tryBorrowObject`
  * may time out, in which case only `recordPoolWaitDone` fires):
  *   ctor                    -- implicitly starts the wall clock for pool wait
  *   recordPoolWaitDone      -- `tryBorrowObject` returned (success OR timeout)
  *   recordPidAcquired       -- `buildCommand` returned; pid known (success only)
  *   recordInputBytes / recordOutputBytes -- IO buffers report bytes pushed
  *   recordReleased          -- ShellCommandSource cleanup, before returnObject
  *
  * CPU and memory deltas are derived by sampling /proc/<pid>/{stat,status} for
  * every descendant of the pool's child process. The pre-snapshot zeroes
  * VmHWM via /proc/<pid>/clear_refs (mode 5) so VmHWM at release reflects the
  * peak observed during this borrow, not the lifetime peak of the worker.
  *
  * Procfs failures (ENOENT after a worker died, EACCES, malformed) are
  * silently skipped: CPU/memory increments are dropped but the other events
  * still fire.
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

    /// Pool wait = entry → borrow acquired.
    /// Zero if the borrow never happened (caller should still report 0).
    UInt64 getPoolWaitMicroseconds() const noexcept { return pool_wait_us; }

    /// Borrow-internal wall = borrow acquired → released.
    UInt64 getElapsedMicroseconds() const noexcept { return elapsed_us; }

    UInt64 getUserTimeMicroseconds() const noexcept { return user_time_us; }
    UInt64 getSystemTimeMicroseconds() const noexcept { return system_time_us; }
    UInt64 getPeakMemoryByteSeconds() const noexcept { return peak_memory_byte_seconds; }
    UInt64 getInputBytes() const noexcept { return input_bytes.load(std::memory_order_relaxed); }
    UInt64 getOutputBytes() const noexcept { return output_bytes.load(std::memory_order_relaxed); }

    bool poolWaitDone() const noexcept { return pool_wait_done; }
    bool borrowAcquired() const noexcept { return borrow_acquired; }

private:
    Stopwatch entry_watch;
    Stopwatch borrow_watch;

    pid_t root_pid = -1;
    bool pool_wait_done = false;
    bool borrow_acquired = false;

    UInt64 pool_wait_us = 0;
    UInt64 elapsed_us = 0;
    UInt64 user_time_us = 0;
    UInt64 system_time_us = 0;
    UInt64 peak_memory_byte_seconds = 0;

    std::atomic<UInt64> input_bytes{0};
    std::atomic<UInt64> output_bytes{0};

    struct PreSnapshot
    {
        UInt64 utime_us = 0;
        UInt64 stime_us = 0;
    };
    std::unordered_map<pid_t, PreSnapshot> pre_snapshot;
};


/// Free helpers for /proc parsing. On non-Linux platforms they are no-ops
/// that return empty / false.
namespace UDFProcfs
{
    /// Recursively enumerate the root pid plus every descendant by walking
    /// `/proc/<pid>/task/<tid>/children`. Returns at least {root_pid} when
    /// procfs is unavailable.
    std::vector<pid_t> walkSubtree(pid_t root_pid);

    /// Write "5\n" to /proc/<pid>/clear_refs to reset VmHWM.
    /// Silently ignores any error.
    void clearRefs(pid_t pid) noexcept;

    /// Parse user and system CPU time from /proc/<pid>/stat, converting clock
    /// ticks to microseconds. `utime_us` sums fields 14 (`utime`, this pid's
    /// own user CPU) and 16 (`cutime`, user CPU of children this pid has
    /// reaped). `stime_us` sums fields 15 (`stime`) and 17 (`cstime`). Reading
    /// cutime/cstime is necessary because a short-lived helper (e.g. python
    /// `subprocess.run`) finishes and is `waitpid`-ed before the post-walk
    /// even sees it; without those two fields the helper's CPU is invisible.
    bool readStat(pid_t pid, UInt64 & utime_us, UInt64 & stime_us) noexcept;

    /// Parse VmHWM from /proc/<pid>/status, converted to bytes.
    bool readPeakRss(pid_t pid, UInt64 & bytes) noexcept;
}

}
