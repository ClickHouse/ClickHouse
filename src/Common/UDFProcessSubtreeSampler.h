#pragma once

#include <base/types.h>
#include <Common/Stopwatch.h>

#include <atomic>
#include <unordered_map>
#include <vector>

#include <sys/resource.h>
#include <sys/types.h>


namespace DB
{

/** Per-invocation resource accounting for `executable` and `executable_pool` UDFs.
  *
  * Two mutually exclusive modes, selected by which record* methods the caller invokes:
  *
  * Pool mode (`executable_pool`):
  *   CPU and memory are derived by sampling /proc/<pid>/{stat,status} across
  *   the entire process subtree. The pre-snapshot resets VmHWM via
  *   /proc/<pid>/clear_refs (mode 5) so the reported peak reflects only the
  *   duration of the borrow. Procfs failures are silently skipped.
  *
  *   Lifecycle (callbacks tolerate being skipped):
  *     ctor                    -- starts the wall clock for pool wait
  *     recordPoolWaitDone      -- `tryBorrowObject` returned (success OR timeout)
  *     recordPidAcquired       -- `buildCommand` returned; pid known (success only)
  *     recordInputBytes / recordOutputBytes -- IO buffers report bytes pushed
  *     recordReleased          -- ShellCommandSource cleanup, before returnObject
  *
  * Executable mode (`executable`):
  *   CPU and memory are read from the `rusage` struct populated by `wait4` when
  *   the child exits. No procfs access needed.
  *
  *   Lifecycle:
  *     ctor                    -- starts the entry wall clock
  *     recordInputBytes / recordOutputBytes -- IO buffers report bytes pushed
  *     recordExecutableFinished -- called from ShellCommandSource::cleanup with
  *                                 the rusage from `wait4`; fills all accumulators
  *
  * Created by `UserDefinedExecutableFunctionFactory::executeImpl` and passed
  * via `ShellCommandSourceConfiguration` into the IO machinery. The factory
  * reads the accumulators from the SCOPE_EXIT guard and emits ProfileEvents.
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

    /// Executable (non-pool) path: consume the rusage from `wait4` and fill
    /// elapsed_us, user_time_us, system_time_us, and peak_memory_byte_seconds.
    /// Must be called at most once per sampler lifetime.
    void recordExecutableFinished(const ::rusage & ru) noexcept;

    /// Pool wait = entry → borrow acquired.
    /// Zero if the borrow never happened (caller should still report 0).
    UInt64 getPoolWaitMicroseconds() const noexcept { return pool_wait_us; }

    /// Pool mode: borrow-internal wall time (borrow acquired → released).
    /// Executable mode: entry-wall time (ctor → child exit).
    UInt64 getElapsedMicroseconds() const noexcept { return elapsed_us; }

    UInt64 getUserTimeMicroseconds() const noexcept { return user_time_us; }
    UInt64 getSystemTimeMicroseconds() const noexcept { return system_time_us; }
    UInt64 getPeakMemoryByteSeconds() const noexcept { return peak_memory_byte_seconds; }
    UInt64 getInputBytes() const noexcept { return input_bytes.load(std::memory_order_relaxed); }
    UInt64 getOutputBytes() const noexcept { return output_bytes.load(std::memory_order_relaxed); }

    bool poolWaitDone() const noexcept { return pool_wait_done; }
    bool borrowAcquired() const noexcept { return borrow_acquired; }
    bool executableFinished() const noexcept { return executable_finished; }

private:
    Stopwatch entry_watch;
    Stopwatch borrow_watch;

    pid_t root_pid = -1;
    bool pool_wait_done = false;
    bool borrow_acquired = false;
    bool executable_finished = false;

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

    /// Parse utime (field 14) and stime (field 15) from /proc/<pid>/stat,
    /// converting clock ticks to microseconds.
    bool readStat(pid_t pid, UInt64 & utime_us, UInt64 & stime_us) noexcept;

    /// Parse VmHWM from /proc/<pid>/status, converted to bytes.
    bool readPeakRss(pid_t pid, UInt64 & bytes) noexcept;
}

}
