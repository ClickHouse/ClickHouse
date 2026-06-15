#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <mutex>
#include <unordered_set>

#include <sys/types.h>


namespace DB
{

/** Global registry of executable UDF child pids, powering the point-in-time
  * asynchronous metrics `ExecutableUserDefinedFunctionMemoryResidentBytes`
  * and `ExecutableUserDefinedFunctionProcesses`.
  *
  * `ShellCommand` adds the pid at spawn and removes it when the child is
  * reaped, so idle pool workers stay registered for as long as they live.
  * A never-reaped child stays registered: its pid stays pinned. Once it dies
  * it is a zombie, which has released its address space, so `/proc/<pid>/status`
  * has no `VmRSS` line and `sample` counts it toward neither metric.
  */
class UDFProcessRegistry
{
public:
    static UDFProcessRegistry & instance();

    void add(pid_t pid);

    void remove(pid_t pid);

    struct Sample
    {
        /// Sum of VmRSS over all live registered processes and their descendants.
        /// Shared pages are counted once per process.
        UInt64 memory_resident_bytes = 0;

        /// Number of processes the memory sum was taken over.
        UInt64 process_count = 0;
    };

    /// Walk the /proc subtree of every registered pid and read the VmRSS
    /// once per process.
    Sample sample() const;

private:
    mutable std::mutex mutex;
    std::unordered_set<pid_t> pids TSA_GUARDED_BY(mutex);
};

}
