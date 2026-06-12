#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <mutex>
#include <unordered_set>

#include <sys/types.h>


namespace DB
{

/** Global registry of executable UDF child pids, powering the point-in-time
  * asynchronous metrics `ExecutableUserDefinedFunctionMemoryResident`
  * and `ExecutableUserDefinedFunctionProcesses`.
  *
  * `ShellCommand` adds the pid at spawn (opt-in via
  * `Config::register_in_udf_process_registry`, set only by the executable UDF
  * path) and removes it at reap or wrapper destruction, so idle pool workers
  * stay registered for as long as they live.
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
        /// Shared pages are counted once per process, so this is an upper bound.
        UInt64 memory_resident_bytes = 0;

        /// Number of processes the memory sum was taken over.
        UInt64 process_count = 0;
    };

    /// Walk the /proc subtree of every registered pid and read the VmRSS
    /// once per process. Returns 0 for non-Linux.
    Sample sample() const;

private:
    mutable std::mutex mutex;
    std::unordered_set<pid_t> pids TSA_GUARDED_BY(mutex);
};

}
