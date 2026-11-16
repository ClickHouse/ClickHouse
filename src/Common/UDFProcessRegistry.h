#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <mutex>
#include <unordered_map>

#include <sys/types.h>


namespace DB
{

/** Global registry of executable UDF child pids, powering the asynchronous
  * metrics `ExecutableUserDefinedFunctionMemoryResidentBytes` and
  * `ExecutableUserDefinedFunctionProcesses`.
  *
  * `add` stamps each pid with a generation that `ShellCommand` passes back to
  * `removeIfGenerationMatches` at reap, so a stale removal cannot erase a
  * successor that reused the pid. An unreaped child stays counted (its pid is
  * pinned); once it becomes a zombie it has no `VmRSS`, drops out of both
  * metrics, and `sample` prunes its entry.
  */
class UDFProcessRegistry
{
public:
    static UDFProcessRegistry & instance();

    UInt64 add(pid_t pid);

    void removeIfGenerationMatches(pid_t pid, UInt64 generation);

    struct Sample
    {
        /// Sum of VmRSS over all live registered processes and their descendants.
        /// Shared pages are counted once per process.
        UInt64 memory_resident_bytes = 0;

        /// Number of processes the memory sum was taken over.
        UInt64 process_count = 0;
    };

    Sample sample();

private:
    std::mutex mutex;
    std::unordered_map<pid_t, UInt64> pids TSA_GUARDED_BY(mutex);
    UInt64 next_generation TSA_GUARDED_BY(mutex) = 0;
};

}
