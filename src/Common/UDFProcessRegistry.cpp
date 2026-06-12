#include <Common/UDFProcessRegistry.h>

#include <Common/UDFProcessSubtreeSampler.h>

#include <vector>


namespace DB
{

UDFProcessRegistry & UDFProcessRegistry::instance()
{
    static UDFProcessRegistry registry;
    return registry;
}


void UDFProcessRegistry::add(pid_t pid)
{
    if (pid <= 0)
        return;

    std::lock_guard lock(mutex);
    pids.insert(pid);
}


void UDFProcessRegistry::remove(pid_t pid)
{
    std::lock_guard lock(mutex);
    pids.erase(pid);
}


UDFProcessRegistry::Sample UDFProcessRegistry::sample() const
{
    std::vector<pid_t> roots;
    {
        std::lock_guard lock(mutex);
        roots.assign(pids.begin(), pids.end());
    }

    Sample result;

    /// Dedup defends against pid reuse during the sweep; disjoint subtrees
    /// cannot otherwise overlap. A truncated walk (> MAX_PIDS) under-counts.
    std::unordered_set<pid_t> seen;

    for (pid_t root : roots)
    {
        /// A root reaped after the snapshot may have been reused by an
        /// unrelated process; re-check membership to narrow that window.
        {
            std::lock_guard lock(mutex);
            if (!pids.contains(root))
                continue;
        }

        bool truncated = false;
        for (pid_t pid : UDFProcfs::walkSubtree(root, truncated))
        {
            if (!seen.insert(pid).second)
                continue;

            UInt64 bytes = 0;
            /// Skip vanished/zombie pids in both the sum and the count so the
            /// two metrics describe the same set of processes.
            if (UDFProcfs::readCurrentRss(pid, bytes))
            {
                result.memory_resident_bytes += bytes;
                ++result.process_count;
            }
        }
    }

    return result;
}

}
