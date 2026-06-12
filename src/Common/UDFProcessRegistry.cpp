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

    for (pid_t root : roots)
    {
        bool truncated = false;
        for (pid_t pid : UDFProcfs::walkSubtree(root, truncated))
        {
            UInt64 bytes = 0;
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
