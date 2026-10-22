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


UInt64 UDFProcessRegistry::add(pid_t pid)
{
    if (pid <= 0)
        return 0;

    std::lock_guard lock(mutex);
    UInt64 generation = ++next_generation;
    pids[pid] = generation;
    return generation;
}


void UDFProcessRegistry::removeIfGenerationMatches(pid_t pid, UInt64 generation)
{
    std::lock_guard lock(mutex);
    auto entry = pids.find(pid);
    if (entry != pids.end() && entry->second == generation)
        pids.erase(entry);
}


UDFProcessRegistry::Sample UDFProcessRegistry::sample()
{
    std::vector<std::pair<pid_t, UInt64>> roots;
    {
        std::lock_guard lock(mutex);
        roots.reserve(pids.size());
        for (const auto & [pid, generation] : pids)
            roots.emplace_back(pid, generation);
    }

    Sample result;

    for (const auto & [root, generation] : roots)
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

        if (UDFProcfs::isZombie(root))
            removeIfGenerationMatches(root, generation);
    }

    return result;
}

}
