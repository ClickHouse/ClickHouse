#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyPartitionMutex.h>

#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event UniqueKeyMutexHoldMicroseconds;
}

namespace DB
{

void recordUniqueKeyMutexHold(std::uint64_t hold_us)
{
    ProfileEvents::increment(ProfileEvents::UniqueKeyMutexHoldMicroseconds, hold_us);
}


std::shared_ptr<std::mutex> UniqueKeyPartitionMutex::getOrCreate(const String & partition_id)
{
    std::lock_guard lock(map_mutex);
    auto it = partition_mutexes.find(partition_id);
    if (it != partition_mutexes.end())
        return it->second;

    auto m = std::make_shared<std::mutex>();
    partition_mutexes.emplace(partition_id, m);
    return m;
}

}
