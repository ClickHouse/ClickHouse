#include <Storages/MaterializedView/RefreshAllCombiner.h>

#include <algorithm>

namespace DB
{

RefreshAllCombiner::RefreshAllCombiner()
    : time_arrived{false}
{}

RefreshAllCombiner::RefreshAllCombiner(const std::vector<StorageID> & parents)
    : time_arrived{false}
{
    parents_arrived.reserve(parents.size());
    for (auto && parent : parents)
        parents_arrived.emplace(parent.uuid, false);
}

bool RefreshAllCombiner::arriveTime()
{
    std::lock_guard lock(combiner_mutex);
    time_arrived = true;
    return allArrivedLocked();
}

bool RefreshAllCombiner::arriveParent(const StorageID & id)
{
    std::lock_guard lock(combiner_mutex);
    parents_arrived[id.uuid] = true;
    return allArrivedLocked();
}

void RefreshAllCombiner::flush()
{
    std::lock_guard lock(combiner_mutex);
    flushLocked();
}

bool RefreshAllCombiner::allArrivedLocked()
{
    auto is_value = [](auto && key_value) { return key_value.second; };
    if (time_arrived && std::ranges::all_of(parents_arrived, is_value))
    {
        flushLocked();
        return true;
    }
    return false;
}

void RefreshAllCombiner::flushLocked()
{
    for (auto & [parent, arrived] : parents_arrived)
        arrived = false;
    time_arrived = false;
}

}
