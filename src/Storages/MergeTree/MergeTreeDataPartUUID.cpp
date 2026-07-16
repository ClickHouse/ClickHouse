#include <vector>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>


namespace DB
{

std::vector<UUID> PartUUIDs::add(const std::vector<UUID> & new_uuids)
{
    std::lock_guard lock(mutex);
    std::vector<UUID> intersection;

    /// First check any presence of uuids in a uuids, return duplicates back if any
    for (const auto & uuid : new_uuids)
    {
        if (uuids.contains(uuid))
            intersection.emplace_back(uuid);
    }

    if (intersection.empty())
    {
        for (const auto & uuid : new_uuids)
            uuids.emplace(uuid);
    }
    return intersection;
}

std::vector<UUID> PartUUIDs::get() const
{
    std::lock_guard lock(mutex);
    return std::vector<UUID>(uuids.begin(), uuids.end());
}

bool PartUUIDs::has(const UUID & uuid) const
{
    std::lock_guard lock(mutex);
    return uuids.contains(uuid);
}

}
