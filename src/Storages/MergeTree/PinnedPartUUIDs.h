#pragma once

#include <Common/ZooKeeper/IKeeper.h>
#include <Core/UUID.h>
#include <set>

namespace DB
{

struct PinnedPartUUIDs
{
    std::set<UUID> part_uuids;
    Coordination::Stat stat{};

    bool contains(const UUID & part_uuid) const
    {
        return part_uuids.contains(part_uuid);
    }

    String toString() const;
    void fromString(const String & buf);

private:
    static constexpr auto JSON_KEY_UUIDS = "part_uuids";
};

}
