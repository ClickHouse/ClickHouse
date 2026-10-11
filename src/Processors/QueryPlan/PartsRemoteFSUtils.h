#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct PartsRemoteFSInfo
{
    bool all_parts_on_remote_disk = false;
    bool any_parts_on_remote_disk = false;
};

inline PartsRemoteFSInfo analyzePartsOnRemoteFS(const RangesInDataParts & parts)
{
    PartsRemoteFSInfo result;

    if (parts.empty())
        return result;

    result.all_parts_on_remote_disk = true;

    for (const auto & part : parts)
    {
        const bool part_on_remote_disk = part.data_part->isStoredOnRemoteDisk();
        result.any_parts_on_remote_disk |= part_on_remote_disk;
        result.all_parts_on_remote_disk &= part_on_remote_disk;

        // Once both flags diverge, the result cannot change anymore.
        if (result.any_parts_on_remote_disk && !result.all_parts_on_remote_disk)
            break;
    }

    return result;
}

}
