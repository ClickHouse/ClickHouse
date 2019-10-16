#include "MergeTreeDataPartFactory.h"
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>

namespace DB
{
    std::shared_ptr<IMergeTreeDataPart> createPart(const MergeTreeData & storage, const DiskSpace::DiskPtr & disk, const String & name,
        const MergeTreePartInfo & info, const String & relative_path)
    {
        MergeTreeIndexGranularityInfo index_granularity_info(storage, ".mrk2", 3 * sizeof(size_t));
        return std::make_shared<MergeTreeDataPartWide>(storage, name, info, index_granularity_info, disk, relative_path);
    }

    std::shared_ptr<IMergeTreeDataPart> createPart(MergeTreeData & storage, const DiskSpace::DiskPtr & disk,
        const String & name, const String & relative_path)
    {
        MergeTreeIndexGranularityInfo index_granularity_info(storage, ".mrk2", 3 * sizeof(size_t));
        return std::make_shared<MergeTreeDataPartWide>(storage, name, index_granularity_info, disk, relative_path);
    }
}
