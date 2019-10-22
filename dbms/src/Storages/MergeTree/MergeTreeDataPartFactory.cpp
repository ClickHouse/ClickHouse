#include "MergeTreeDataPartFactory.h"
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>

namespace DB
{
    std::shared_ptr<IMergeTreeDataPart> createPart(const MergeTreeData & storage, const DiskSpace::DiskPtr & disk, const String & name,
        const MergeTreePartInfo & info, const String & relative_path)
    {
        /// FIXME
        size_t size_of_mark = sizeof(size_t) + sizeof(size_t) * 2 * storage.getColumns().getAllPhysical().size();
        MergeTreeIndexGranularityInfo index_granularity_info(storage, ".mrk3", size_of_mark);
        return std::make_shared<MergeTreeDataPartCompact>(storage, name, info, index_granularity_info, disk, relative_path);
    }

    std::shared_ptr<IMergeTreeDataPart> createPart(MergeTreeData & storage, const DiskSpace::DiskPtr & disk,
        const String & name, const String & relative_path)
    {
        /// FIXME
        size_t size_of_mark = sizeof(size_t) + sizeof(size_t) * 2 * storage.getColumns().getAllPhysical().size();
        MergeTreeIndexGranularityInfo index_granularity_info(storage, ".mrk3", size_of_mark);
        return std::make_shared<MergeTreeDataPartCompact>(storage, name, index_granularity_info, disk, relative_path);
    }
}
