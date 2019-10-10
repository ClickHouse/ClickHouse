#include "MergeTreeDataPartFactory.h"

namespace DB
{
    std::shared_ptr<IMergeTreeDataPart> createPart(const MergeTreeData & storage, const DiskSpace::DiskPtr & disk, const String & name,
        const MergeTreePartInfo & info, const String & relative_path)
    {
        return std::make_shared<MergeTreeDataPartWide>(storage, name, info, disk, relative_path);
    }

    std::shared_ptr<IMergeTreeDataPart> createPart(MergeTreeData & storage, const DiskSpace::DiskPtr & disk,
        const String & name, const String & relative_path)
    {
        return std::make_shared<MergeTreeDataPartWide>(storage, name, disk, relative_path);
    }
}
