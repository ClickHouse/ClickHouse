#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>

namespace DB
{
    std::shared_ptr<IMergeTreeDataPart> createPart(const MergeTreeData & storage_, const DiskSpace::DiskPtr & disk_, const String & name_, const MergeTreePartInfo & info_, const String & relative_path);

    std::shared_ptr<IMergeTreeDataPart> createPart(MergeTreeData & storage_, const DiskSpace::DiskPtr & disk_, const String & name_, const String & relative_path);
}
