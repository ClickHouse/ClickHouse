#include <Storages/MergeTree/MergeTreePartialAggregateInfo.h>

#include <Storages/MergeTree/MergeTreeData.h>

#include <fmt/format.h>

namespace DB
{

PartialAggregateInfoPtr partialAggregateInfoFromMergeTreePart(const IMergeTreeDataPart & data_part)
{
    const String part_name = data_part.isProjectionPart()
        ? fmt::format("{}:{}", data_part.getParentPartName(), data_part.name)
        : data_part.name;
    return std::make_shared<PartialAggregateInfo>(
        data_part.storage.getStorageID().uuid,
        part_name,
        data_part.info.mutation);
}

}
