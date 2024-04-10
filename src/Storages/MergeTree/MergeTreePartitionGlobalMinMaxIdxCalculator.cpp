#include <Storages/MergeTree/MergeTreePartitionGlobalMinMaxIdxCalculator.h>

namespace DB
{

IMergeTreeDataPart::MinMaxIndex
MergeTreePartitionGlobalMinMaxIdxCalculator::calculate(const DataPartsVector & parts, const MergeTreeData & storage)
{
    IMergeTreeDataPart::MinMaxIndex global_min_max_indexes;

    for (const auto & part : parts)
    {
        auto metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(part.get());

        auto local_min_max_index = MergeTreeData::DataPart::MinMaxIndex();

        local_min_max_index.load(storage, metadata_manager);

        global_min_max_indexes.merge(local_min_max_index);
    }

    return global_min_max_indexes;
}

}
