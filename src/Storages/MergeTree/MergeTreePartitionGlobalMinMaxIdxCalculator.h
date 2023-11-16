#pragma once

#include <utility>

#include <Core/Field.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/*
 * Calculates global min max indexes for a given set of parts.
 * */
class MergeTreePartitionGlobalMinMaxIdxCalculator
{
    using DataPart = IMergeTreeDataPart;
    using DataPartPtr = std::shared_ptr<const DataPart>;
    using DataPartsVector = std::vector<DataPartPtr>;
public:
    static std::vector<std::pair<Field, Field>> calculate(
        const MergeTreeData & storage,
        const DataPartsVector & parts
    );

private:
    static std::vector<std::pair<Field, Field>> extractMinMaxIndexesFromBlock(const Block & block_with_min_max_idx);
    static std::vector<std::pair<Field, Field>> calculateMinMaxIndexesForPart(const MergeTreeData & storage, const DataPartPtr & part);
    static void updateGlobalMinMaxIndexes(
        std::vector<std::pair<Field, Field>> & global_min_max_indexes,
        const std::vector<std::pair<Field, Field>> & local_min_max_indexes);

};

}
