#pragma once

#include <Storages/MergeTree/MergeTreeData.h>

/*
 * Computes the min max partition id for all parts. Min and max values are returned.
 * Currently used to attach partitions to tables with an equivalent, but not equal, partition expression.
 * */

namespace DB {

    class PartMinMaxPartitionIdCalculator
{
    public:
        std::pair<uint64_t, uint64_t> calculate(const MergeTreeData::DataPartPtr & dataPartPtr, StorageMetadataPtr metadata, ContextPtr context) const;

    private:
        std::pair<uint64_t, uint64_t> extractMinMaxValuesFromBlock(const NameAndTypePair & partition_id_column, Block & block) const;
    };


}
