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
    static Block calculate(
        const MergeTreeData & storage,
        const DataPartsVector & parts
    );
};

}
