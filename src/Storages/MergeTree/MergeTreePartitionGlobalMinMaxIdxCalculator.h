#pragma once

#include <utility>

#include <Core/Field.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/*
 * Calculates global min max indexes for a given set of parts on given storage.
 * */
class MergeTreePartitionGlobalMinMaxIdxCalculator
{
    using DataPart = IMergeTreeDataPart;
    using DataPartPtr = std::shared_ptr<const DataPart>;
    using DataPartsVector = std::vector<DataPartPtr>;

public:
    static IMergeTreeDataPart::MinMaxIndex calculate(const DataPartsVector & parts, const MergeTreeData & storage);
};

}
