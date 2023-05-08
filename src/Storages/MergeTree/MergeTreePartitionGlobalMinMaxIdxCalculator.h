#pragma once

#include <utility>

#include <Core/Field.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class MergeTreePartitionGlobalMinMaxIdxCalculator
{
    using DataPart = IMergeTreeDataPart;
    using DataPartPtr = std::shared_ptr<const DataPart>;
    using DataPartsVector = std::vector<DataPartPtr>;
public:
    static std::pair<Field, Field> calculate(const MergeTreeData & storage, const DataPartsVector & parts);

private:
    static std::pair<Field, Field> extractMinMaxFromBlock(const Block & block);
};

}
