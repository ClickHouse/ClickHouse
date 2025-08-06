#include <Storages/System/StorageSystemLatencyBuckets.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/IColumn.h>
#include <Storages/ColumnsDescription.h>
#include <Common/LatencyBuckets.h>

namespace DB
{
ColumnsDescription StorageSystemLatencyBuckets::getColumnsDescription()
{
    ColumnsDescription result;

    for (size_t i = 0, end = LatencyBuckets::end(); i < end; ++i)
    {
        auto name = fmt::format("LatencyEvent_{}", LatencyBuckets::getName(LatencyBuckets::LatencyEvent(i)));
        const auto * comment = LatencyBuckets::getDocumentation(LatencyBuckets::LatencyEvent(i));
        result.add({std::move(name), std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), comment});
    }

    return result;
}

void StorageSystemLatencyBuckets::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (size_t i = 0, end = LatencyBuckets::end(); i < end; ++i)
    {
        auto & bucket_bounds = LatencyBuckets::getBucketBounds(LatencyBuckets::LatencyEvent(i));
        res_columns[i]->insert(Array(bucket_bounds.begin(), bucket_bounds.end()));
    }
}
}
