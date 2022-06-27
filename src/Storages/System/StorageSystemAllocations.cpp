#include <Storages/System/StorageSystemAllocations.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Common/MemoryAllocationTracker.h>

namespace DB
{

StorageSystemAllocations::StorageSystemAllocations(const StorageID & table_id_)
    : IStorageSystemOneBlock<StorageSystemAllocations>(table_id_)
{
}

NamesAndTypesList StorageSystemAllocations::getNamesAndTypes()
{
    return
    {
        { "trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()) },
        { "bytes", std::make_shared<DataTypeUInt64>() },
    };
}

void StorageSystemAllocations::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    auto * trace_array = typeid_cast<ColumnArray *>(res_columns[0].get());
    auto & trace_values = typeid_cast<ColumnUInt64 &>(trace_array->getData()).getData();
    auto & trace_offsets = typeid_cast<ColumnUInt64 &>(trace_array->getOffsetsColumn()).getData();
    auto & bytes = typeid_cast<ColumnUInt64 *>(res_columns[1].get())->getData();

    auto traces = MemoryAllocationTracker::dump_allocations(0, 0, false);

    UInt64 offset = 0;
    for (const auto & trace : traces)
    {
        for (const auto frame : trace.frames)
            trace_values.push_back(frame);

        offset += trace.frames.size();
        trace_offsets.push_back(offset);
        bytes.push_back(trace.allocated);
    }
}

}
