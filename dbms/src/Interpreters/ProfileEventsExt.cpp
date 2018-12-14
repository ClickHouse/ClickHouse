#include "ProfileEventsExt.h"
#include <Common/typeid_cast.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>

namespace ProfileEvents
{

/// Put implementation here to avoid extra linking dependencies for clickhouse_common_io
void dumpToArrayColumns(const Counters & counters, DB::IColumn * column_names_, DB::IColumn * column_values_, bool nonzero_only)
{
    /// Convert ptr and make simple check
    auto column_names = (column_names_) ? &typeid_cast<DB::ColumnArray &>(*column_names_) : nullptr;
    auto column_values = (column_values_) ? &typeid_cast<DB::ColumnArray &>(*column_values_) : nullptr;

    size_t size = 0;

    for (Event event = 0; event < Counters::num_counters; ++event)
    {
        UInt64 value = counters[event].load(std::memory_order_relaxed);

        if (nonzero_only && 0 == value)
            continue;

        ++size;

        if (column_names)
        {
            const char * desc = ProfileEvents::getName(event);
            column_names->getData().insertData(desc, strlen(desc));
        }

        if (column_values)
            column_values->getData().insert(value);
    }

    if (column_names)
    {
        auto & offsets = column_names->getOffsets();
        offsets.push_back((offsets.empty() ? 0 : offsets.back()) + size);
    }

    if (column_values)
    {
        /// Nested columns case
        bool the_same_offsets = column_names && column_names->getOffsetsPtr().get() == column_values->getOffsetsPtr().get();
        if (!the_same_offsets)
        {
            auto & offsets = column_values->getOffsets();
            offsets.push_back((offsets.empty() ? 0 : offsets.back()) + size);
        }
    }
}

}
