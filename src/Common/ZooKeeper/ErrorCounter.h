#pragma once

#include <magic_enum.hpp>
#include <Common/ZooKeeper/IKeeper.h>
#include "Columns/ColumnArray.h"
#include "Columns/ColumnMap.h"
#include "Columns/ColumnTuple.h"

namespace Coordination
{

class ErrorCounter
{
private:
    static constexpr size_t error_count = magic_enum::enum_count<Coordination::Error>();
    std::array<UInt32, error_count> errors{};

public:
    void increment(Coordination::Error error)
    {
        const std::optional<size_t> index = magic_enum::enum_index(error);
        errors[*index]++;
    }

    void dumpToMapColumn(DB::ColumnMap * column) const
    {

        auto & offsets = column->getNestedColumn().getOffsets();
        auto & tuple_column = column->getNestedData();
        auto & key_column = tuple_column.getColumn(0);
        auto & value_column = tuple_column.getColumn(1);

        for (size_t i = 0; i != error_count; ++i)
        {
            static constexpr auto values = magic_enum::enum_values<Coordination::Error>();
            key_column.insert(values[i]);
            value_column.insert(errors[i]);
        }

        offsets.push_back(offsets.back() + error_count);
    }
};

}
