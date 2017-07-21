#pragma once

#include <Common/PODArray.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

namespace Conditional
{

/// This class provides access to the values of a condition in a multiIf function.
class CondSource final
{
public:
    CondSource(const Block & block, const ColumnNumbers & args, size_t i);

    /// Get the value of this condition for a given row.
    inline bool get(size_t row) const
    {
        bool is_null = !null_map.empty() && null_map[row];
        return is_null ? false : static_cast<bool>(data_array[row]);
    }

    inline size_t getSize() const
    {
        return data_array.size();
    }

private:
    const ColumnPtr materialized_col;
    const PaddedPODArray<UInt8> & data_array;
    const PaddedPODArray<UInt8> & null_map;
};

using CondSources = std::vector<CondSource>;

}

}
