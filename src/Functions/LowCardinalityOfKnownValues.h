#pragma once

#include <Columns/IColumnUnique.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// A dictionary for the `LowCardinality` result of a function that chooses the value of every row among
/// a few values known in advance: the constant branches of `if` and `multiIf`, or the constant mapping of
/// `transform`. The function computes, for every row, the number of the chosen value, and translates it
/// to a dictionary index through `positions`, instead of inserting every row into a new dictionary.
struct LowCardinalityOfKnownValues
{
    MutableColumnUniquePtr dictionary;
    /// `positions[i]` is the index in `dictionary` of row `i` of `values`.
    VectorWithMemoryTracking<UInt64> positions;

    /// `values` is a full column of the dictionary type of `type`.
    LowCardinalityOfKnownValues(const DataTypeLowCardinality & type, const IColumn & values)
        : dictionary(DataTypeLowCardinality::createColumnUnique(*type.getDictionaryType()))
    {
        /// The same insertion as in the default implementation for `LowCardinality` results, so NULLs and
        /// the default value are mapped exactly as there.
        auto positions_column = dictionary->uniqueInsertRangeFrom(values, 0, values.size());
        positions.resize(values.size());
        for (size_t i = 0; i < values.size(); ++i)
            positions[i] = positions_column->getUInt(i);
    }
};

}
