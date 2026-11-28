#pragma once

#include <Columns/IColumnUnique.h>
#include <Common/PODArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// A dictionary for the `LowCardinality` result of a function that chooses the value of every row among
/// a few values known in advance: the constant branches of `if` and `multiIf`, or the constant mapping of
/// `transform`. The function computes, for every row, the number of the chosen value, and translates it
/// to a dictionary index through `positions`, instead of inserting every row into a new dictionary.
///
/// The dictionary of the result must hold only the values that some row refers to (and the default value
/// and NULL, which every dictionary has), as it does when the rows are inserted one by one. A function
/// that gets a `LowCardinality` argument is evaluated over the whole dictionary, so a value that no row
/// has chosen would otherwise reach it, and it can throw on such a value: `match('a', if(cond, 'a', '['))`
/// must not compile `[` when no row takes the second branch. So pass only the chosen values here, or
/// check the result with `allDictionaryValuesAreReferenced`.
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

/// Whether every value of `dictionary` is referenced by some of `indexes`. The default value and NULL
/// are not counted: they are in every dictionary, referenced or not.
template <typename IndexType>
bool allDictionaryValuesAreReferenced(const IColumnUnique & dictionary, const PaddedPODArray<IndexType> & indexes)
{
    const size_t dictionary_size = dictionary.size();
    const size_t num_special_values = dictionary.getNestedTypeDefaultValueIndex() + 1;
    if (dictionary_size <= num_special_values)
        return true;
    if (indexes.size() < dictionary_size - num_special_values)
        return false;

    VectorWithMemoryTracking<UInt8> is_referenced(dictionary_size, 0);
    UInt8 * __restrict is_referenced_data = is_referenced.data();
    for (const auto index : indexes)
        is_referenced_data[index] = 1;

    for (size_t i = num_special_values; i < dictionary_size; ++i)
        if (!is_referenced_data[i])
            return false;
    return true;
}

}
