#pragma once

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumnUnique.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/HashTable/HashMap.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>

namespace DB
{

/// A dictionary for the `LowCardinality` result of a function that chooses the value of every row among
/// a few values known in advance: the constant branches of `if` and `multiIf`, or the constant mapping of
/// `transform`. The function computes, for every row, the number of the chosen value, and translates it
/// to a dictionary index through `positions`, instead of inserting every row into a new dictionary.
///
/// The dictionary must hold only the values that the result refers to. A function that takes the result as
/// an argument is executed over the whole dictionary (see `replaceLowCardinalityColumnByNestedAndGetDictionaryIndexes`),
/// so a value that no row has chosen would otherwise reach it, and it can throw on that value:
/// `match('a', if(number = 0, 'a', '['))` must not compile `[` when no row takes the `else` branch.
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

/// Builds the `LowCardinality` result out of `value_numbers`: for every row, the number of the row of `values`
/// that is the value of the result. `values` is a full column of the dictionary type of `type`, and
/// `value_numbers` is a `ColumnVector<IndexType>`, which is reused for the indexes of the result, so `IndexType`
/// must be able to hold `values.size() + 1` (the dictionary also has the default value and NULL).
/// The dictionary gets only the values that `value_numbers` refers to.
template <typename IndexType>
ColumnPtr createLowCardinalityOfReferencedValues(
    const DataTypeLowCardinality & type, const IColumn & values, MutableColumnPtr && value_numbers)
{
    auto & numbers = assert_cast<ColumnVector<IndexType> &>(*value_numbers).getData();
    const size_t num_values = values.size();

    auto referenced_numbers = ColumnUInt64::create();
    auto & referenced = referenced_numbers->getData();

    auto create_result = [&](LowCardinalityOfKnownValues && referenced_values)
    {
        return ColumnLowCardinality::create(
            MutableColumnPtr(std::move(referenced_values.dictionary)), std::move(value_numbers), /*is_shared=*/false);
    };

    /// The work must be proportional to the block: `values` can be large (the whole mapping of `transform`),
    /// while a block refers to a few of them. Arrays over all `values` are used only when they are not larger
    /// than the block, otherwise the referenced numbers are collected in a hash table.
    if (num_values <= std::max<size_t>(numbers.size(), 256))
    {
        VectorWithMemoryTracking<UInt8> is_referenced(num_values, 0);
        for (const auto number : numbers)
            is_referenced[number] = 1;

        for (size_t i = 0; i < num_values; ++i)
            if (is_referenced[i])
                referenced.push_back(i);

        LowCardinalityOfKnownValues referenced_values(type, *values.index(*referenced_numbers, 0));

        VectorWithMemoryTracking<IndexType> position_of_value(num_values, 0);
        for (size_t i = 0; i < referenced.size(); ++i)
            position_of_value[referenced[i]] = static_cast<IndexType>(referenced_values.positions[i]);

        for (auto & number : numbers)
            number = position_of_value[number];

        return create_result(std::move(referenced_values));
    }

    /// Every referenced number gets a compact number in the order of the first reference, and the rows are
    /// switched to the compact numbers right away, so the hash table is looked up once per row.
    HashMap<IndexType, IndexType> compact_number_of_value;
    for (auto & number : numbers)
    {
        typename HashMap<IndexType, IndexType>::LookupResult it;
        bool inserted;
        compact_number_of_value.emplace(number, it, inserted);
        if (inserted)
        {
            it->getMapped() = static_cast<IndexType>(referenced.size());
            referenced.push_back(number);
        }
        number = it->getMapped();
    }

    LowCardinalityOfKnownValues referenced_values(type, *values.index(*referenced_numbers, 0));

    for (auto & number : numbers)
        number = static_cast<IndexType>(referenced_values.positions[number]);

    return create_result(std::move(referenced_values));
}

}
