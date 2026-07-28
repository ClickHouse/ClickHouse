#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/castColumn.h>

#include <optional>

namespace DB
{

namespace LowCardinalityExecutionHelpers
{

/// Convert a constant arrayElement index to a zero-based position within one array row.
/// Returns empty for out-of-bounds indexes, which means arrayElement should return the default value.
inline std::optional<size_t> adjustedIndexFromField(const Field & index, size_t array_size)
{
    if (index.getType() == Field::Types::UInt64 || (index.getType() == Field::Types::Int64 && index.safeGet<Int64>() >= 0))
    {
        UInt64 positive_index = index.safeGet<UInt64>();
        if (positive_index > 0 && positive_index <= array_size)
            return positive_index - 1;
    }
    else if (index.getType() == Field::Types::Int64)
    {
        /// Cast to UInt64 before negation allows to avoid undefined behaviour for negation of the most negative number.
        UInt64 index_from_end = -static_cast<UInt64>(index.safeGet<Int64>());
        if (index_from_end <= array_size)
            return array_size - index_from_end;
    }

    return {};
}

struct FilterAndOffsets
{
    IColumn::Filter filter;
    ColumnArray::ColumnOffsets::MutablePtr offsets;
    size_t result_size = 0;
};

/// View of an Array(LowCardinality(T))-shaped column: LC element indexes plus row offsets.
/// Keep this header-only so callers can compile the row loops down to the same code as local loops.
struct LowCardinalityArrayView
{
    const ColumnLowCardinality & elements;
    const ColumnArray::Offsets & offsets;
    size_t rows;

    template <typename Function>
    inline void forEachRange(Function && function) const
    {
        for (size_t row = 0; row != rows; ++row)
        {
            size_t begin = offsets[ssize_t(row) - 1];
            size_t end = offsets[row];
            function(row, begin, end);
        }
    }

    /// Build arrayElement(Array(LowCardinality), const) by selecting dictionary values through LC indexes,
    /// instead of materializing the whole nested array first.
    inline ColumnPtr arrayElementConst(const Field & index, const IDataType & result_type) const
    {
        auto result = result_type.createColumn();
        result->reserve(rows);

        const auto & dictionary = *elements.getDictionary().getNestedColumn();
        forEachRange([&](size_t, size_t begin, size_t end)
        {
            if (auto adjusted_index = adjustedIndexFromField(index, end - begin))
                result->insertFrom(dictionary, elements.getIndexAt(begin + *adjusted_index));
            else
                result->insertDefault();
        });

        return result;
    }

    /// Given a per-dictionary-entry predicate result, compute whether any element in each array row matches.
    inline ColumnUInt8::MutablePtr existsByDictionaryMatches(const PaddedPODArray<UInt8> & dictionary_matches) const
    {
        auto result = ColumnUInt8::create();
        auto & result_data = result->getData();
        result_data.resize_fill(rows);

        forEachRange([&](size_t row, size_t begin, size_t end)
        {
            for (size_t i = begin; i != end; ++i)
            {
                if (dictionary_matches[elements.getIndexAt(i)])
                {
                    result_data[row] = 1;
                    break;
                }
            }
        });

        return result;
    }

    /// Given a per-dictionary-entry predicate result, build a filter and offsets for matching array elements.
    inline FilterAndOffsets filterByDictionaryMatches(const PaddedPODArray<UInt8> & dictionary_matches) const
    {
        FilterAndOffsets result{.filter = IColumn::Filter(elements.size()), .offsets = ColumnArray::ColumnOffsets::create()};
        auto & new_offsets = result.offsets->getData();
        new_offsets.reserve(rows);

        forEachRange([&](size_t, size_t begin, size_t end)
        {
            for (size_t i = begin; i != end; ++i)
            {
                UInt8 matched = dictionary_matches[elements.getIndexAt(i)];
                result.filter[i] = matched;
                result.result_size += matched;
            }
            new_offsets.push_back(result.result_size);
        });

        return result;
    }
};

/// Evaluate a predicate over LC dictionary entries, either over the whole dictionary or only over
/// dictionary indexes used by this block. [evaluate] must return a UInt8 column sized like its input.
template <typename Evaluate>
inline ColumnPtr dictionaryMatchesForSelectedIndexes(
    const ColumnLowCardinality & low_cardinality_column,
    Evaluate && evaluate,
    /// Local benchmarks chose 4 to keep full-dictionary scans for normal LC blocks but avoid O(dictionary) LIKE work for small slices.
    size_t max_dictionary_to_elements_ratio_for_full_scan = 4)
{
    size_t dictionary_size = low_cardinality_column.getDictionary().size();
    size_t selected_elements = low_cardinality_column.size();

    /// For small dictionaries it is cheaper to evaluate every dictionary value than to build distinct selected indexes.
    if (selected_elements != 0 && dictionary_size <= selected_elements * max_dictionary_to_elements_ratio_for_full_scan)
        return evaluate(low_cardinality_column.getDictionary().getNestedColumn());

    auto sparse_matches = ColumnUInt8::create(dictionary_size, UInt8{0});
    auto & sparse_matches_data = sparse_matches->getData();

    auto distinct_indexes = low_cardinality_column.getDistinctIndexes(0, selected_elements);
    if (distinct_indexes.empty())
        return sparse_matches;

    auto distinct_indexes_column = ColumnUInt64::create();
    distinct_indexes_column->getData() = std::move(distinct_indexes);
    const auto & distinct_indexes_data = distinct_indexes_column->getData();

    auto dictionary_values = low_cardinality_column.getDictionary().getNestedColumn()->index(*distinct_indexes_column, 0);
    auto selected_matches_column = evaluate(std::move(dictionary_values));
    const auto & selected_matches = assert_cast<const ColumnUInt8 &>(*selected_matches_column).getData();

    for (size_t i = 0; i != distinct_indexes_data.size(); ++i)
        sparse_matches_data[distinct_indexes_data[i]] = selected_matches[i];

    return sparse_matches;
}

/// Returns false if the constant value is not present in the dictionary. If the constant is NULL,
/// returns true and sets [dictionary_index] to the default LC null index, matching the existing
/// Array(LowCardinality) index-function behavior.
/// Keep this inlined: the Array(LowCardinality) index functions are sensitive to this setup codegen.
inline __attribute__((always_inline)) bool dictionaryIndexForConstant(
    const ColumnLowCardinality & low_cardinality_data,
    const ColumnPtr & value_column,
    const DataTypePtr & value_type,
    const DataTypePtr & target_type,
    UInt64 & dictionary_index)
{
    dictionary_index = 0;

    auto value = recursiveRemoveLowCardinality(value_column);
    if (value->isNullAt(0))
        return true;

    auto value_type_without_low_cardinality = recursiveRemoveLowCardinality(value_type);
    value = castColumn({value, value_type_without_low_cardinality, ""}, target_type);

    if (value->isNullable())
        value = assert_cast<const ColumnNullable &>(*value).getNestedColumnPtr();

    std::string_view elem = value->getDataAt(0);
    if (auto maybe_index = low_cardinality_data.getDictionary().getOrFindValueIndex(elem))
    {
        dictionary_index = *maybe_index;
        return true;
    }

    return false;
}

}

}
