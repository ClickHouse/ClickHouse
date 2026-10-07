#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/FieldAccurateComparison.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
    extern const int CANNOT_PARSE_BOOL;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_UUID;
    extern const int DECIMAL_OVERFLOW;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

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

/// Is [code] a cast declining its input, rather than a fault of the caller? Anything else (a memory
/// limit, a logical error, a cancellation) is not an answer about the value and must propagate.
inline bool isConstantCastDecline(int code)
{
    return code == ErrorCodes::CANNOT_CONVERT_TYPE
        || code == ErrorCodes::CANNOT_PARSE_BOOL
        || code == ErrorCodes::CANNOT_PARSE_DATE
        || code == ErrorCodes::CANNOT_PARSE_DATETIME
        || code == ErrorCodes::CANNOT_PARSE_IPV4
        || code == ErrorCodes::CANNOT_PARSE_IPV6
        || code == ErrorCodes::CANNOT_PARSE_NUMBER
        || code == ErrorCodes::CANNOT_PARSE_TEXT
        || code == ErrorCodes::CANNOT_PARSE_UUID
        || code == ErrorCodes::DECIMAL_OVERFLOW
        || code == ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
        || code == ErrorCodes::NOT_IMPLEMENTED
        || code == ErrorCodes::TOO_LARGE_STRING_SIZE
        || code == ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM
        || code == ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

/// Did [value] survive the cast that produced [image]? The cast alone cannot report loss, since it
/// truncates UInt64(256) to UInt8(0) and succeeds, so compare the two in the type they meet in, where
/// neither side's padding is a difference.
inline bool targetTypeRepresentsValue(
    const ColumnPtr & value, const DataTypePtr & value_type, const ColumnPtr & image, const DataTypePtr & image_type)
{
    try
    {
        /// Without a common type the pair only compares as numbers, so [value_type] is where they meet.
        const auto common_type = tryGetLeastSupertype(DataTypes{value_type, image_type});
        const auto compare_type = common_type ? makeNullable(common_type) : makeNullable(value_type);

        const auto restored = castColumnAccurateOrNull({image, image_type, ""}, compare_type);
        if (restored->empty() || restored->isNullAt(0))
            return false;

        const auto original = castColumnAccurateOrNull({value, value_type, ""}, compare_type);
        if (original->empty() || original->isNullAt(0))
            return false;

        return accurateEquals((*restored)[0], (*original)[0]);
    }
    catch (const Exception & e)
    {
        if (!isConstantCastDecline(e.code()))
            throw;

        return false;
    }
}

/// Returns false if the constant value is not present in the dictionary. A NULL constant is present
/// only in a dictionary that can hold one, and then it sits in slot 0, the LC null index.
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
    {
        /// Slot 0 is the NULL value only in a nullable dictionary. In a non-nullable one it holds the
        /// nested type's default value, which a NULL needle does not equal, so answering from it would
        /// report every default element as a NULL.
        return low_cardinality_data.nestedIsNullable();
    }

    auto value_type_without_low_cardinality = recursiveRemoveLowCardinality(value_type);
    auto original_value = value;
    auto cast_type = target_type;
    value = castColumn({value, value_type_without_low_cardinality, ""}, target_type);

    if (value->isNullable())
    {
        value = assert_cast<const ColumnNullable &>(*value).getNestedColumnPtr();
        cast_type = removeNullable(cast_type);
    }

    const auto & dictionary = low_cardinality_data.getDictionary();

    /// The cast narrows without reporting loss, so Int8(-1) reaches the dictionary as UInt8(255) and a
    /// DateTime reaches a Date dictionary with its time of day dropped, and either would be answered
    /// from an element that the comparison this function stands for tells apart. A constant that did
    /// not survive the cast equals no element, whichever slot its image happens to hit -- the default
    /// one, which holds its value whether or not any row references it, as much as any other -- so
    /// decline before looking it up.
    /// Padding a String to a FixedString is not such a loss, and is not treated as one: the two meet
    /// as String, where the padding the cast added is trimmed back off.
    if (!target_type->equals(*value_type_without_low_cardinality)
        && !targetTypeRepresentsValue(original_value, value_type_without_low_cardinality, value, cast_type))
        return false;

    if (auto maybe_index = dictionary.getOrFindValueIndex(value->getDataAt(0)))
    {
        dictionary_index = *maybe_index;
        return true;
    }

    return false;
}

}

}
