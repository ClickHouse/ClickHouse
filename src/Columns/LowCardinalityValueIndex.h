#pragma once

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

#include <limits>
#include <string_view>


namespace DB
{

enum class LowCardinalityValueLookupResult
{
    /// The dictionary does not hold strings, so the caller has to compare the values itself.
    Unsupported,
    /// No row of the column can hold the value.
    NotFound,
    Found,
};

/// Resolves `value` to its position in the dictionary of `column` and calls
/// `callback(const IndexType * indexes, IndexType value_index)`, so that comparing a row of `column`
/// with `value` becomes an integer comparison of dictionary positions.
template <typename Callback>
LowCardinalityValueLookupResult callWithLowCardinalityValueIndex(
    const ColumnLowCardinality & column, std::string_view value, Callback && callback)
{
    const auto & dictionary = column.getDictionary();
    const auto & dictionary_values = *dictionary.getNestedNotNullableColumn();
    if (!typeid_cast<const ColumnString *>(&dictionary_values) && !typeid_cast<const ColumnFixedString *>(&dictionary_values))
        return LowCardinalityValueLookupResult::Unsupported;

    auto value_index = dictionary.getOrFindValueIndex(value);
    if (!value_index)
        return LowCardinalityValueLookupResult::NotFound;

    const IColumn & indexes = column.getIndexes();

    auto call_for_index_type = [&]<typename IndexType>(IndexType) -> LowCardinalityValueLookupResult
    {
        /// A shared dictionary also holds values of other columns, so a dictionary position is not
        /// necessarily representable in the index type of this column. No row here references it then.
        if constexpr (!std::is_same_v<IndexType, UInt64>)
        {
            if (*value_index > std::numeric_limits<IndexType>::max())
                return LowCardinalityValueLookupResult::NotFound;
        }

        callback(
            assert_cast<const ColumnVector<IndexType> &>(indexes).getData().data(),
            static_cast<IndexType>(*value_index));

        return LowCardinalityValueLookupResult::Found;
    };

    switch (column.getSizeOfIndexType())
    {
        case sizeof(UInt8): return call_for_index_type(UInt8{});
        case sizeof(UInt16): return call_for_index_type(UInt16{});
        case sizeof(UInt32): return call_for_index_type(UInt32{});
        case sizeof(UInt64): return call_for_index_type(UInt64{});
        default: throwUnexpectedLowCardinalityIndexType(column.getSizeOfIndexType());
    }
}

}
