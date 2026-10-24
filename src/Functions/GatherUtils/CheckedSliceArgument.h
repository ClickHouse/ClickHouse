#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/TypeId.h>

#include <limits>
#include <optional>

namespace DB::ErrorCodes
{
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace DB::GatherUtils
{

/// Read string-slicing offsets and lengths without interpreting large UInt64 values as negative.
class CheckedSliceArgument
{
public:
    CheckedSliceArgument(const IColumn & column_, const char * argument_name_)
        : column(column_), argument_name(argument_name_), is_uint64(column_.getDataType() == TypeIndex::UInt64)
    {
    }

    Int64 get(size_t row) const
    {
        if (!is_uint64)
            return column.getInt(row);

        const UInt64 value = column.getUInt(row);
        if (value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception(
                ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                "The {} argument of substring-like function is out of range for Int64: {}",
                argument_name, value);

        return static_cast<Int64>(value);
    }

private:
    const IColumn & column;
    const char * argument_name;
    const bool is_uint64;
};

/// Array slicing keeps its existing semantics; string functions opt into checked UInt64 arguments.
enum class SliceArgumentPolicy
{
    PreserveLegacySemantics,
    RejectOutOfRangeUInt64,
};

/// Keeps nullable/default handling identical for string and array slices.
/// Only string call sites request checked UInt64 conversion.
template <SliceArgumentPolicy policy>
class DynamicSliceArgumentReader
{
public:
    DynamicSliceArgumentReader(const IColumn * column, const char * argument_name)
    {
        if (!column || column->onlyNull())
            return;

        if (const auto * nullable = typeid_cast<const ColumnNullable *>(column))
        {
            nested_column = &nullable->getNestedColumn();
            null_map = &nullable->getNullMapData();
        }
        else
            nested_column = column;

        if constexpr (policy == SliceArgumentPolicy::RejectOutOfRangeUInt64)
            checked_reader.emplace(*nested_column, argument_name);
    }

    Int64 getOr(size_t row, Int64 default_value) const
    {
        return hasValue(row) ? get(row) : default_value;
    }

    /// The fallback is evaluated only if the argument is NULL or absent.
    template <typename DefaultGetter>
    Int64 getOrElse(size_t row, DefaultGetter get_default) const
    {
        return hasValue(row) ? get(row) : get_default();
    }

private:
    bool hasValue(size_t row) const
    {
        return nested_column && (!null_map || !(*null_map)[row]);
    }

    Int64 get(size_t row) const
    {
        if constexpr (policy == SliceArgumentPolicy::RejectOutOfRangeUInt64)
            return checked_reader->get(row);
        else
            return nested_column->getInt(row);
    }

    const IColumn * nested_column = nullptr;
    const ColumnUInt8::Container * null_map = nullptr;
    std::optional<CheckedSliceArgument> checked_reader;
};

}
