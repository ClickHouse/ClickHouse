#pragma once

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/TypeId.h>

#include <limits>

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

}
