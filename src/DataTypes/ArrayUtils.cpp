#include <DataTypes/ArrayUtils.h>
#include <Columns/ColumnArray.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

void serializeArraySizesPositionIndependent(const IColumn & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    const ColumnArray & column_array = typeid_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offset_values = column_array.getOffsets();
    size_t size = offset_values.size();

    if (!size)
        return;

    size_t end = limit && (offset + limit < size)
        ? offset + limit
        : size;

    ColumnArray::Offset prev_offset = offset_values[offset - 1];
    for (size_t i = offset; i < end; ++i)
    {
        ColumnArray::Offset current_offset = offset_values[i];
        writeIntBinary(current_offset - prev_offset, ostr);
        prev_offset = current_offset;
    }
}

void deserializeArraySizesPositionIndependent(IColumn & column, ReadBuffer & istr, UInt64 limit)
{
    ColumnArray & column_array = typeid_cast<ColumnArray &>(column);
    ColumnArray::Offsets & offset_values = column_array.getOffsets();
    size_t initial_size = offset_values.size();
    offset_values.resize(initial_size + limit);

    size_t i = initial_size;
    ColumnArray::Offset current_offset = initial_size ? offset_values[initial_size - 1] : 0;
    while (i < initial_size + limit && !istr.eof())
    {
        ColumnArray::Offset current_size = 0;
        readIntBinary(current_size, istr);
        current_offset += current_size;
        offset_values[i] = current_offset;
        ++i;
    }

    offset_values.resize(i);
}

}
