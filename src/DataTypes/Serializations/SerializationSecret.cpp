#include <DataTypes/Serializations/SerializationSecret.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>

#include <algorithm>
#include <string_view>

namespace DB
{

void SerializationSecret::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    size_t size = column.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    writeIntBinary(static_cast<UInt64>(limit), ostr);
}


void SerializationSecret::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    UInt64 limit_from_data;
    readIntBinary(limit_from_data, istr);
    limit = std::min<UInt64>(limit, limit_from_data);

    typeid_cast<ColumnString &>(column).insertManyDefaults(limit);
}

}
