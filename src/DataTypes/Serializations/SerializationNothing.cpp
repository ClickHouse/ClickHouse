#include <DataTypes/Serializations/SerializationNothing.h>
#include <Columns/ColumnNothing.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{

void SerializationNothing::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    size_t size = column.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    for (size_t i = 0; i < limit; ++i)
        ostr.write('0');
}

void SerializationNothing::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typeid_cast<ColumnNothing &>(column).addSize(istr.tryIgnore(limit));
}

}
