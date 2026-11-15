#include <DataTypes/Serializations/SerializationNothing.h>
#include <Columns/ColumnNothing.h>
#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

void SerializationNothing::throwNoSerialization()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization is not implemented for type Nothing");
}

void SerializationNothing::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    size_t size = column.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    for (size_t i = 0; i < limit; ++i)
        ostr.write('0');
}

void SerializationNothing::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double /*avg_value_size_hint*/) const
{
    istr.ignore(rows_offset);
    typeid_cast<ColumnNothing &>(column).addSize(istr.tryIgnore(limit));
}

}
