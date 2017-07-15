#include <Columns/ColumnConst.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeNull.h>


namespace DB
{

void DataTypeNull::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    size_t size = column.size();

    if ((limit == 0) || ((offset + limit) > size))
        limit = size - offset;

    UInt8 x = 1;
    for (size_t i = 0; i < limit; ++i)
        writeBinary(x, ostr);
}

void DataTypeNull::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    ColumnNull & null_col = static_cast<ColumnNull &>(column);

    istr.ignore(sizeof(UInt8) * limit);
    null_col.insertRangeFrom(ColumnNull{0, Null()}, 0, limit);
}

ColumnPtr DataTypeNull::createColumn() const
{
    return std::make_shared<ColumnNull>(0, Null());
}

ColumnPtr DataTypeNull::createConstColumn(size_t size, const Field & field) const
{
    return std::make_shared<ColumnNull>(size, Null());
}

size_t DataTypeNull::getSizeOfField() const        /// TODO Check where it is needed.
{
    /// NULL has the size of the smallest non-null type.
    return sizeof(UInt8);
}

void DataTypeNull::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    UInt8 x = 1;    /// Value is 1 to be consistent with NULLs serialization in DataTypeNullable.
    writeBinary(x, ostr);
}

void DataTypeNull::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    UInt8 x;
    readBinary(x, istr);
    field = Null();
}

void DataTypeNull::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    UInt8 x = 1;
    writeBinary(x, ostr);
}

void DataTypeNull::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    UInt8 x;
    readBinary(x, istr);
    column.insertDefault();
}

void DataTypeNull::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeCString("\\N", ostr);
}

void DataTypeNull::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    assertString("\\N", istr);
}

void DataTypeNull::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeCString("NULL", ostr);
}

void DataTypeNull::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    assertStringCaseInsensitive("NULL", istr);
}

void DataTypeNull::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeCString("\\N", ostr);
}

void DataTypeNull::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
    assertString("\\N", istr);
}

void DataTypeNull::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeCString("NULL", ostr);
}

void DataTypeNull::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const
{
    writeCString("null", ostr);
}

void DataTypeNull::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    assertString("null", istr);
}

}
