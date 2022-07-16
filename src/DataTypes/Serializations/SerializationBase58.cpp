#include <DataTypes/Serializations/SerializationBase58.h>

#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Formats/FormatSettings.h>
#include <Common/base58.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
    extern const int ILLEGAL_COLUMN;
}

SerializationBase58::SerializationBase58(const SerializationPtr & nested_) : SerializationCustomSimpleText(nested_)
{
}

void SerializationBase58::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const ColumnString * col = checkAndGetColumn<ColumnString>(&column);
    if (!col)
    {
        throw Exception("Base58 type can only serialize columns of type String." + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    auto value = col->getDataAtWithTerminatingZero(row_num);
    char buffer[value.size * 2 + 1];
    char * ptr = buffer;
    encodeBase58(reinterpret_cast<const char8_t *>(value.data), reinterpret_cast<char8_t *>(ptr));
    ostr.write(buffer, strlen(buffer));
}

void SerializationBase58::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    ColumnString * col = typeid_cast<ColumnString *>(&column);
    if (!col)
    {
        throw Exception("Base58 type can only deserialize columns of type String." + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    size_t allocated = 32;
    std::string encoded(allocated, '\0');

    size_t read_position = 0;
    while (istr.read(encoded[read_position]))
    {
        ++read_position;
        if (read_position == allocated)
        {
            allocated *= 2;
            encoded.resize(allocated, '\0');
        }
    }

    char buffer[read_position + 1];
    if (!decodeBase58(reinterpret_cast<const char8_t *>(encoded.c_str()), reinterpret_cast<char8_t *>(buffer)))
    {
        throw Exception("Invalid Base58 encoded value, cannot parse." + column.getName(), ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING);
    }

    col->insertDataWithTerminatingZero(buffer, read_position+1);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Base58");
}
}
