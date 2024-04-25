#include "CBORReader.h"

#if USE_CBOR

#include <Columns/ColumnTuple.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int ILLEGAL_COLUMN;
}

CBORReader::CBORReader(ReadBuffer & in_)
    : buf(in_), cbor_reader(reinterpret_cast<unsigned char *>(buf.buffer().begin()), static_cast<int>(buf.buffer().size()))
{
}

CBORReader::~CBORReader() = default;

size_t CBORReader::extractBytesCountFromMinorType(unsigned char minor_type)
{
    if (minor_type < 24)
        return 0;
    else if (minor_type == 24)
        return sizeof(unsigned char);
    else if (minor_type == 25)
        return sizeof(unsigned short);
    else if (minor_type == 26)
        return sizeof(unsigned int);
    else if (minor_type == 27)
        return sizeof(unsigned long long);
    else
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: invalid minor type");
}

uint64_t CBORReader::extractValueFromMinorType(unsigned char minor_type)
{
    if (minor_type < 24)
        return minor_type;
    else if (minor_type == 24)
        return cbor_reader.get_byte();
    else if (minor_type == 25)
        return cbor_reader.get_short();
    else if (minor_type == 26)
        return cbor_reader.get_int();
    else if (minor_type == 27)
        return cbor_reader.get_long();
    else
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: invalid minor type");
}

size_t CBORReader::getArraySize(unsigned char minor_type)
{
    if (minor_type == 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: extra long array.");
    else if (minor_type > 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: invalid array type.");
    return extractValueFromMinorType(minor_type);
}

size_t CBORReader::getMapSize(unsigned char minor_type)
{
    if (minor_type == 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: extra long map.");
    if (minor_type > 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: invalid map type.");
    return extractValueFromMinorType(minor_type);
}

size_t CBORReader::getIntegerBytesCount(unsigned char minor_type)
{
    if (minor_type > 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: invalid integer type");
    extractValueFromMinorType(minor_type);
    return extractBytesCountFromMinorType(minor_type);
}

size_t CBORReader::getByteStringBytesCount(unsigned char minor_type)
{
    if (minor_type == 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: extra long bytes");
    if (minor_type > 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: invalid bytes type");
    size_t bytes_count = extractValueFromMinorType(minor_type);
    return bytes_count + extractBytesCountFromMinorType(minor_type);
}

size_t CBORReader::getStringBytesCount(unsigned char minor_type)
{
    if (minor_type == 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: extra long string");
    if (minor_type > 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: invalid string type");
    size_t bytes_count = extractValueFromMinorType(minor_type);
    return bytes_count + extractBytesCountFromMinorType(minor_type);
}

size_t CBORReader::getArrayBytesCount(unsigned char minor_type)
{
    size_t array_size = getArraySize(minor_type);
    size_t bytes_count = 0;
    for (size_t i = 0; i < array_size; ++i)
    {
        if (!cbor_reader.has_bytes(1))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Not supported CBOR data schema.");
        bytes_count += getTypeBytesCount(cbor_reader.get_byte());
    }
    return bytes_count + extractBytesCountFromMinorType(minor_type);
}

size_t CBORReader::getMapBytesCount(unsigned char minor_type)
{
    size_t map_size = getMapSize(minor_type);
    size_t bytes_count = 0;
    for (size_t i = 0; i < map_size; ++i)
    {
        if (!cbor_reader.has_bytes(1))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Not supported CBOR data schema.");
        bytes_count += getTypeBytesCount(cbor_reader.get_byte());
        if (!cbor_reader.has_bytes(1))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Not supported CBOR data schema.");
        bytes_count += getTypeBytesCount(cbor_reader.get_byte());
    }
    return bytes_count + extractBytesCountFromMinorType(minor_type);
}

size_t CBORReader::getTagBytesCount(unsigned char minor_type)
{
    if (minor_type > 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: invalid tag type");
    extractValueFromMinorType(minor_type);
    size_t bytes_count = extractBytesCountFromMinorType(minor_type);
    bytes_count += getTypeBytesCount(cbor_reader.get_byte());
    return bytes_count;
}

size_t CBORReader::getSpecialBytesCount(unsigned char minor_type)
{
    if (minor_type > 27)
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: invalid special type");
    extractValueFromMinorType(minor_type);
    return extractBytesCountFromMinorType(minor_type);
}

enum CBORMajorTypes : uint8_t
{
    POSITIVE_INTEGER = 0,
    NEGATIVE_INTEGER = 1,
    BYTES = 2,
    STRING = 3,
    ARRAY = 4,
    MAP = 5,
    TAG = 6,
    SPECIAL = 7
};

size_t CBORReader::getTypeBytesCount(unsigned char type)
{
    unsigned char major_type = type >> 5;
    unsigned char minor_type = static_cast<unsigned char>(type & 31);
    // Result is bytes for object + 1, because in all case it need one byte for type marker
    if (major_type == CBORMajorTypes::POSITIVE_INTEGER)
        return getIntegerBytesCount(minor_type) + 1;
    else if (major_type == CBORMajorTypes::NEGATIVE_INTEGER)
        return getIntegerBytesCount(minor_type) + 1;
    else if (major_type == CBORMajorTypes::BYTES)
        return getByteStringBytesCount(minor_type) + 1;
    else if (major_type == CBORMajorTypes::STRING)
        return getStringBytesCount(minor_type) + 1;
    else if (major_type == CBORMajorTypes::ARRAY)
        return getArrayBytesCount(minor_type) + 1;
    else if (major_type == CBORMajorTypes::MAP)
        return getMapBytesCount(minor_type) + 1;
    else if (major_type == CBORMajorTypes::TAG)
        return getTagBytesCount(minor_type) + 1;
    else if (major_type == CBORMajorTypes::SPECIAL)
        return getSpecialBytesCount(minor_type) + 1;
    else
        throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: invalid major type");
}

void CBORReader::readAndCheckPrefix()
{
    if (!cbor_reader.has_bytes(1))
        throw Exception(ErrorCodes::INCORRECT_DATA, "There is no data in the buffer to read.");
    unsigned char byte = cbor_reader.get_byte();
    ++buf.position();
    if (byte != 0x9F)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not supported CBOR data schema.");
}

bool CBORReader::readRow(size_t expected_size, cbor::listener & listener)
{
    // Check that first byte is byte of array
    if (!cbor_reader.has_bytes(1) || buf.eof())
        return false;
    unsigned char type = cbor_reader.get_byte();
    ++buf.position();
    // if it is a break marker -> stop reading rows
    if (type == 0xFF)
        return false;

    unsigned char major_type = type >> 5;
    unsigned char minor_type = static_cast<unsigned char>(type & 31);
    if (major_type != 4)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not supported CBOR data schema.");

    // If it is array, get size of it in elements
    size_t array_size = getArraySize(minor_type);

    // TODO: Perhaps in this case it is worth cutting off unnecessary elements or adding missing ones by default?
    if (array_size != expected_size)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "The number of elements in the CBOR row does not match the number of elements in the ClickHouse column.");

    size_t offset = getArrayBytesCount(array_size);
    // Count the number of bytes until the next row
    cbor::input input(buf.position(), static_cast<int>(offset));
    cbor::decoder decoder(input, listener);
    decoder.run();
    buf.position() += offset;
    return true;
}

}

#endif
