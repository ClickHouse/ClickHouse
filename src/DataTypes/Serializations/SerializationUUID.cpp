#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationUUID.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>

#include <ranges>

namespace DB
{

void SerializationUUID::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(assert_cast<const ColumnUUID &>(column).getData()[row_num], ostr);
}

void SerializationUUID::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    UUID x;
    readText(x, istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "UUID");
}

void SerializationUUID::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeText(column, istr, settings, false);
}

void SerializationUUID::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationUUID::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationUUID::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID uuid;
    bool fast = false;
    if (istr.available() >= 38)
    {
        assertChar('\'', istr);
        char * next_pos = find_first_symbols<'\\', '\''>(istr.position(), istr.buffer().end());
        const size_t len = next_pos - istr.position();
        if ((len == 32 || len == 36) && istr.position()[len] == '\'')
        {
            uuid = parseUUID(std::span(reinterpret_cast<const UInt8 *>(istr.position()), len));
            istr.ignore(len + 1);
            fast = true;
        }
        else
        {
            // It's ok to go back in the position because we haven't read from the buffer except the first char
            // and we know there were at least 38 bytes available (so no new read has been triggered)
            istr.position()--;
        }
    }

    if (!fast)
    {
        String quoted_chars;
        readQuotedStringInto<false>(quoted_chars, istr);
        ReadBufferFromString parsed_quoted_buffer(quoted_chars);
        readText(uuid, parsed_quoted_buffer);
    }

    assert_cast<ColumnUUID &>(column).getData().push_back(std::move(uuid)); /// It's important to do this at the end - for exception safety.
}

void SerializationUUID::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationUUID::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    assertChar('"', istr);
    readText(x, istr);
    assertChar('"', istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(x);
}

void SerializationUUID::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationUUID::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID value;
    readCSV(value, istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(value);
}


void SerializationUUID::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    UUID x = field.get<UUID>();
    writeBinaryLittleEndian(x, ostr);
}

void SerializationUUID::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    readBinaryLittleEndian(x, istr);
    field = NearestFieldType<UUID>(x);
}

void SerializationUUID::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeBinaryLittleEndian(assert_cast<const ColumnVector<UUID> &>(column).getData()[row_num], ostr);
}

void SerializationUUID::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    readBinaryLittleEndian(x, istr);
    assert_cast<ColumnVector<UUID> &>(column).getData().push_back(x);
}

void SerializationUUID::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<UUID>::Container & x = typeid_cast<const ColumnVector<UUID> &>(column).getData();
    if (const size_t size = x.size(); limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit == 0)
        return;

    if constexpr (std::endian::native == std::endian::big)
    {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunreachable-code"
        std::ranges::for_each(
            x | std::views::drop(offset) | std::views::take(limit), [&ostr](const auto & uuid) { writeBinaryLittleEndian(uuid, ostr); });
#pragma clang diagnostic pop
    }
    else
        ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(UUID) * limit);
}

void SerializationUUID::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typename ColumnVector<UUID>::Container & x = typeid_cast<ColumnVector<UUID> &>(column).getData();
    const size_t initial_size = x.size();
    x.resize(initial_size + limit);
    const size_t size = istr.readBig(reinterpret_cast<char *>(&x[initial_size]), sizeof(UUID) * limit);
    x.resize(initial_size + size / sizeof(UUID));

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunreachable-code"
    if constexpr (std::endian::native == std::endian::big)
        std::ranges::for_each(
            x | std::views::drop(initial_size), [](auto & uuid) { transformEndianness<std::endian::big, std::endian::little>(uuid); });
#pragma clang diagnostic pop
}
}
