#include <Columns/ColumnsNumber.h>
#include <Common/SipHash.h>
#include <Common/transformEndianness.h>
#include <Core/UUID.h>
#include <DataTypes/Serializations/SerializationUUID2.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>

namespace DB
{

/** `UUID2` stores the value as a big-endian integer of the 16 canonical bytes (so that natural integer comparison
  * matches the lexicographic order). Text serialization uses the same canonical textual form as `UUID`, so we
  * convert the stored value to the `UUID` layout with `UUIDHelpers::swapHalves` before formatting, and back after
  * parsing. Binary serialization writes the canonical big-endian byte order.
  */

UInt128 SerializationUUID2::getHash()
{
    SipHash hash;
    hash.update("UUID2");
    return hash.get128();
}

SerializationPtr SerializationUUID2::create()
{
    return ISerialization::pooled(getHash(), [] { return new SerializationUUID2(); });
}

void SerializationUUID2::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(UUIDHelpers::swapHalves(assert_cast<const ColumnUUID &>(column).getData()[row_num]), ostr);
}

void SerializationUUID2::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    UUID x;
    readText(x, istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(UUIDHelpers::swapHalves(x));

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "UUID2");
}

bool SerializationUUID2::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const
{
    UUID x;
    if (!tryReadText(x, istr) || (whole && !istr.eof()))
        return false;

    assert_cast<ColumnUUID &>(column).getData().push_back(UUIDHelpers::swapHalves(x));
    return true;
}

void SerializationUUID2::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationUUID2::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
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

    assert_cast<ColumnUUID &>(column).getData().push_back(UUIDHelpers::swapHalves(uuid)); /// It's important to do this at the end - for exception safety.
}

bool SerializationUUID2::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID uuid;
    if (!checkChar('\'', istr) || !tryReadText(uuid, istr) || !checkChar('\'', istr))
        return false;

    assert_cast<ColumnUUID &>(column).getData().push_back(UUIDHelpers::swapHalves(uuid));
    return true;
}

void SerializationUUID2::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationUUID2::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    assertChar('"', istr);
    readText(x, istr);
    assertChar('"', istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(UUIDHelpers::swapHalves(x));
}

bool SerializationUUID2::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    if (!checkChar('"', istr) || !tryReadText(x, istr) || !checkChar('"', istr))
        return false;
    assert_cast<ColumnUUID &>(column).getData().push_back(UUIDHelpers::swapHalves(x));
    return true;
}

void SerializationUUID2::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationUUID2::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID value;
    readCSV(value, istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(UUIDHelpers::swapHalves(value));
}

bool SerializationUUID2::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID value;
    if (!tryReadCSV(value, istr))
        return false;
    assert_cast<ColumnUUID &>(column).getData().push_back(UUIDHelpers::swapHalves(value));
    return true;
}

void SerializationUUID2::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    UUID x = field.safeGet<UUID>();
    writeBinaryBigEndian(x, ostr);
}

void SerializationUUID2::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    readBinaryBigEndian(x, istr);
    field = NearestFieldType<UUID>(x);
}

void SerializationUUID2::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeBinaryBigEndian(assert_cast<const ColumnVector<UUID> &>(column).getData()[row_num], ostr);
}

void SerializationUUID2::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    readBinaryBigEndian(x, istr);
    assert_cast<ColumnVector<UUID> &>(column).getData().push_back(x);
}

void SerializationUUID2::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<UUID>::Container & x = typeid_cast<const ColumnVector<UUID> &>(column).getData();
    if (const size_t size = x.size(); limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit == 0)
        return;

    /// The value is stored as a big-endian integer, so its canonical byte order requires an endianness transform
    /// on little-endian platforms; unlike `UUID`, there is no raw-memory fast path here.
    for (size_t i = offset; i < offset + limit; ++i)
        writeBinaryBigEndian(x[i], ostr);
}

void SerializationUUID2::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typename ColumnVector<UUID>::Container & x = typeid_cast<ColumnVector<UUID> &>(column).getData();
    const size_t initial_size = x.size();
    x.resize(initial_size + limit);
    const size_t size = istr.readBig(reinterpret_cast<char *>(&x[initial_size]), sizeof(UUID) * limit);
    x.resize(initial_size + size / sizeof(UUID));

    /// On-disk bytes are in canonical big-endian order; convert them to the native representation of the value.
    for (size_t i = initial_size; i < x.size(); ++i)
        transformEndianness<std::endian::native, std::endian::big>(x[i]);
}

}
