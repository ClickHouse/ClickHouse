#include <Common/SipHash.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/Serializations/SerializationVersion.h>
#include <IO/WriteHelpers.h>

namespace DB
{

void SerializationVersion::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(assert_cast<const ColumnVector<Version> &>(column).getData()[row_num], ostr);
}

void SerializationVersion::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    Version x;
    readText(x, istr);

    assert_cast<ColumnVector<Version> &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, {TypeName<Version>.data(), TypeName<Version>.size()});
}

bool SerializationVersion::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const
{
    Version x;
    if (!tryReadText(x, istr) || (whole && !istr.eof()))
        return false;

    assert_cast<ColumnVector<Version> &>(column).getData().push_back(x);
    return true;
}

void SerializationVersion::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationVersion::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    Version x;
    assertChar('\'', istr);
    readText(x, istr);
    assertChar('\'', istr);
    assert_cast<ColumnVector<Version> &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

bool SerializationVersion::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    Version x;
    if (!checkChar('\'', istr) || !tryReadText(x, istr) || !checkChar('\'', istr))
        return false;
    assert_cast<ColumnVector<Version> &>(column).getData().push_back(x);
    return true;
}

void SerializationVersion::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationVersion::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    Version x;
    assertChar('"', istr);
    readText(x, istr);
    /// this code looks weird, but we want to throw specific exception to match original behavior...
    if (istr.eof())
        assertChar('"', istr);
    assert_cast<ColumnVector<Version> &>(column).getData().push_back(x);
    if (*istr.position() != '"')
        throwUnexpectedDataAfterParsedValue(column, istr, settings, {TypeName<Version>.data(), TypeName<Version>.size()});
    istr.ignore();
}

bool SerializationVersion::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    Version x;
    if (!checkChar('"', istr) || !tryReadText(x, istr) || !checkChar('"', istr))
        return false;

    assert_cast<ColumnVector<Version> &>(column).getData().push_back(x);
    return true;
}

void SerializationVersion::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationVersion::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    Version value;
    readCSV(value, istr);

    assert_cast<ColumnVector<Version> &>(column).getData().push_back(value);
}

bool SerializationVersion::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    Version value;
    if (!tryReadCSV(value, istr))
        return false;

    assert_cast<ColumnVector<Version> &>(column).getData().push_back(value);
    return true;
}

/// NOTE (implementer decision, see rule 7): unlike SerializationIP<IPv>, which branches between
/// writeBinary (IPv6) and writeBinaryLittleEndian (IPv4) for the Field-based overload, Version has
/// no legacy on-disk format to preserve and no network-byte-order concern (see base/base/Version.h
/// and the packing_notes in the plan). We therefore consistently use plain writeBinary/readBinary,
/// matching how UInt128-backed StrongTypedef-likes such as IPv6 already behave in that same file.
/// This is a deliberate simplification for this POC and should be revisited before this leaves
/// prototype status if a stable on-disk binary format for Version needs to be guaranteed.
void SerializationVersion::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    Version x = field.safeGet<Version>();
    writeBinary(x, ostr);
}

void SerializationVersion::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    Version x;
    readBinary(x, istr);
    field = NearestFieldType<Version>(x);
}

void SerializationVersion::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeBinary(assert_cast<const ColumnVector<Version> &>(column).getData()[row_num], ostr);
}

void SerializationVersion::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    Version x;
    readBinary(x.toUnderType(), istr);
    assert_cast<ColumnVector<Version> &>(column).getData().push_back(x);
}

void SerializationVersion::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnVector<Version>::Container & x = typeid_cast<const ColumnVector<Version> &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(Version) * limit);
}

void SerializationVersion::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double) const
{
    ColumnVector<Version>::Container & x = typeid_cast<ColumnVector<Version> &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    istr.ignore(sizeof(Version) * rows_offset);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(Version) * limit);
    x.resize(initial_size + size / sizeof(Version));
}

UInt128 SerializationVersion::getHash()
{
    SipHash hash;
    hash.update(TypeName<Version>);
    return hash.get128();
}

SerializationPtr SerializationVersion::create()
{
    return ISerialization::pooled(getHash(), [] { return new SerializationVersion(); });
}

}
