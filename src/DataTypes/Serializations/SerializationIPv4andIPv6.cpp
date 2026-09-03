#include <Common/SipHash.h>
#include <Common/transformEndianness.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/Serializations/SerializationIPv4andIPv6.h>
#include <IO/WriteHelpers.h>

#include <bit>

namespace DB
{

template <typename IPv>
void SerializationIP<IPv>::serializeText(const DB::IColumn & column, size_t row_num, DB::WriteBuffer & ostr, const DB::FormatSettings &) const
{
    writeText(assert_cast<const ColumnVector<IPv> &>(column).getData()[row_num], ostr);
}

template <typename IPv>
void SerializationIP<IPv>::deserializeText(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, bool whole) const
{
    IPv x;
    readText(x, istr);

    assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, {TypeName<IPv>.data(), TypeName<IPv>.size()});
}

template <typename IPv>
bool SerializationIP<IPv>::tryDeserializeText(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings &, bool whole) const
{
    IPv x;
    if (!tryReadText(x, istr) || (whole && !istr.eof()))
        return false;

    assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);
    return true;
}

template <typename IPv>
void SerializationIP<IPv>::serializeTextQuoted(const DB::IColumn & column, size_t row_num, DB::WriteBuffer & ostr, const DB::FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

template <typename IPv>
void SerializationIP<IPv>::deserializeTextQuoted(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings &) const
{
    IPv x;
    assertChar('\'', istr);
    readText(x, istr);
    assertChar('\'', istr);
    assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

template <typename IPv>
bool SerializationIP<IPv>::tryDeserializeTextQuoted(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings &) const
{
    IPv x;
    if (!checkChar('\'', istr) || !tryReadText(x, istr) || !checkChar('\'', istr))
        return false;
    assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);
    return true;
}

template <typename IPv>
void SerializationIP<IPv>::serializeTextJSON(const DB::IColumn & column, size_t row_num, DB::WriteBuffer & ostr, const DB::FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

template <typename IPv>
void SerializationIP<IPv>::deserializeTextJSON(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    IPv x;
    assertChar('"', istr);
    readText(x, istr);
    /// this code looks weird, but we want to throw specific exception to match original behavior...
    if (istr.eof())
        assertChar('"', istr);
    assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);
    if (*istr.position() != '"')
        throwUnexpectedDataAfterParsedValue(column, istr, settings, {TypeName<IPv>.data(), TypeName<IPv>.size()});
    istr.ignore();
}

template <typename IPv>
bool SerializationIP<IPv>::tryDeserializeTextJSON(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings &) const
{
    IPv x;
    if (!checkChar('"', istr) || !tryReadText(x, istr) || !checkChar('"', istr))
        return false;

    assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);
    return true;
}

template <typename IPv>
void SerializationIP<IPv>::serializeTextCSV(const DB::IColumn & column, size_t row_num, DB::WriteBuffer & ostr, const DB::FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

template <typename IPv>
void SerializationIP<IPv>::deserializeTextCSV(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings &) const
{
    IPv value;
    readCSV(value, istr);

    assert_cast<ColumnVector<IPv> &>(column).getData().push_back(value);
}

template <typename IPv>
bool SerializationIP<IPv>::tryDeserializeTextCSV(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings &) const
{
    IPv value;
    if (!tryReadCSV(value, istr))
        return false;

    assert_cast<ColumnVector<IPv> &>(column).getData().push_back(value);
    return true;
}

template <typename IPv>
void SerializationIP<IPv>::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    IPv x = field.safeGet<IPv>();
    if constexpr (std::is_same_v<IPv, IPv6>)
        writeBinary(x, ostr);
    else
        writeBinaryLittleEndian(x, ostr);
}

template <typename IPv>
void SerializationIP<IPv>::deserializeBinary(DB::Field & field, DB::ReadBuffer & istr, const DB::FormatSettings &) const
{
    IPv x;
    if constexpr (std::is_same_v<IPv, IPv6>)
        readBinary(x, istr);
    else
        readBinaryLittleEndian(x, istr);
    field = NearestFieldType<IPv>(x);
}

/// The `IPv4` binary wire format is documented as unconditionally little-endian, independent of
/// host endianness (see `docs/reference/formats/RowBinary/RowBinary.mdx` and
/// `docs/reference/interfaces/specs/NativeFormat.mdx`). The `Field`-based overloads above already
/// use `writeBinaryLittleEndian`/`readBinaryLittleEndian` for `IPv4` since commit `319ae440b6b`
/// ("Implement Variant data type", 2023-12-19) - that's also the format of `MergeTree`
/// `minmax_<column>.idx` files (see `IMergeTreeDataPart::MinMaxIndex::store`). The `IColumn`-based
/// and bulk overloads below just make that pre-existing contract consistent across all paths; on
/// little-endian hosts (which is everything `ClickHouse` ships to production) the `if constexpr`
/// branches below are no-ops. This mirrors the same unconditional, unversioned pattern already used
/// by `SerializationNumber<T>` and `SerializationUUID`.
template <typename IPv>
void SerializationIP<IPv>::serializeBinary(const DB::IColumn & column, size_t row_num, DB::WriteBuffer & ostr, const DB::FormatSettings &) const
{
    IPv x = assert_cast<const ColumnVector<IPv> &>(column).getData()[row_num];
    if constexpr (std::is_same_v<IPv, IPv6>)
        writeBinary(x, ostr);
    else
        writeBinaryLittleEndian(x.toUnderType(), ostr);
}

template <typename IPv>
void SerializationIP<IPv>::deserializeBinary(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings &) const
{
    IPv x;
    if constexpr (std::is_same_v<IPv, IPv6>)
        readBinary(x.toUnderType(), istr);
    else
        readBinaryLittleEndian(x.toUnderType(), istr);
    assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);
}

template <typename IPv>
void SerializationIP<IPv>::serializeBinaryBulk(const DB::IColumn & column, DB::WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<IPv>::Container & x = typeid_cast<const ColumnVector<IPv> &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit == 0)
        return;

    /// IPv4 uses the little-endian wire format, the same as the row-wise overloads above
    /// (IPv6 is serialized as raw bytes in both).
    if constexpr (std::endian::native == std::endian::big && std::is_same_v<IPv, IPv4>)
        for (size_t i = offset; i < offset + limit; ++i)
            writeBinaryLittleEndian(x[i], ostr);
    else
        ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(IPv) * limit);
}

template <typename IPv>
void SerializationIP<IPv>::deserializeBinaryBulk(DB::IColumn & column, DB::ReadBuffer & istr, size_t limit, double) const
{
    typename ColumnVector<IPv>::Container & x = typeid_cast<ColumnVector<IPv> &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(IPv) * limit);
    x.resize(initial_size + size / sizeof(IPv));

    if constexpr (std::endian::native == std::endian::big && std::is_same_v<IPv, IPv4>)
        for (size_t i = initial_size; i < x.size(); ++i)
            transformEndianness<std::endian::big, std::endian::little>(x[i]);
}

template <typename IPv>
UInt128 SerializationIP<IPv>::getHash()
{
    SipHash hash;
    hash.update(TypeName<IPv>);
    return hash.get128();
}

template <typename IPv>
SerializationPtr SerializationIP<IPv>::create()
{
    return ISerialization::pooled(getHash(), [] { return new SerializationIP<IPv>(); });
}

template class SerializationIP<IPv4>;
template class SerializationIP<IPv6>;

}
