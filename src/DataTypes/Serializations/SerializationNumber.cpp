#include <DataTypes/Serializations/SerializationNumber.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/NaNUtils.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <ranges>

namespace DB
{

template <typename T>
void SerializationNumber<T>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], ostr);
}

template <typename T>
void SerializationNumber<T>::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    T x;

    if constexpr (is_integer<T> && is_arithmetic_v<T>)
        readIntTextUnsafe(x, istr);
    else
        readText(x, istr);

    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Number");
}

template <typename T>
bool SerializationNumber<T>::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const
{
    T x;

    if (!tryReadText(x, istr) || (whole && !istr.eof()))
        return false;

    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
    return true;
}

template <typename T>
void SerializationNumber<T>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto x = assert_cast<const ColumnVector<T> &>(column).getData()[row_num];
    writeJSONNumber(x, ostr, settings);
}

template <typename T, typename ReturnType>
ReturnType deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;
    bool has_quote = false;
    if (!istr.eof() && *istr.position() == '"')        /// We understand the number both in quotes and without.
    {
        has_quote = true;
        ++istr.position();
    }

    T x;

    /// null
    if (!has_quote && !istr.eof() && *istr.position() == 'n')
    {
        ++istr.position();
        if constexpr (throw_exception)
            assertString("ull", istr);
        else if (!checkString("ull", istr))
            return ReturnType(false);

        x = NaNOrZero<T>();
    }
    else
    {
        static constexpr bool is_uint8 = std::is_same_v<T, UInt8>;
        static constexpr bool is_int8 = std::is_same_v<T, Int8>;

        if (settings.json.read_bools_as_numbers || is_uint8 || is_int8)
        {
            // extra conditions to parse true/false strings into 1/0
            if (istr.eof())
            {
                if constexpr (throw_exception)
                    throwReadAfterEOF();
                else
                    return false;
            }

            if (*istr.position() == 't' || *istr.position() == 'f')
            {
                bool tmp = false;
                if constexpr (throw_exception)
                    readBoolTextWord(tmp, istr);
                else if (!readBoolTextWord<bool>(tmp, istr))
                    return ReturnType(false);

                x = tmp;
            }
            else
            {
                if constexpr (throw_exception)
                    readText(x, istr);
                else if (!tryReadText(x, istr))
                    return ReturnType(false);
            }
        }
        else
        {
            if constexpr (throw_exception)
                readText(x, istr);
            else if (!tryReadText(x, istr))
                return ReturnType(false);
        }

        if (has_quote)
        {
            if constexpr (throw_exception)
                assertChar('"', istr);
            else if (!checkChar('"', istr))
                return ReturnType(false);
        }
    }

    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
    return ReturnType(true);
}

template <typename T>
void SerializationNumber<T>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextJSONImpl<T, void>(column, istr, settings);
}

template <typename T>
bool SerializationNumber<T>::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return deserializeTextJSONImpl<T, bool>(column, istr, settings);
}

template <typename T>
void SerializationNumber<T>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    FieldType x;
    readCSV(x, istr);
    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

template <typename T>
bool SerializationNumber<T>::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    FieldType x;
    if (!tryReadCSV(x, istr))
        return false;
    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
    return true;
}

template <typename T>
void SerializationNumber<T>::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    /// ColumnVector<T>::ValueType is a narrower type. For example, UInt8, when the Field type is UInt64
    typename ColumnVector<T>::ValueType x = static_cast<typename ColumnVector<T>::ValueType>(field.safeGet<FieldType>());
    writeBinaryLittleEndian(x, ostr);
}

template <typename T>
void SerializationNumber<T>::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    typename ColumnVector<T>::ValueType x;
    readBinaryLittleEndian(x, istr);
    field = NearestFieldType<FieldType>(x);
}

template <typename T>
void SerializationNumber<T>::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeBinaryLittleEndian(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], ostr);
}

template <typename T>
void SerializationNumber<T>::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    typename ColumnVector<T>::ValueType x;
    readBinaryLittleEndian(x, istr);
    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

template <typename T>
void SerializationNumber<T>::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<T>::Container & x = typeid_cast<const ColumnVector<T> &>(column).getData();
    if (const size_t size = x.size(); limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit == 0)
        return;

    if constexpr (std::endian::native == std::endian::big && sizeof(T) >= 2)
        for (size_t i = offset; i < offset + limit; ++i)
            writeBinaryLittleEndian(x[i], ostr);
    else
        ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(typename ColumnVector<T>::ValueType) * limit);
}

template <typename T>
void SerializationNumber<T>::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typename ColumnVector<T>::Container & x = typeid_cast<ColumnVector<T> &>(column).getData();
    const size_t initial_size = x.size();
    x.resize(initial_size + limit);
    const size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(typename ColumnVector<T>::ValueType) * limit);
    x.resize(initial_size + size / sizeof(typename ColumnVector<T>::ValueType));

    if constexpr (std::endian::native == std::endian::big && sizeof(T) >= 2)
        for (size_t i = initial_size; i < x.size(); ++i)
            transformEndianness<std::endian::big, std::endian::little>(x[i]);
}

template class SerializationNumber<UInt8>;
template class SerializationNumber<UInt16>;
template class SerializationNumber<UInt32>;
template class SerializationNumber<UInt64>;
template class SerializationNumber<UInt128>;
template class SerializationNumber<UInt256>;
template class SerializationNumber<Int8>;
template class SerializationNumber<Int16>;
template class SerializationNumber<Int32>;
template class SerializationNumber<Int64>;
template class SerializationNumber<Int128>;
template class SerializationNumber<Int256>;
template class SerializationNumber<Float32>;
template class SerializationNumber<Float64>;

}
