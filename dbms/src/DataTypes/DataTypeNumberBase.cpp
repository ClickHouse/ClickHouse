#include <type_traits>
#include <DataTypes/DataTypeNumberBase.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/NaNUtils.h>
#include <Common/typeid_cast.h>
#include <Formats/FormatSettings.h>


namespace DB
{

template <typename T>
void DataTypeNumberBase<T>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(static_cast<const ColumnVector<T> &>(column).getData()[row_num], ostr);
}

template <typename T>
void DataTypeNumberBase<T>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}


template <typename T>
static void deserializeText(IColumn & column, ReadBuffer & istr)
{
    T x;

    if constexpr (std::is_integral_v<T> && std::is_arithmetic_v<T>)
        readIntTextUnsafe(x, istr);
    else
        readText(x, istr);

    static_cast<ColumnVector<T> &>(column).getData().push_back(x);
}


template <typename T>
void DataTypeNumberBase<T>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeText<T>(column, istr);
}

template <typename T>
void DataTypeNumberBase<T>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    deserializeText<T>(column, istr);
}


template <typename T>
static inline void writeDenormalNumber(T x, WriteBuffer & ostr)
{
    if constexpr (std::is_floating_point_v<T>)
    {
        if (std::signbit(x))
        {
            if (isNaN(x))
                writeCString("-nan", ostr);
            else
                writeCString("-inf", ostr);
        }
        else
        {
            if (isNaN(x))
                writeCString("nan", ostr);
            else
                writeCString("inf", ostr);
        }
    }
    else
    {
        /// This function is not called for non floating point numbers.
        (void)x;
    }
}


template <typename T>
void DataTypeNumberBase<T>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto x = static_cast<const ColumnVector<T> &>(column).getData()[row_num];
    bool is_finite = isFinite(x);

    const bool need_quote = (std::is_integral_v<T> && (sizeof(T) == 8) && settings.json.quote_64bit_integers)
        || (settings.json.quote_denormals && !is_finite);

    if (need_quote)
        writeChar('"', ostr);

    if (is_finite)
        writeText(x, ostr);
    else if (!settings.json.quote_denormals)
        writeCString("null", ostr);
    else
        writeDenormalNumber(x, ostr);

    if (need_quote)
        writeChar('"', ostr);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    bool has_quote = false;
    if (!istr.eof() && *istr.position() == '"')        /// We understand the number both in quotes and without.
    {
        has_quote = true;
        ++istr.position();
    }

    FieldType x;

    /// null
    if (!has_quote && !istr.eof() && *istr.position() == 'n')
    {
        ++istr.position();
        assertString("ull", istr);

        x = NaNOrZero<T>();
    }
    else
    {
        static constexpr bool is_uint8 = std::is_same_v<T, UInt8>;
        static constexpr bool is_int8 = std::is_same_v<T, Int8>;

        if (is_uint8 || is_int8)
        {
            // extra conditions to parse true/false strings into 1/0
            if (istr.eof())
                throwReadAfterEOF();
            if (*istr.position() == 't' || *istr.position() == 'f')
            {
                bool tmp = false;
                readBoolTextWord(tmp, istr);
                x = tmp;
            }
            else
                readText(x, istr);
        }
        else
        {
            readText(x, istr);
        }

        if (has_quote)
            assertChar('"', istr);
    }

    static_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

template <typename T>
void DataTypeNumberBase<T>::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    FieldType x;
    readCSV(x, istr);
    static_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

template <typename T>
Field DataTypeNumberBase<T>::getDefault() const
{
    return typename NearestFieldType<FieldType>::Type();
}

template <typename T>
void DataTypeNumberBase<T>::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    /// ColumnVector<T>::value_type is a narrower type. For example, UInt8, when the Field type is UInt64
    typename ColumnVector<T>::value_type x = get<typename NearestFieldType<FieldType>::Type>(field);
    writeBinary(x, ostr);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    typename ColumnVector<T>::value_type x;
    readBinary(x, istr);
    field = typename NearestFieldType<FieldType>::Type(x);
}

template <typename T>
void DataTypeNumberBase<T>::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeBinary(static_cast<const ColumnVector<T> &>(column).getData()[row_num], ostr);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    typename ColumnVector<T>::value_type x;
    readBinary(x, istr);
    static_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

template <typename T>
void DataTypeNumberBase<T>::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<T>::Container & x = typeid_cast<const ColumnVector<T> &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(typename ColumnVector<T>::value_type) * limit);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typename ColumnVector<T>::Container & x = typeid_cast<ColumnVector<T> &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(typename ColumnVector<T>::value_type) * limit);
    x.resize(initial_size + size / sizeof(typename ColumnVector<T>::value_type));
}

template <typename T>
MutableColumnPtr DataTypeNumberBase<T>::createColumn() const
{
    return ColumnVector<T>::create();
}

template <typename T>
bool DataTypeNumberBase<T>::isValueRepresentedByInteger() const
{
    return std::is_integral_v<T>;
}

template <typename T>
bool DataTypeNumberBase<T>::isValueRepresentedByUnsignedInteger() const
{
    return std::is_integral_v<T> && std::is_unsigned_v<T>;
}


/// Explicit template instantiations - to avoid code bloat in headers.
template class DataTypeNumberBase<UInt8>;
template class DataTypeNumberBase<UInt16>;
template class DataTypeNumberBase<UInt32>;
template class DataTypeNumberBase<UInt64>;
template class DataTypeNumberBase<UInt128>;
template class DataTypeNumberBase<Int8>;
template class DataTypeNumberBase<Int16>;
template class DataTypeNumberBase<Int32>;
template class DataTypeNumberBase<Int64>;
template class DataTypeNumberBase<Float32>;
template class DataTypeNumberBase<Float64>;

}
