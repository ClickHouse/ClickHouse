#include <DataTypes/Serializations/SerializationDecimal.h>

#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/readDecimalText.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
}

template <typename T>
bool SerializationDecimal<T>::tryReadText(T & x, ReadBuffer & istr, UInt32 precision, UInt32 scale)
{
    UInt32 unread_scale = scale;
    if (!tryReadDecimalText(istr, x, precision, unread_scale))
        return false;

    if (common::mulOverflow(x.value, DecimalUtils::scaleMultiplier<T>(unread_scale), x.value))
        return false;

    return true;
}

template <typename T>
void SerializationDecimal<T>::readText(T & x, ReadBuffer & istr, UInt32 precision, UInt32 scale, bool csv)
{
    UInt32 unread_scale = scale;
    if (csv)
        readCSVDecimalText(istr, x, precision, unread_scale);
    else
        readDecimalText(istr, x, precision, unread_scale);

    if (common::mulOverflow(x.value, DecimalUtils::scaleMultiplier<T>(unread_scale), x.value))
        throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
}

template <typename T>
void SerializationDecimal<T>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    T value = assert_cast<const ColumnType &>(column).getData()[row_num];
    writeText(value, this->scale, ostr, settings.decimal_trailing_zeros);
}

template <typename T>
void SerializationDecimal<T>::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    T x;
    readText(x, istr);
    assert_cast<ColumnType &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        ISerialization::throwUnexpectedDataAfterParsedValue(column, istr, settings, "Decimal");
}

template <typename T>
void SerializationDecimal<T>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    T x;
    readText(x, istr, true);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

template class SerializationDecimal<Decimal32>;
template class SerializationDecimal<Decimal64>;
template class SerializationDecimal<Decimal128>;
template class SerializationDecimal<Decimal256>;

}
