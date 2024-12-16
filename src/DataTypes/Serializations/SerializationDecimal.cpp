#include <DataTypes/Serializations/SerializationDecimal.h>

#include <Columns/ColumnVector.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/readDecimalText.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
}

template <typename T>
bool SerializationDecimal<T>::tryReadText(T & x, ReadBuffer & istr, UInt32 precision, UInt32 scale, bool csv)
{
    UInt32 unread_scale = scale;
    if (csv)
    {
        if (!tryReadCSVDecimalText(istr, x, precision, unread_scale))
            return false;
    }
    else
    {
        if (!tryReadDecimalText(istr, x, precision, unread_scale))
            return false;
    }

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
        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
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
bool SerializationDecimal<T>::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const
{
    T x;
    if (!tryReadText(x, istr) || (whole && !istr.eof()))
        return false;
    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

template <typename T>
void SerializationDecimal<T>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    T x;
    readText(x, istr, true);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

template <typename T>
bool SerializationDecimal<T>::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    T x;
    if (!tryReadText(x, istr, true))
        return false;
    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

template <typename T>
void SerializationDecimal<T>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.json.quote_decimals)
        writeChar('"', ostr);

    serializeText(column, row_num, ostr, settings);

    if (settings.json.quote_decimals)
        writeChar('"', ostr);
}

template <typename T>
void SerializationDecimal<T>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    bool have_quotes = checkChar('"', istr);
    deserializeText(column, istr, settings, false);
    if (have_quotes)
        assertChar('"', istr);
}

template <typename T>
bool SerializationDecimal<T>::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    bool have_quotes = checkChar('"', istr);
    T x;
    if (!tryReadText(x, istr) || (have_quotes && !checkChar('"', istr)))
        return false;

    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}


template class SerializationDecimal<Decimal32>;
template class SerializationDecimal<Decimal64>;
template class SerializationDecimal<Decimal128>;
template class SerializationDecimal<Decimal256>;

}
