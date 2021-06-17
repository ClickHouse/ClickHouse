#include <Common/FieldVisitorToString.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
}


template <typename T>
static inline String formatQuoted(T x)
{
    WriteBufferFromOwnString wb;
    writeQuoted(x, wb);
    return wb.str();
}

template <typename T>
static inline void writeQuoted(const DecimalField<T> & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeText(x.getValue(), x.getScale(), buf);
    writeChar('\'', buf);
}

/** In contrast to writeFloatText (and writeQuoted),
  *  even if number looks like integer after formatting, prints decimal point nevertheless (for example, Float64(1) is printed as 1.).
  * - because resulting text must be able to be parsed back as Float64 by query parser (otherwise it will be parsed as integer).
  *
  * Trailing zeros after decimal point are omitted.
  *
  * NOTE: Roundtrip may lead to loss of precision.
  */
static String formatFloat(const Float64 x)
{
    DoubleConverter<true>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto result = DoubleConverter<true>::instance().ToShortest(x, &builder);

    if (!result)
        throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

    return { buffer, buffer + builder.position() };
}


String FieldVisitorToString::operator() (const Null &) const { return "NULL"; }
String FieldVisitorToString::operator() (const UInt64 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int64 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Float64 & x) const { return formatFloat(x); }
String FieldVisitorToString::operator() (const String & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal32> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal64> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal128> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal256> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int128 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const UInt128 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const UInt256 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int256 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const UUID & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const AggregateFunctionStateData & x) const { return formatQuoted(x.data); }

String FieldVisitorToString::operator() (const Array & x) const
{
    WriteBufferFromOwnString wb;

    wb << '[';
    for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb.write(", ", 2);
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorToString::operator() (const Tuple & x) const
{
    WriteBufferFromOwnString wb;

    // For single-element tuples we must use the explicit tuple() function,
    // or they will be parsed back as plain literals.
    if (x.size() > 1)
    {
        wb << '(';
    }
    else
    {
        wb << "tuple(";
    }

    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorToString::operator() (const Map & x) const
{
    WriteBufferFromOwnString wb;

    wb << '(';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

}

