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

    if constexpr (is_decimal_field<T>)
    {
        writeChar('\'', wb);
        writeText(x.getValue(), x.getScale(), wb, {});
        writeChar('\'', wb);
    }
    else if constexpr (is_big_int_v<T>)
    {
        writeChar('\'', wb);
        writeText(x, wb);
        writeChar('\'', wb);
    }
    else
    {
        /// While `writeQuoted` sounds like it will always write the value in quotes,
        /// in fact it means: write according to the rules of the quoted format, like VALUES,
        /// where strings, dates, date-times, UUID are in quotes, and numbers are not.

        /// That's why we take extra care to put Decimal and big integers inside quotes
        /// when formatting literals in SQL language,
        /// because it is different from the quoted formats like VALUES.

        /// In fact, there are no Decimal and big integer literals in SQL,
        /// but they can appear if we format the query from a modified AST.

        /// We can fix this idiosyncrasy later.

        writeQuoted(x, wb);
    }
    return wb.str();
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
        throw Exception(ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER, "Cannot print float or double number");

    return { buffer, buffer + builder.position() };
}

String FieldVisitorToString::operator() (const Null & x) const { return x.isNegativeInfinity() ? "-Inf" : (x.isPositiveInfinity() ? "+Inf" : "NULL"); }
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
String FieldVisitorToString::operator() (const IPv4 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const IPv6 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const AggregateFunctionStateData & x) const { return formatQuoted(x.data); }
String FieldVisitorToString::operator() (const bool & x) const { return x ? "true" : "false"; }
String FieldVisitorToString::operator() (const CustomType & x) const { return x.toString(); }

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

    wb << '[';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorToString::operator() (const Object & x) const
{
    WriteBufferFromOwnString wb;

    wb << '{';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";

        writeDoubleQuoted(it->first, wb);
        wb << ": " << applyVisitor(*this, it->second);
    }
    wb << '}';

    return wb.str();

}

String convertFieldToString(const Field & field)
{
    if (field.getType() == Field::Types::Which::String)
        return field.safeGet<String>();
    return applyVisitor(FieldVisitorToString(), field);
}

}
