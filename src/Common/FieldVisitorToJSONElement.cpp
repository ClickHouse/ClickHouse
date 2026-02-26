#include <Common/FieldVisitorToJSONElement.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

static inline String formatString(const String & x)
{
    WriteBufferFromOwnString wb;
    writeJSONString(x, wb, FormatSettings{});
    return wb.str();
}

template <typename T>
static inline String formatAsString(T x)
{
    WriteBufferFromOwnString wb;
    wb << "\"";
    writeText(x, wb);
    wb << "\"";
    return wb.str();
}

template <typename T>
static inline String formatNumber(T x)
{
    WriteBufferFromOwnString wb;
    FormatSettings settings;
    settings.json.quote_64bit_integers = false;
    writeJSONNumber(x, wb, settings);
    return wb.str();
}

String FieldVisitorToJSONElement::operator() (const Null & x) const { return x.isNegativeInfinity() ? "\"-Inf\"" : (x.isPositiveInfinity() ? "\"+Inf\"" : "null"); }
String FieldVisitorToJSONElement::operator() (const UInt64 & x) const { return formatNumber(x); }
String FieldVisitorToJSONElement::operator() (const Int64 & x) const { return formatNumber(x); }
String FieldVisitorToJSONElement::operator() (const Float64 & x) const { return formatNumber(x); }
String FieldVisitorToJSONElement::operator() (const String & x) const { return formatString(x); }
String FieldVisitorToJSONElement::operator() (const DecimalField<Decimal32> & x) const { return formatNumber(x); }
String FieldVisitorToJSONElement::operator() (const DecimalField<Decimal64> & x) const { return formatNumber(x); }
String FieldVisitorToJSONElement::operator() (const DecimalField<Decimal128> & x) const { return formatNumber(x); }
String FieldVisitorToJSONElement::operator() (const DecimalField<Decimal256> & x) const { return formatNumber(x); }
String FieldVisitorToJSONElement::operator() (const Int128 & x) const { return formatAsString(x); }
String FieldVisitorToJSONElement::operator() (const UInt128 & x) const { return formatAsString(x); }
String FieldVisitorToJSONElement::operator() (const UInt256 & x) const { return formatAsString(x); }
String FieldVisitorToJSONElement::operator() (const Int256 & x) const { return formatAsString(x); }
String FieldVisitorToJSONElement::operator() (const UUID & x) const { return formatAsString(x); }
String FieldVisitorToJSONElement::operator() (const IPv4 & x) const { return formatAsString(x); }
String FieldVisitorToJSONElement::operator() (const IPv6 & x) const { return formatAsString(x); }
String FieldVisitorToJSONElement::operator() (const AggregateFunctionStateData & x) const { return formatAsString(x.data); }
String FieldVisitorToJSONElement::operator() (const bool & x) const { return x ? "true" : "false"; }
String FieldVisitorToJSONElement::operator() (const CustomType & x) const { return formatString(x.toString()); }

String FieldVisitorToJSONElement::operator() (const Array & x) const
{
    WriteBufferFromOwnString wb;

    wb << '[';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb.write(", ", 2);
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorToJSONElement::operator() (const Tuple & x) const
{
    WriteBufferFromOwnString wb;

    wb << '[';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb.write(", ", 2);
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorToJSONElement::operator() (const Map & x) const
{
    WriteBufferFromOwnString wb;

    wb << '{';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        auto pair = it->safeGet<Tuple>();
        wb << formatString(toString(pair[0]));
        wb << ": " << applyVisitor(*this, pair[1]);
    }
    wb << '}';

    return wb.str();
}

String FieldVisitorToJSONElement::operator() (const Object & x) const
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

}
