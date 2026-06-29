#include <Common/FieldVisitorToCastedLiteral.h>

#include <Common/FieldVisitorToString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <cmath>


namespace DB
{


FieldVisitorToCastedLiteral::FieldVisitorToCastedLiteral(bool skip_unambiguous_cast_)
    : skip_unambiguous_cast(skip_unambiguous_cast_)
{
}

String FieldVisitorToCastedLiteral::operator() (const Null & x) const
{
    /// `NULL` already parses unambiguously as a Null Field.
    /// We don't add anything to it even of skip_unambiguous_cast == false
    /// because NULL::Nullable(Nothing) looks excessive.
    return FieldVisitorToString()(x);
}

String FieldVisitorToCastedLiteral::operator() (const UInt64 & x) const
{
    String out = FieldVisitorToString()(x);
    if (!skip_unambiguous_cast)
        out += "::UInt64";
    return out;
}

String FieldVisitorToCastedLiteral::operator() (const Int64 & x) const
{
    String out = FieldVisitorToString()(x);
    /// Positive Int64 would be parsed back as UInt64; so we need to add the suffix "::Int64".
    if (!skip_unambiguous_cast || x >= 0)
        out += "::Int64";
    return out;
}

String FieldVisitorToCastedLiteral::operator() (const Float64 & x) const
{
    String out = FieldVisitorToString()(x);
    /// FieldVisitorToString should emit a decimal point or exponent for finite Float64,
    /// it's important here because the trailing decimal point keeps the SQL parser from re-parsing
    /// the literal as an integer.
    chassert(!std::isfinite(x) || out.find('.') != String::npos || out.find('e') != String::npos || out.find('E') != String::npos);
    if (!skip_unambiguous_cast)
        out += "::Float64";
    return out;
}

String FieldVisitorToCastedLiteral::operator() (const String & x) const
{
    String out = FieldVisitorToString()(x);
    if (!skip_unambiguous_cast)
        out += "::String";
    return out;
}

String FieldVisitorToCastedLiteral::operator() (const bool & x) const
{
    String out = FieldVisitorToString()(x);
    if (!skip_unambiguous_cast)
        out += "::Bool";
    return out;
}

String FieldVisitorToCastedLiteral::operator() (const UInt128 & x) const
{
    return FieldVisitorToString()(x) + "::UInt128";
}

String FieldVisitorToCastedLiteral::operator() (const UInt256 & x) const
{
    return FieldVisitorToString()(x) + "::UInt256";
}

String FieldVisitorToCastedLiteral::operator() (const Int128 & x) const
{
    return FieldVisitorToString()(x) + "::Int128";
}

String FieldVisitorToCastedLiteral::operator() (const Int256 & x) const
{
    return FieldVisitorToString()(x) + "::Int256";
}

String FieldVisitorToCastedLiteral::operator() (const UUID & x) const
{
    return FieldVisitorToString()(x) + "::UUID";
}

String FieldVisitorToCastedLiteral::operator() (const IPv4 & x) const
{
    return FieldVisitorToString()(x) + "::IPv4";
}

String FieldVisitorToCastedLiteral::operator() (const IPv6 & x) const
{
    return FieldVisitorToString()(x) + "::IPv6";
}

String FieldVisitorToCastedLiteral::operator() (const DecimalField<Decimal32> & x) const
{
    return FieldVisitorToString()(x) + "::Decimal32(" + std::to_string(x.getScale()) + ")";
}

String FieldVisitorToCastedLiteral::operator() (const DecimalField<Decimal64> & x) const
{
    return FieldVisitorToString()(x) + "::Decimal64(" + std::to_string(x.getScale()) + ")";
}

String FieldVisitorToCastedLiteral::operator() (const DecimalField<Decimal128> & x) const
{
    return FieldVisitorToString()(x) + "::Decimal128(" + std::to_string(x.getScale()) + ")";
}

String FieldVisitorToCastedLiteral::operator() (const DecimalField<Decimal256> & x) const
{
    return FieldVisitorToString()(x) + "::Decimal256(" + std::to_string(x.getScale()) + ")";
}

String FieldVisitorToCastedLiteral::operator() (const Decimal32 & x, UInt32 scale) const
{
    return FieldVisitorToString()(x, scale) + "::Decimal32(" + std::to_string(scale) + ")";
}

String FieldVisitorToCastedLiteral::operator() (const Decimal64 & x, UInt32 scale) const
{
    return FieldVisitorToString()(x, scale) + "::Decimal64(" + std::to_string(scale) + ")";
}

String FieldVisitorToCastedLiteral::operator() (const Decimal128 & x, UInt32 scale) const
{
    return FieldVisitorToString()(x, scale) + "::Decimal128(" + std::to_string(scale) + ")";
}

String FieldVisitorToCastedLiteral::operator() (const Decimal256 & x, UInt32 scale) const
{
    return FieldVisitorToString()(x, scale) + "::Decimal256(" + std::to_string(scale) + ")";
}

String FieldVisitorToCastedLiteral::operator() (const Array & x) const
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

String FieldVisitorToCastedLiteral::operator() (const Tuple & x) const
{
    WriteBufferFromOwnString wb;

    /// Single-element tuples need the explicit tuple() form, otherwise they'd
    /// parse as plain parenthesised literals.
    if (x.size() > 1)
        wb << '(';
    else
        wb << "tuple(";

    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';
    return wb.str();
}

String FieldVisitorToCastedLiteral::operator() (const Map & x) const
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

String FieldVisitorToCastedLiteral::operator() (const Object & x) const
{
    return FieldVisitorToString()(x);
}

String FieldVisitorToCastedLiteral::operator() (const AggregateFunctionStateData & x) const
{
    return FieldVisitorToString()(x);
}

String FieldVisitorToCastedLiteral::operator() (const CustomType & x) const
{
    return FieldVisitorToString()(x);
}

}
