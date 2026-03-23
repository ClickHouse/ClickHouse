#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>

namespace DB
{


String ASTSampleRatio::toString(BigNum num)
{
    if (num == 0)
        return "0";

    static const size_t MAX_WIDTH = 40;

    char tmp[MAX_WIDTH];

    char * pos;
    for (pos = tmp + MAX_WIDTH - 1; num != 0; --pos)
    {
        *pos = '0' + num % 10;
        num /= 10;
    }

    ++pos;

    return String(pos, tmp + MAX_WIDTH - pos);
}


String ASTSampleRatio::toString(Rational ratio)
{
    if (ratio.denominator == 1)
        return toString(ratio.numerator);
    return toString(ratio.numerator) + " / " + toString(ratio.denominator);
}

void ASTSampleRatio::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings &, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << toString(ratio);
}

void ASTSampleRatio::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "SampleRatio");
    w.writeString("numerator", toString(ratio.numerator));
    w.writeString("denominator", toString(ratio.denominator));
}

static ASTSampleRatio::BigNum parseBigNum(const String & s)
{
    ASTSampleRatio::BigNum result = 0;
    for (char c : s)
    {
        result = result * 10 + (c - '0');
    }
    return result;
}

void ASTSampleRatio::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    ratio.numerator = parseBigNum(r.getString("numerator"));
    ratio.denominator = parseBigNum(r.getString("denominator", "1"));
}

}
