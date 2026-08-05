#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


String ASTSampleRatio::toString(BigNum num)
{
    if (num == 0)
        return "0";

    static const size_t MAX_WIDTH = 40;

    char tmp[MAX_WIDTH];

    char * pos = nullptr;
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

static ASTSampleRatio::BigNum parseBigNum(const String & key, const String & s)
{
    if (s.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Empty '{}' for SampleRatio during AST JSON deserialization", key);

    ASTSampleRatio::BigNum result = 0;
    for (char c : s)
    {
        if (c < '0' || c > '9')
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Non-digit character in '{}' for SampleRatio during AST JSON deserialization", key);
        result = result * 10 + (c - '0');
    }
    return result;
}

void ASTSampleRatio::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    if (!r.has("numerator"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Missing 'numerator' for SampleRatio during AST JSON deserialization");

    ratio.numerator = parseBigNum("numerator", r.getString("numerator"));

    if (r.has("denominator"))
        ratio.denominator = parseBigNum("denominator", r.getString("denominator"));
    else
        ratio.denominator = 1;
}

}
