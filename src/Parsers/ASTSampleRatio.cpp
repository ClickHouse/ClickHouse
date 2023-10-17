#include <Parsers/ASTSampleRatio.h>
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
    else
        return toString(ratio.numerator) + " / " + toString(ratio.denominator);
}

void ASTSampleRatio::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << toString(ratio);
}

}
