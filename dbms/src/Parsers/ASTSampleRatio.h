#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** Sampling factor in the form 0.1 or 1/10.
  * It's important to save it as a rational number without converting it to IEEE-754.
  */
class ASTSampleRatio : public IAST
{
public:
#ifdef __SIZEOF_INT128__
    using BigNum = __uint128_t;    /// Must contain the result of multiplying two UInt64.
#else
    // TODO: incomplete temporary fallback. change with boost::multiprecision
    using BigNum = uint64_t;
#endif

    struct Rational
    {
        BigNum numerator = 0;
        BigNum denominator = 1;
    };

    Rational ratio;

    ASTSampleRatio() = default;
    ASTSampleRatio(const StringRange range_) : IAST(range_) {}
    ASTSampleRatio(const StringRange range_, Rational & ratio_) : IAST(range_), ratio(ratio_) {}

    String getID() const override { return "SampleRatio_" + toString(ratio); }

    ASTPtr clone() const override { return std::make_shared<ASTSampleRatio>(*this); }

    static String toString(BigNum num);
    static String toString(Rational ratio);

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << toString(ratio);
    }
};

}
