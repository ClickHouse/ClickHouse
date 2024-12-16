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
    using BigNum = __uint128_t;    /// Must contain the result of multiplying two UInt64.

    struct Rational
    {
        BigNum numerator = 0;
        BigNum denominator = 1;
    };

    Rational ratio;

    explicit ASTSampleRatio(const Rational & ratio_) : ratio(ratio_) {}

    String getID(char delim) const override { return "SampleRatio" + (delim + toString(ratio)); }

    ASTPtr clone() const override { return std::make_shared<ASTSampleRatio>(*this); }

    static String toString(BigNum num);
    static String toString(Rational ratio);

    void formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const override;
};

inline bool operator==(const ASTSampleRatio::Rational & lhs, const ASTSampleRatio::Rational & rhs)
{
    return lhs.numerator == rhs.numerator && lhs.denominator == rhs.denominator;
}

inline bool operator!=(const ASTSampleRatio::Rational & lhs, const ASTSampleRatio::Rational & rhs)
{
    return !(lhs == rhs);
}

}
