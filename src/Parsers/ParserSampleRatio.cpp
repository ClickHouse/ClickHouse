#include <IO/ReadHelpers.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ParserSampleRatio.h>
#include <base/extended_types.h>


namespace DB
{


static constexpr int MAX_128BIT_EXPONENT = 38; /// 10^38 < 2^128 < 10^39.
static constexpr auto MAX_BIG_NUM = std::numeric_limits<unsigned __int128>::max();

/// Returns 10^x as __uint128_t. Saturates to max for x > 38.
static ASTSampleRatio::BigNum bigIntExp10(Int64 x)
{
    if (x < 0)
        return 0;
    if (x > MAX_128BIT_EXPONENT)
        return MAX_BIG_NUM;

    ASTSampleRatio::BigNum result = 1;
    for (Int64 i = 0; i < x; ++i)
        result *= 10;
    return result;
}

/// Saturating multiplication for __uint128_t.
static ASTSampleRatio::BigNum saturatingMultiply(ASTSampleRatio::BigNum a, ASTSampleRatio::BigNum b)
{
    if (a == 0 || b == 0)
        return 0;
    if (a > MAX_BIG_NUM / b)
        return MAX_BIG_NUM;
    return a * b;
}

/// Saturating addition for __uint128_t.
static ASTSampleRatio::BigNum saturatingAdd(ASTSampleRatio::BigNum a, ASTSampleRatio::BigNum b)
{
    if (a > MAX_BIG_NUM - b)
        return MAX_BIG_NUM;
    return a + b;
}

/// Read unsigned integer into __uint128_t via UInt128 (wide::integer) to work around
/// clang-tidy crash on tryReadIntText<__uint128_t> (llvm/llvm-project#186256).
static const char * readUInt128Text(ASTSampleRatio::BigNum & x, const char * pos, const char * end)
{
    /// Skip leading zeros to count only significant digits.
    const char * significant = pos;
    while (significant < end && *significant == '0')
        ++significant;

    UInt128 tmp = 0;
    const char * result = tryReadIntText(tmp, pos, end);
    auto num_significant = result - significant;
    /// tryReadIntText skips overflow checks for big-int types (is_big_int_v),
    /// so values wrap modulo 2^128. Numbers with 40+ significant digits always overflow.
    /// For exactly 39, compare the significant digits lexicographically against 2^128 - 1.
    static constexpr std::string_view MAX_UINT128_STR = "340282366920938463463374607431768211455";
    if (num_significant >= 40 || (num_significant == 39 && std::string_view(significant, 39) > MAX_UINT128_STR))
        x = MAX_BIG_NUM;
    else
        x = static_cast<ASTSampleRatio::BigNum>(tmp);
    return result;
}

static bool parseDecimal(const char * pos, const char * end, ASTSampleRatio::Rational & res)
{
    ASTSampleRatio::BigNum num_before = 0;
    ASTSampleRatio::BigNum num_after = 0;
    Int32 exponent = 0;

    const char * pos_after_first_num = readUInt128Text(num_before, pos, end);

    bool has_num_before_point = pos_after_first_num > pos;
    pos = pos_after_first_num;
    bool has_point = pos < end && *pos == '.';

    if (has_point)
        ++pos;

    if (!has_num_before_point && !has_point)
        return false;

    int number_of_digits_after_point = 0;

    if (has_point)
    {
        const char * pos_after_second_num = readUInt128Text(num_after, pos, end);
        number_of_digits_after_point = static_cast<int>(pos_after_second_num - pos);
        pos = pos_after_second_num;
    }

    bool has_exponent = pos < end && (*pos == 'e' || *pos == 'E');

    if (has_exponent)
    {
        ++pos;
        const char * pos_after_exponent = tryReadIntText(exponent, pos, end);

        if (pos_after_exponent == pos)
            return false;
    }

    ASTSampleRatio::BigNum decimal_scale = bigIntExp10(number_of_digits_after_point);
    auto product = saturatingMultiply(num_before, decimal_scale);
    res.numerator = saturatingAdd(product, num_after);
    res.denominator = decimal_scale;

    if (exponent > 0)
        res.numerator = saturatingMultiply(res.numerator, bigIntExp10(exponent));
    if (exponent < 0)
        res.denominator = saturatingMultiply(res.denominator, bigIntExp10(-static_cast<Int64>(exponent)));

    /// NOTE You do not need to remove the common power of ten from the numerator and denominator.
    return true;
}


/** Possible options:
  *
  * 12345
  * - an integer
  *
  * 0.12345
  * .12345
  * 0.
  * - fraction in ordinary decimal notation
  *
  * 1.23e-1
  * - fraction in scientific decimal notation
  *
  * 123 / 456
  * - fraction with an ordinary denominator
  *
  * Just in case, in the numerator and denominator of the fraction, we support the previous cases.
  * Example:
  * 123.0 / 456e0
  */
bool ParserSampleRatio::parseImpl(Pos & pos, ASTPtr & node, Expected &)
{
    ASTSampleRatio::Rational numerator;
    ASTSampleRatio::Rational denominator;
    ASTSampleRatio::Rational res;

    if (!parseDecimal(pos->begin, pos->end, numerator))
        return false;
    ++pos;

    bool has_slash = pos->type == TokenType::Slash;

    if (has_slash)
    {
        ++pos;

        if (!parseDecimal(pos->begin, pos->end, denominator))
            return false;
        ++pos;

        res.numerator = saturatingMultiply(numerator.numerator, denominator.denominator);
        res.denominator = saturatingMultiply(numerator.denominator, denominator.numerator);
    }
    else
    {
        res = numerator;
    }

    node = make_intrusive<ASTSampleRatio>(res);
    return true;
}

}
