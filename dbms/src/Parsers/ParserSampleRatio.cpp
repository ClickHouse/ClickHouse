#include <common/intExp.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ASTSampleRatio.h>
#include <IO/ReadHelpers.h>


namespace DB
{


static bool parseDecimal(const char * pos, const char * end, ASTSampleRatio::Rational & res)
{
    UInt64 num_before = 0;
    UInt64 num_after = 0;
    Int64 exponent = 0;

    const char * pos_after_first_num = tryReadIntText(num_before, pos, end);

    bool has_num_before_point = pos_after_first_num > pos;
    pos = pos_after_first_num;
    bool has_point = pos < end && *pos == '.';

    if (has_point)
        ++pos;

    if (!has_num_before_point && !has_point)
        return false;

    size_t number_of_digits_after_point = 0;

    if (has_point)
    {
        const char * pos_after_second_num = tryReadIntText(num_after, pos, end);
        number_of_digits_after_point = pos_after_second_num - pos;
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

    res.numerator = num_before * intExp10(number_of_digits_after_point) + num_after;
    res.denominator = intExp10(number_of_digits_after_point);

    if (exponent > 0)
        res.numerator *= intExp10(exponent);
    if (exponent < 0)
        res.denominator *= intExp10(-exponent);

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

        res.numerator = numerator.numerator * denominator.denominator;
        res.denominator = numerator.denominator * denominator.numerator;
    }
    else
    {
        res = numerator;
    }

    node = std::make_shared<ASTSampleRatio>(res);
    return true;
}

}
