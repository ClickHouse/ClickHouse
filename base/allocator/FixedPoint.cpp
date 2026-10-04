#include <allocator/FixedPoint.h>

#include <allocator/Format.h>

namespace jemalloc::fxp
{

namespace
{

/// jemalloc: fxp_isdigit
bool isDigit(char c)
{
    return '0' <= c && c <= '9';
}

}

/// jemalloc: fxp_parse
bool parse(FixedPoint * result, const char * str, const char ** end)
{
    uint32_t integer_part = 0;
    const char * cur = str;

    /// The string must start with a digit or a decimal point.
    if (*cur != '.' && !isDigit(*cur))
        return true;

    while ('0' <= *cur && *cur <= '9')
    {
        integer_part *= 10;
        integer_part += uint32_t(*cur - '0');
        if (integer_part >= (1U << 16))
            return true;
        ++cur;
    }

    /// Either we're done, or there's a fractional part.
    if (*cur != '.')
    {
        *result = integer_part << 16;
        if (end != nullptr)
            *end = cur;
        return false;
    }

    /// There's a fractional part.
    ++cur;
    if (!isDigit(*cur))
        return true; /// Shouldn't end on the decimal point.

    /// 14 digits of precision: enough to get exact values for small powers of two denominators.
    uint64_t fractional_part = 0;
    uint64_t frac_div = 1;
    for (int i = 0; i < FRACTIONAL_PART_DIGITS; ++i)
    {
        fractional_part *= 10;
        frac_div *= 10;
        if (isDigit(*cur))
        {
            fractional_part += uint64_t(*cur - '0');
            ++cur;
        }
    }
    /// Ignore any digits after the first FRACTIONAL_PART_DIGITS.
    while (isDigit(*cur))
        ++cur;

    JE_ASSERT(fractional_part < frac_div);
    uint32_t fractional_repr = static_cast<uint32_t>((fractional_part << 16) / frac_div);

    *result = (integer_part << 16) + fractional_repr;
    if (end != nullptr)
        *end = cur;
    return false;
}

/// jemalloc: fxp_print
void print(FixedPoint a, char (&buf)[BUF_SIZE])
{
    uint32_t integer_part = roundDown(a);
    uint32_t fractional_part = a & ((1U << 16) - 1);

    int leading_fraction_zeros = 0;
    uint64_t fraction_digits = fractional_part;
    for (int i = 0; i < FRACTIONAL_PART_DIGITS; ++i)
    {
        if (fraction_digits < (1U << 16) && fraction_digits * 10 >= (1U << 16))
            leading_fraction_zeros = i;
        fraction_digits *= 10;
    }
    fraction_digits >>= 16;
    while (fraction_digits > 0 && fraction_digits % 10 == 0)
        fraction_digits /= 10;

    size_t printed = format(buf, BUF_SIZE, "%" FMTu32 ".", integer_part);
    for (int i = 0; i < leading_fraction_zeros; ++i)
    {
        buf[printed] = '0';
        ++printed;
    }
    format(&buf[printed], BUF_SIZE - printed, "%" FMTu64, fraction_digits);
}

}
