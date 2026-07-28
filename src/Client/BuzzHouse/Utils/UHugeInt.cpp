#include <Client/BuzzHouse/Utils/HugeInt.h>
#include <Client/BuzzHouse/Utils/UHugeInt.h>
#include <base/defines.h>

#include <cmath>

namespace BuzzHouse
{

UHugeInt::UHugeInt(uint64_t value)
{
    this->lower = value;
    this->upper = 0;
}

bool UHugeInt::operator==(const UHugeInt & rhs) const
{
    int lower_equals = this->lower == rhs.lower;
    int upper_equals = this->upper == rhs.upper;
    return lower_equals & upper_equals;
}

bool UHugeInt::operator!=(const UHugeInt & rhs) const
{
    int lower_not_equals = this->lower != rhs.lower;
    int upper_not_equals = this->upper != rhs.upper;
    return lower_not_equals | upper_not_equals;
}

bool UHugeInt::operator<(const UHugeInt & rhs) const
{
    int upper_smaller = this->upper < rhs.upper;
    int upper_equal = this->upper == rhs.upper;
    int lower_smaller = this->lower < rhs.lower;
    return upper_smaller | (upper_equal & lower_smaller);
}

bool UHugeInt::operator<=(const UHugeInt & rhs) const
{
    int upper_smaller = this->upper < rhs.upper;
    int upper_equal = this->upper == rhs.upper;
    int lower_smaller_equals = this->lower <= rhs.lower;
    return upper_smaller | (upper_equal & lower_smaller_equals);
}

bool UHugeInt::operator>(const UHugeInt & rhs) const
{
    int upper_bigger = this->upper > rhs.upper;
    int upper_equal = this->upper == rhs.upper;
    int lower_bigger = this->lower > rhs.lower;
    return upper_bigger | (upper_equal & lower_bigger);
}

bool UHugeInt::operator>=(const UHugeInt & rhs) const
{
    int upper_bigger = this->upper > rhs.upper;
    int upper_equal = this->upper == rhs.upper;
    int lower_bigger_equals = this->lower >= rhs.lower;
    return upper_bigger | (upper_equal & lower_bigger_equals);
}

UHugeInt UHugeInt::operator+(const UHugeInt & rhs) const
{
    return UHugeInt(upper + rhs.upper + ((lower + rhs.lower) < lower), lower + rhs.lower);
}

UHugeInt UHugeInt::operator-(const UHugeInt & rhs) const
{
    return UHugeInt(upper - rhs.upper - ((lower - rhs.lower) > lower), lower - rhs.lower);
}

UHugeInt UHugeInt::operator*(const UHugeInt & rhs) const
{
    UHugeInt result = *this;
    result *= rhs;
    return result;
}

UHugeInt UHugeInt::operator>>(const UHugeInt & rhs) const
{
    const uint64_t shift = rhs.lower;
    if (rhs.upper != 0 || shift >= 128)
    {
        return UHugeInt(0);
    }
    else if (shift == 0)
    {
        return *this;
    }
    else if (shift == 64)
    {
        return UHugeInt(0, upper);
    }
    else if (shift < 64)
    {
        return UHugeInt(upper >> shift, (upper << (64 - shift)) + (lower >> shift));
    }
    else if ((128 > shift) && (shift > 64))
    {
        return UHugeInt(0, (upper >> (shift - 64)));
    }
    return UHugeInt(0);
}

UHugeInt UHugeInt::operator<<(const UHugeInt & rhs) const
{
    const uint64_t shift = rhs.lower;
    if (rhs.upper != 0 || shift >= 128)
    {
        return UHugeInt(0);
    }
    else if (shift == 0)
    {
        return *this;
    }
    else if (shift == 64)
    {
        return UHugeInt(lower, 0);
    }
    else if (shift < 64)
    {
        return UHugeInt((upper << shift) + (lower >> (64 - shift)), lower << shift);
    }
    else if ((128 > shift) && (shift > 64))
    {
        return UHugeInt(lower << (shift - 64), 0);
    }
    return UHugeInt(0);
}

UHugeInt UHugeInt::operator&(const UHugeInt & rhs) const
{
    UHugeInt result;
    result.lower = lower & rhs.lower;
    result.upper = upper & rhs.upper;
    return result;
}

UHugeInt UHugeInt::operator|(const UHugeInt & rhs) const
{
    UHugeInt result;
    result.lower = lower | rhs.lower;
    result.upper = upper | rhs.upper;
    return result;
}

UHugeInt UHugeInt::operator^(const UHugeInt & rhs) const
{
    UHugeInt result;
    result.lower = lower ^ rhs.lower;
    result.upper = upper ^ rhs.upper;
    return result;
}

UHugeInt UHugeInt::operator~() const
{
    UHugeInt result;
    result.lower = ~lower;
    result.upper = ~upper;
    return result;
}

UHugeInt & UHugeInt::operator+=(const UHugeInt & rhs)
{
    *this = *this + rhs;
    return *this;
}

UHugeInt & UHugeInt::operator-=(const UHugeInt & rhs)
{
    *this = *this - rhs;
    return *this;
}

static UHugeInt multiply(UHugeInt lhs, UHugeInt rhs)
{
    UHugeInt result;
#if ((__GNUC__ >= 5) || defined(__clang__)) && defined(__SIZEOF_INT128__)
    __uint128_t left = __uint128_t(lhs.lower) + (__uint128_t(lhs.upper) << 64);
    __uint128_t right = __uint128_t(rhs.lower) + (__uint128_t(rhs.upper) << 64);
    __uint128_t result_u128;

    result_u128 = left * right;
    result.upper = uint64_t(result_u128 >> 64);
    result.lower = uint64_t(result_u128 & 0xffffffffffffffff);
#else
    /// Split values into 4 32-bit parts
    uint64_t top[4] = {lhs.upper >> 32, lhs.upper & 0xffffffff, lhs.lower >> 32, lhs.lower & 0xffffffff};
    uint64_t bottom[4] = {rhs.upper >> 32, rhs.upper & 0xffffffff, rhs.lower >> 32, rhs.lower & 0xffffffff};
    uint64_t products[4][4];

    /// Multiply each component of the values
    for (int y = 3; y > -1; y--)
    {
        for (int x = 3; x > -1; x--)
        {
            products[3 - x][y] = top[x] * bottom[y];
        }
    }

    /// First row
    uint64_t fourth32 = (products[0][3] & 0xffffffff);
    uint64_t third32 = (products[0][2] & 0xffffffff) + (products[0][3] >> 32);
    uint64_t second32 = (products[0][1] & 0xffffffff) + (products[0][2] >> 32);
    uint64_t first32 = (products[0][0] & 0xffffffff) + (products[0][1] >> 32);

    /// Second row
    third32 += (products[1][3] & 0xffffffff);
    second32 += (products[1][2] & 0xffffffff) + (products[1][3] >> 32);
    first32 += (products[1][1] & 0xffffffff) + (products[1][2] >> 32);

    /// Third row
    second32 += (products[2][3] & 0xffffffff);
    first32 += (products[2][2] & 0xffffffff) + (products[2][3] >> 32);

    /// Fourth row
    first32 += (products[3][3] & 0xffffffff);

    /// Move carry to next digit
    third32 += fourth32 >> 32;
    second32 += third32 >> 32;
    first32 += second32 >> 32;

    //. Remove carry from current digit
    fourth32 &= 0xffffffff;
    third32 &= 0xffffffff;
    second32 &= 0xffffffff;
    first32 &= 0xffffffff;

    /// Combine components
    result.lower = (third32 << 32) | fourth32;
    result.upper = (first32 << 32) | second32;
#endif
    return result;
}

UHugeInt & UHugeInt::operator*=(const UHugeInt & rhs)
{
    *this = multiply(*this, rhs);
    return *this;
}

static uint8_t Bits(UHugeInt x)
{
    uint8_t out = 0;
    if (x.upper)
    {
        out = 64;
        for (uint64_t upper = x.upper; upper; upper >>= 1)
        {
            ++out;
        }
    }
    else
    {
        for (uint64_t lower = x.lower; lower; lower >>= 1)
        {
            ++out;
        }
    }
    return out;
}

static UHugeInt divMod(UHugeInt lhs, UHugeInt rhs, UHugeInt & remainder)
{
    if (rhs == UHugeInt(0))
    {
        remainder = lhs;
        return UHugeInt(0);
    }

    remainder = UHugeInt(0);
    if (rhs == UHugeInt(1))
    {
        return lhs;
    }
    else if (lhs == rhs)
    {
        return UHugeInt(1);
    }
    else if (lhs == UHugeInt(0) || lhs < rhs)
    {
        remainder = lhs;
        return UHugeInt(0);
    }

    UHugeInt result{0};
    for (uint8_t idx = Bits(lhs); idx > 0; --idx)
    {
        result <<= UHugeInt(1);
        remainder <<= UHugeInt(1);

        if (((lhs >> UHugeInt(idx - 1U)) & UHugeInt(1)) != UHugeInt(0))
        {
            remainder += UHugeInt(1);
        }

        if (remainder >= rhs)
        {
            remainder -= rhs;
            result += UHugeInt(1);
        }
    }
    return result;
}

static UHugeInt divide(UHugeInt lhs, UHugeInt rhs)
{
    UHugeInt remainder;
    return divMod(lhs, rhs, remainder);
}

static UHugeInt modulo(UHugeInt lhs, UHugeInt rhs)
{
    UHugeInt remainder;
    /// Here it is interested in the remainder only
    const auto u = divMod(lhs, rhs, remainder);
    UNUSED(u);
    return remainder;
}

UHugeInt & UHugeInt::operator/=(const UHugeInt & rhs)
{
    *this = divide(*this, rhs);
    return *this;
}

UHugeInt & UHugeInt::operator%=(const UHugeInt & rhs)
{
    *this = modulo(*this, rhs);
    return *this;
}

UHugeInt UHugeInt::operator/(const UHugeInt & rhs) const
{
    return divide(*this, rhs);
}

UHugeInt UHugeInt::operator%(const UHugeInt & rhs) const
{
    return modulo(*this, rhs);
}

static UHugeInt negateInPlace(const UHugeInt & input)
{
    UHugeInt result{0};
    result -= input;
    return result;
}

UHugeInt UHugeInt::operator-() const
{
    return negateInPlace(*this);
}

UHugeInt & UHugeInt::operator>>=(const UHugeInt & rhs)
{
    *this = *this >> rhs;
    return *this;
}

UHugeInt & UHugeInt::operator<<=(const UHugeInt & rhs)
{
    *this = *this << rhs;
    return *this;
}

UHugeInt & UHugeInt::operator&=(const UHugeInt & rhs)
{
    lower &= rhs.lower;
    upper &= rhs.upper;
    return *this;
}

UHugeInt & UHugeInt::operator|=(const UHugeInt & rhs)
{
    lower |= rhs.lower;
    upper |= rhs.upper;
    return *this;
}

UHugeInt & UHugeInt::operator^=(const UHugeInt & rhs)
{
    lower ^= rhs.lower;
    upper ^= rhs.upper;
    return *this;
}

bool UHugeInt::operator!() const
{
    return *this == UHugeInt(0);
}

UHugeInt::operator bool() const
{
    return *this != UHugeInt(0);
}

String UHugeInt::toString() const
{
    String res;
    UHugeInt input = *this;
    UHugeInt remainder;

    while (true)
    {
        if (!input.lower && !input.upper)
        {
            break;
        }
        input = divMod(input, UHugeInt(10), remainder);
        res.insert(0, String(1, static_cast<char>('0' + remainder.lower)));
    }
    /// If empty then value is zero
    return res.empty() ? "0" : res;
}

}
