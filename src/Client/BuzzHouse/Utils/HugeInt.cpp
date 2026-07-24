#include <Client/BuzzHouse/Utils/HugeInt.h>
#include <Client/BuzzHouse/Utils/UHugeInt.h>
#include <base/defines.h>

#include <cmath>

namespace BuzzHouse
{

static void negateInPlace(HugeInt & input)
{
    input.lower = std::numeric_limits<uint64_t>::max() - input.lower + 1ull;
    input.upper = -1 - input.upper + (input.lower == 0);
}

static uint8_t positiveHugeintHighestBit(HugeInt bits)
{
    uint8_t out = 0;
    if (bits.upper)
    {
        out = 64;
        uint64_t up = static_cast<uint64_t>(bits.upper);
        while (up)
        {
            up >>= 1;
            out++;
        }
    }
    else
    {
        uint64_t low = bits.lower;
        while (low)
        {
            low >>= 1;
            out++;
        }
    }
    return out;
}

static bool positiveHugeintIsBitSet(HugeInt lhs, uint8_t bit_position)
{
    if (bit_position < 64)
    {
        return lhs.lower & (uint64_t(1) << uint64_t(bit_position));
    }
    else
    {
        return static_cast<uint64_t>(lhs.upper) & (uint64_t(1) << uint64_t(bit_position - 64));
    }
}

static HugeInt positiveHugeintLeftShift(HugeInt lhs, uint32_t amount)
{
    chassert(amount > 0 && amount < 64);
    HugeInt result;
    result.lower = lhs.lower << amount;
    result.upper = static_cast<int64_t>((static_cast<uint64_t>(lhs.upper) << amount) + (lhs.lower >> (64 - amount)));
    return result;
}

static HugeInt divModPositive(HugeInt lhs, uint64_t rhs, uint64_t & remainder)
{
    chassert(lhs.upper >= 0);
    /// DivMod code adapted from:
    /// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

    /// Initialize the result and remainder to 0
    HugeInt div_result;
    div_result.lower = 0;
    div_result.upper = 0;
    remainder = 0;

    uint8_t highest_bit_set = positiveHugeintHighestBit(lhs);
    /// Now iterate over the amount of bits that are set in the LHS
    for (uint8_t x = highest_bit_set; x > 0; x--)
    {
        /// Left-shift the current result and remainder by 1
        div_result = positiveHugeintLeftShift(div_result, 1);
        remainder <<= 1;
        /// We get the value of the bit at position X, where position 0 is the least-significant bit
        if (positiveHugeintIsBitSet(lhs, x - 1))
        {
            /// Increment the remainder
            remainder++;
        }
        if (remainder >= rhs)
        {
            /// The remainder has passed the division multiplier: add one to the divide result
            remainder -= rhs;
            div_result.lower++;
            if (div_result.lower == 0)
            {
                /// Overflow
                div_result.upper++;
            }
        }
    }
    return div_result;
}

int sign(HugeInt n)
{
    return ((n > HugeInt(0)) - (n < HugeInt(0)));
}

HugeInt abs(HugeInt n)
{
    chassert(n != NumericLimits<HugeInt>::minimum());
    return (n * static_cast<HugeInt>(sign(n)));
}

static HugeInt divMod(HugeInt lhs, HugeInt rhs, HugeInt & remainder);

static HugeInt divModMinimum(HugeInt lhs, HugeInt rhs, HugeInt & remainder)
{
    chassert(lhs == NumericLimits<HugeInt>::minimum() || rhs == NumericLimits<HugeInt>::minimum());
    if (rhs == NumericLimits<HugeInt>::minimum())
    {
        if (lhs == NumericLimits<HugeInt>::minimum())
        {
            remainder = HugeInt(0);
            return HugeInt(1);
        }
        remainder = lhs;
        return HugeInt(0);
    }

    /// Add 1 to minimum and run through divMod again
    HugeInt result = divMod(NumericLimits<HugeInt>::minimum() + HugeInt(1), rhs, remainder);

    /// If the 1 mattered we need to adjust the result, otherwise the remainder
    if (abs(remainder) + HugeInt(1) == abs(rhs))
    {
        result -= static_cast<HugeInt>(sign(rhs));
        remainder = HugeInt(0);
    }
    else
    {
        remainder -= HugeInt(1);
    }
    return result;
}

static HugeInt divMod(HugeInt lhs, HugeInt rhs, HugeInt & remainder)
{
    if (rhs == HugeInt(0))
    {
        remainder = lhs;
        return HugeInt(0);
    }

    /// Check if one of the sides is HugeInt minimum, as that can't be negated.
    if (lhs == NumericLimits<HugeInt>::minimum() || rhs == NumericLimits<HugeInt>::minimum())
    {
        return divModMinimum(lhs, rhs, remainder);
    }

    bool lhs_negative = lhs.upper < 0;
    bool rhs_negative = rhs.upper < 0;
    if (lhs_negative)
    {
        negateInPlace(lhs);
    }
    if (rhs_negative)
    {
        negateInPlace(rhs);
    }
    /// DivMod code adapted from:
    /// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

    /// Initialize the result and remainder to 0
    HugeInt div_result;
    div_result.lower = 0;
    div_result.upper = 0;
    remainder.lower = 0;
    remainder.upper = 0;

    uint8_t highest_bit_set = positiveHugeintHighestBit(lhs);
    /// Now iterate over the amount of bits that are set in the LHS
    for (uint8_t x = highest_bit_set; x > 0; x--)
    {
        /// Left-shift the current result and remainder by 1
        div_result = positiveHugeintLeftShift(div_result, 1);
        remainder = positiveHugeintLeftShift(remainder, 1);

        /// We get the value of the bit at position X, where position 0 is the least-significant bit
        if (positiveHugeintIsBitSet(lhs, x - 1))
        {
            remainder += HugeInt(1);
        }
        if (remainder >= rhs)
        {
            /// The remainder has passed the division multiplier: add one to the divide result
            remainder -= rhs;
            div_result += HugeInt(1);
        }
    }
    if (lhs_negative ^ rhs_negative)
    {
        negateInPlace(div_result);
    }
    if (lhs_negative)
    {
        negateInPlace(remainder);
    }
    return div_result;
}

static HugeInt divide(HugeInt lhs, HugeInt rhs)
{
    HugeInt remainder;
    return divMod(lhs, rhs, remainder);
}

static HugeInt modulo(HugeInt lhs, HugeInt rhs)
{
    HugeInt remainder;
    /// Here it is interested in the remainder only
    const auto u = divMod(lhs, rhs, remainder);
    UNUSED(u);
    return remainder;
}

static HugeInt multiply(HugeInt lhs, HugeInt rhs)
{
    HugeInt result;
    bool lhs_negative = lhs.upper < 0;
    bool rhs_negative = rhs.upper < 0;
    if (lhs_negative)
    {
        negateInPlace(lhs);
    }
    if (rhs_negative)
    {
        negateInPlace(rhs);
    }

#if ((__GNUC__ >= 5) || defined(__clang__)) && defined(__SIZEOF_INT128__)
    __uint128_t left = __uint128_t(lhs.lower) + (__uint128_t(lhs.upper) << 64);
    __uint128_t right = __uint128_t(rhs.lower) + (__uint128_t(rhs.upper) << 64);
    __uint128_t result_i128;
    result_i128 = left * right;
    uint64_t upper = uint64_t(result_i128 >> 64);
    result.upper = int64_t(upper);
    result.lower = uint64_t(result_i128 & 0xffffffffffffffff);
#else
    /// Multiply code adapted from:
    /// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

    /// Split values into 4 32-bit parts
    uint64_t top[4] = {uint64_t(lhs.upper) >> 32, uint64_t(lhs.upper) & 0xffffffff, lhs.lower >> 32, lhs.lower & 0xffffffff};
    uint64_t bottom[4] = {uint64_t(rhs.upper) >> 32, uint64_t(rhs.upper) & 0xffffffff, rhs.lower >> 32, rhs.lower & 0xffffffff};
    uint64_t products[4][4];

    /// Multiply each component of the values
    for (auto x = 0; x < 4; x++)
    {
        for (auto y = 0; y < 4; y++)
        {
            products[x][y] = top[x] * bottom[y];
        }
    }

    /// First row
    uint64_t fourth32 = (products[3][3] & 0xffffffff);
    uint64_t third32 = (products[3][2] & 0xffffffff) + (products[3][3] >> 32);
    uint64_t second32 = (products[3][1] & 0xffffffff) + (products[3][2] >> 32);
    uint64_t first32 = (products[3][0] & 0xffffffff) + (products[3][1] >> 32);

    /// Second row
    third32 += (products[2][3] & 0xffffffff);
    second32 += (products[2][2] & 0xffffffff) + (products[2][3] >> 32);
    first32 += (products[2][1] & 0xffffffff) + (products[2][2] >> 32);

    /// Third row
    second32 += (products[1][3] & 0xffffffff);
    first32 += (products[1][2] & 0xffffffff) + (products[1][3] >> 32);

    /// Fourth row
    first32 += (products[0][3] & 0xffffffff);

    /// Move carry to next digit
    third32 += fourth32 >> 32;
    second32 += third32 >> 32;
    first32 += second32 >> 32;

    /// Remove carry from current digit
    fourth32 &= 0xffffffff;
    third32 &= 0xffffffff;
    second32 &= 0xffffffff;
    first32 &= 0xffffffff;

    /// Combine components
    result.lower = (third32 << 32) | fourth32;
    result.upper = (first32 << 32) | second32;
#endif
    if (lhs_negative ^ rhs_negative)
    {
        negateInPlace(result);
    }
    return result;
}

template <class DST>
HugeInt hugeintConvertInteger(DST input)
{
    HugeInt result;
    result.lower = static_cast<uint64_t>(input);
    result.upper = (input < 0) * -1;
    return result;
}

HugeInt::HugeInt(int64_t value)
{
    auto result = hugeintConvertInteger<int64_t>(value);
    this->lower = result.lower;
    this->upper = result.upper;
}

bool HugeInt::operator==(const HugeInt & rhs) const
{
    int lower_equals = this->lower == rhs.lower;
    int upper_equals = this->upper == rhs.upper;
    return lower_equals & upper_equals;
}

bool HugeInt::operator!=(const HugeInt & rhs) const
{
    int lower_not_equals = this->lower != rhs.lower;
    int upper_not_equals = this->upper != rhs.upper;
    return lower_not_equals | upper_not_equals;
}

bool HugeInt::operator<(const HugeInt & rhs) const
{
    int upper_smaller = this->upper < rhs.upper;
    int upper_equal = this->upper == rhs.upper;
    int lower_smaller = this->lower < rhs.lower;
    return upper_smaller | (upper_equal & lower_smaller);
}

bool HugeInt::operator<=(const HugeInt & rhs) const
{
    int upper_smaller = this->upper < rhs.upper;
    int upper_equal = this->upper == rhs.upper;
    int lower_smaller_equals = this->lower <= rhs.lower;
    return upper_smaller | (upper_equal & lower_smaller_equals);
}

bool HugeInt::operator>(const HugeInt & rhs) const
{
    int upper_bigger = this->upper > rhs.upper;
    int upper_equal = this->upper == rhs.upper;
    int lower_bigger = this->lower > rhs.lower;
    return upper_bigger | (upper_equal & lower_bigger);
}

bool HugeInt::operator>=(const HugeInt & rhs) const
{
    int upper_bigger = this->upper > rhs.upper;
    int upper_equal = this->upper == rhs.upper;
    int lower_bigger_equals = this->lower >= rhs.lower;
    return upper_bigger | (upper_equal & lower_bigger_equals);
}

HugeInt HugeInt::operator+(const HugeInt & rhs) const
{
    return HugeInt(upper + rhs.upper + ((lower + rhs.lower) < lower), lower + rhs.lower);
}

HugeInt HugeInt::operator-(const HugeInt & rhs) const
{
    return HugeInt(upper - rhs.upper - ((lower - rhs.lower) > lower), lower - rhs.lower);
}

HugeInt HugeInt::operator*(const HugeInt & rhs) const
{
    HugeInt result = *this;
    result *= rhs;
    return result;
}

HugeInt HugeInt::operator/(const HugeInt & rhs) const
{
    return divide(*this, rhs);
}

HugeInt HugeInt::operator%(const HugeInt & rhs) const
{
    return modulo(*this, rhs);
}

HugeInt HugeInt::operator-() const
{
    HugeInt input = *this;
    negateInPlace(input);
    return input;
}

HugeInt HugeInt::operator>>(const HugeInt & rhs) const
{
    HugeInt result;
    uint64_t shift = rhs.lower;
    if (rhs.upper != 0 || shift >= 128)
    {
        return HugeInt(0);
    }
    else if (shift == 0)
    {
        return *this;
    }
    else if (shift == 64)
    {
        result.upper = (upper < 0) ? -1 : 0;
        result.lower = uint64_t(upper);
    }
    else if (shift < 64)
    {
        /// Perform lower shift in unsigned integer, and mask away the most significant bit
        result.lower = (uint64_t(upper) << (64 - shift)) | (lower >> shift);
        result.upper = upper >> shift;
    }
    else
    {
        chassert(shift < 128);
        result.lower = uint64_t(upper >> (shift - 64));
        result.upper = (upper < 0) ? -1 : 0;
    }
    return result;
}

HugeInt HugeInt::operator<<(const HugeInt & rhs) const
{
    if (upper < 0)
    {
        return HugeInt(0);
    }
    HugeInt result;
    uint64_t shift = rhs.lower;
    if (rhs.upper != 0 || shift >= 128)
    {
        return HugeInt(0);
    }
    else if (shift == 64)
    {
        result.upper = int64_t(lower);
        result.lower = 0;
    }
    else if (shift == 0)
    {
        return *this;
    }
    else if (shift < 64)
    {
        /// Perform upper shift in unsigned integer, and mask away the most significant bit
        uint64_t upper_shift = ((uint64_t(upper) << shift) + (lower >> (64 - shift))) & 0x7FFFFFFFFFFFFFFF;
        result.lower = lower << shift;
        result.upper = int64_t(upper_shift);
    }
    else
    {
        chassert(shift < 128);
        result.lower = 0;
        result.upper = static_cast<int64_t>((lower << (shift - 64)) & 0x7FFFFFFFFFFFFFFF);
    }
    return result;
}

HugeInt HugeInt::operator&(const HugeInt & rhs) const
{
    HugeInt result;
    result.lower = lower & rhs.lower;
    result.upper = upper & rhs.upper;
    return result;
}

HugeInt HugeInt::operator|(const HugeInt & rhs) const
{
    HugeInt result;
    result.lower = lower | rhs.lower;
    result.upper = upper | rhs.upper;
    return result;
}

HugeInt HugeInt::operator^(const HugeInt & rhs) const
{
    HugeInt result;
    result.lower = lower ^ rhs.lower;
    result.upper = upper ^ rhs.upper;
    return result;
}

HugeInt HugeInt::operator~() const
{
    HugeInt result;
    result.lower = ~lower;
    result.upper = ~upper;
    return result;
}

HugeInt & HugeInt::operator+=(const HugeInt & rhs)
{
    *this = *this + rhs;
    return *this;
}
HugeInt & HugeInt::operator-=(const HugeInt & rhs)
{
    *this = *this - rhs;
    return *this;
}
HugeInt & HugeInt::operator*=(const HugeInt & rhs)
{
    *this = multiply(*this, rhs);
    return *this;
}
HugeInt & HugeInt::operator/=(const HugeInt & rhs)
{
    *this = divide(*this, rhs);
    return *this;
}
HugeInt & HugeInt::operator%=(const HugeInt & rhs)
{
    *this = modulo(*this, rhs);
    return *this;
}
HugeInt & HugeInt::operator>>=(const HugeInt & rhs)
{
    *this = *this >> rhs;
    return *this;
}
HugeInt & HugeInt::operator<<=(const HugeInt & rhs)
{
    *this = *this << rhs;
    return *this;
}
HugeInt & HugeInt::operator&=(const HugeInt & rhs)
{
    lower &= rhs.lower;
    upper &= rhs.upper;
    return *this;
}
HugeInt & HugeInt::operator|=(const HugeInt & rhs)
{
    lower |= rhs.lower;
    upper |= rhs.upper;
    return *this;
}
HugeInt & HugeInt::operator^=(const HugeInt & rhs)
{
    lower ^= rhs.lower;
    upper ^= rhs.upper;
    return *this;
}

bool HugeInt::operator!() const
{
    return *this == HugeInt(0);
}

HugeInt::operator bool() const
{
    return *this != HugeInt(0);
}

String HugeInt::toString() const
{
    String res;
    String prefix;
    uint64_t remainder;
    HugeInt input = *this;

    if (input == NumericLimits<HugeInt>::minimum())
    {
        return "-170141183460469231731687303715884105728";
    }
    if (input.upper < 0)
    {
        prefix = "-";
        negateInPlace(input);
    }
    while (true)
    {
        if (!input.lower && !input.upper)
        {
            break;
        }
        input = divModPositive(input, 10, remainder);
        res.insert(0, String(1, static_cast<char>('0' + remainder)));
    }
    /// If empty then value is zero
    return res.empty() ? "0" : (prefix + res);
}

}
