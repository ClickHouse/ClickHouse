#pragma once

#include <cstdint>

#include <Client/BuzzHouse/Utils/Nlimits.h>
#include <base/types.h>

namespace BuzzHouse
{

/// Forward declaration to allow conversion between hugeint and uhugeint
struct UHugeInt;

struct HugeInt
{
public:
    uint64_t lower;
    int64_t upper;

    HugeInt() = default;
    explicit HugeInt(int64_t value);
    constexpr HugeInt(int64_t up, uint64_t lo)
        : lower(lo)
        , upper(up)
    {
    }
    constexpr HugeInt(const HugeInt & rhs) = default;
    constexpr HugeInt(HugeInt && rhs) = default;
    HugeInt & operator=(const HugeInt & rhs) = default;
    HugeInt & operator=(HugeInt && rhs) = default;

    String toString() const;

    /// Comparison operators
    bool operator==(const HugeInt & rhs) const;
    bool operator!=(const HugeInt & rhs) const;
    bool operator<=(const HugeInt & rhs) const;
    bool operator<(const HugeInt & rhs) const;
    bool operator>(const HugeInt & rhs) const;
    bool operator>=(const HugeInt & rhs) const;

    /// Arithmetic operators
    HugeInt operator+(const HugeInt & rhs) const;
    HugeInt operator-(const HugeInt & rhs) const;
    HugeInt operator*(const HugeInt & rhs) const;
    HugeInt operator/(const HugeInt & rhs) const;
    HugeInt operator%(const HugeInt & rhs) const;
    HugeInt operator-() const;

    /// Bitwise operators
    HugeInt operator>>(const HugeInt & rhs) const;
    HugeInt operator<<(const HugeInt & rhs) const;
    HugeInt operator&(const HugeInt & rhs) const;
    HugeInt operator|(const HugeInt & rhs) const;
    HugeInt operator^(const HugeInt & rhs) const;
    HugeInt operator~() const;

    /// In-place operators
    HugeInt & operator+=(const HugeInt & rhs);
    HugeInt & operator-=(const HugeInt & rhs);
    HugeInt & operator*=(const HugeInt & rhs);
    HugeInt & operator/=(const HugeInt & rhs);
    HugeInt & operator%=(const HugeInt & rhs);
    HugeInt & operator>>=(const HugeInt & rhs);
    HugeInt & operator<<=(const HugeInt & rhs);
    HugeInt & operator&=(const HugeInt & rhs);
    HugeInt & operator|=(const HugeInt & rhs);
    HugeInt & operator^=(const HugeInt & rhs);

    /// Boolean operators
    explicit operator bool() const;
    bool operator!() const;
};

}
