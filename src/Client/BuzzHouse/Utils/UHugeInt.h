#pragma once

#include <cstdint>

#include <Client/BuzzHouse/Utils/Nlimits.h>
#include <base/types.h>

namespace BuzzHouse
{

/// Forward declaration to allow conversion between hugeint and uhugeint
struct HugeInt;

struct UHugeInt
{
public:
    uint64_t lower;
    uint64_t upper;

    UHugeInt() = default;
    explicit UHugeInt(uint64_t value);
    constexpr UHugeInt(uint64_t up, uint64_t lo)
        : lower(lo)
        , upper(up)
    {
    }
    constexpr UHugeInt(const UHugeInt & rhs) = default;
    constexpr UHugeInt(UHugeInt && rhs) = default;
    UHugeInt & operator=(const UHugeInt & rhs) = default;
    UHugeInt & operator=(UHugeInt && rhs) = default;

    String toString() const;

    /// Comparison operators
    bool operator==(const UHugeInt & rhs) const;
    bool operator!=(const UHugeInt & rhs) const;
    bool operator<=(const UHugeInt & rhs) const;
    bool operator<(const UHugeInt & rhs) const;
    bool operator>(const UHugeInt & rhs) const;
    bool operator>=(const UHugeInt & rhs) const;

    /// Arithmetic operators
    UHugeInt operator+(const UHugeInt & rhs) const;
    UHugeInt operator-(const UHugeInt & rhs) const;
    UHugeInt operator*(const UHugeInt & rhs) const;
    UHugeInt operator/(const UHugeInt & rhs) const;
    UHugeInt operator%(const UHugeInt & rhs) const;
    UHugeInt operator-() const;

    /// Bitwise operators
    UHugeInt operator>>(const UHugeInt & rhs) const;
    UHugeInt operator<<(const UHugeInt & rhs) const;
    UHugeInt operator&(const UHugeInt & rhs) const;
    UHugeInt operator|(const UHugeInt & rhs) const;
    UHugeInt operator^(const UHugeInt & rhs) const;
    UHugeInt operator~() const;

    /// In-place operators
    UHugeInt & operator+=(const UHugeInt & rhs);
    UHugeInt & operator-=(const UHugeInt & rhs);
    UHugeInt & operator*=(const UHugeInt & rhs);
    UHugeInt & operator/=(const UHugeInt & rhs);
    UHugeInt & operator%=(const UHugeInt & rhs);
    UHugeInt & operator>>=(const UHugeInt & rhs);
    UHugeInt & operator<<=(const UHugeInt & rhs);
    UHugeInt & operator&=(const UHugeInt & rhs);
    UHugeInt & operator|=(const UHugeInt & rhs);
    UHugeInt & operator^=(const UHugeInt & rhs);

    /// Boolean operators
    explicit operator bool() const;
    bool operator!() const;
};

template <>
struct NumericLimits<UHugeInt>
{
    static constexpr UHugeInt minimum() { return {0, 0}; }
    static constexpr UHugeInt maximum() { return {std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()}; }
};

}
