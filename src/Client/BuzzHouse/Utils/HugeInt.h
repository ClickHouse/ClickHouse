#pragma once

#include <cstdint>
#include <string>

#include <Client/BuzzHouse/Utils/Nlimits.h>

namespace BuzzHouse
{

// Forward declaration to allow conversion between hugeint and uhugeint
struct UHugeInt;

struct HugeInt
{
public:
    uint64_t lower;
    int64_t upper;

    HugeInt() = default;
    explicit HugeInt(int64_t value);
    constexpr HugeInt(int64_t up, uint64_t lo) : lower(lo), upper(up) { }
    constexpr HugeInt(const HugeInt & rhs) = default;
    constexpr HugeInt(HugeInt && rhs) = default;
    HugeInt & operator=(const HugeInt & rhs) = default;
    HugeInt & operator=(HugeInt && rhs) = default;

    void toString(std::string & res) const;

    // comparison operators
    bool operator==(const HugeInt & rhs) const;
    bool operator!=(const HugeInt & rhs) const;
    bool operator<=(const HugeInt & rhs) const;
    bool operator<(const HugeInt & rhs) const;
    bool operator>(const HugeInt & rhs) const;
    bool operator>=(const HugeInt & rhs) const;

    // arithmetic operators
    HugeInt operator+(const HugeInt & rhs) const;
    HugeInt operator-(const HugeInt & rhs) const;
    HugeInt operator*(const HugeInt & rhs) const;
    HugeInt operator/(const HugeInt & rhs) const;
    HugeInt operator%(const HugeInt & rhs) const;
    HugeInt operator-() const;

    // bitwise operators
    HugeInt operator>>(const HugeInt & rhs) const;
    HugeInt operator<<(const HugeInt & rhs) const;
    HugeInt operator&(const HugeInt & rhs) const;
    HugeInt operator|(const HugeInt & rhs) const;
    HugeInt operator^(const HugeInt & rhs) const;
    HugeInt operator~() const;

    // in-place operators
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

    // boolean operators
    explicit operator bool() const;
    bool operator!() const;
};

}
