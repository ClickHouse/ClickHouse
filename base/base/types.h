#pragma once

#include <cmath>
#include <cstdint>
#include <string>

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

#ifndef __cpp_char8_t
using char8_t = unsigned char;
#endif

/// This is needed for more strict aliasing. https://godbolt.org/z/xpJBSb https://stackoverflow.com/a/57453713
using UInt8 = char8_t;

using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using String = std::string;

namespace DB
{

using UInt8 = ::UInt8;
using UInt16 = ::UInt16;
using UInt32 = ::UInt32;
using UInt64 = ::UInt64;

using Int8 = ::Int8;
using Int16 = ::Int16;
using Int32 = ::Int32;
using Int64 = ::Int64;

using Float32 = float;
using Float64 = double;

using String = std::string;

struct BFloat16 {
public:
    BFloat16() : data(0) {}

    explicit BFloat16(float value) {
      fromValue(value);
    }

    explicit BFloat16(uint16_t x): data(x) {}

    /// Take the most significant 16 bits of the floating point number.
    template <typename Value>
    static BFloat16 toBFloat16(const Value & x) {
        return BFloat16(std::bit_cast<DB::UInt32>(static_cast<DB::Float32>(x)) >> 16);
    }

    template <typename Value>
    inline void fromValue(const Value & x) {
        data = std::bit_cast<DB::UInt32>(static_cast<DB::Float32>(x)) >> 16;
    }

    /// Put the bits into most significant 16 bits of the floating point number and fill other bits with zeros.
    static Float32 toFloat32(const BFloat16 & x) {
        return std::bit_cast<DB::Float32>(x.data << 16);
    }

    Float32 toFloat32() const {
        return std::bit_cast<DB::Float32>(this->data << 16);
    }

    bool operator==(const BFloat16& other) const {
        return data == other.data;
    }

    bool operator!=(const BFloat16& other) const {
        return data != other.data;
    }

    bool operator<(const BFloat16& other) const {
        return toFloat32() < other.toFloat32();
    }

    bool operator<=(const BFloat16& other) const {
        return toFloat32() <= other.toFloat32();
    }

    bool operator>(const BFloat16& other) const {
        return toFloat32() > other.toFloat32();
    }

    bool operator>=(const BFloat16& other) const {
        return toFloat32() >= other.toFloat32();
    }

    BFloat16 operator+(const BFloat16& other) const {
        return BFloat16(toFloat32() + other.toFloat32());
    }

    BFloat16 operator-(const BFloat16& other) const {
        return BFloat16(toFloat32() - other.toFloat32());
    }

    BFloat16 operator*(const BFloat16& other) const {
        return BFloat16(toFloat32() * other.toFloat32());
    }

    BFloat16 operator/(const BFloat16& other) const {
        return BFloat16(toFloat32() / other.toFloat32());
    }

    BFloat16& operator+=(const BFloat16& other) {
        *this = *this + other;
        return *this;
    }

    BFloat16& operator-=(const BFloat16& other) {
        *this = *this - other;
        return *this;
    }

    BFloat16& operator*=(const BFloat16& other) {
        *this = *this * other;
        return *this;
    }

    BFloat16& operator/=(const BFloat16& other) {
        *this = *this / other;
        return *this;
    }

    BFloat16& operator++() {
        *this += BFloat16(1.0f);
        return *this;
    }

    BFloat16 operator++(int) {
        BFloat16 temp = *this;
        ++(*this);
        return temp;
    }

    BFloat16& operator--() {
        *this -= BFloat16(1.0f);
        return *this;
    }

    BFloat16 operator--(int) {
        BFloat16 temp = *this;
        --(*this);
        return temp;
    }

    operator float() const {
        return toFloat32();
    }

private:
    uint16_t data;
};

}
