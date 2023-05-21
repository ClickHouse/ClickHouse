#pragma once

#include <cmath>
#include <cstdint>
#include <string>
#include <type_traits>
#include <ostream>

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
<<<<<<< HEAD
    BFloat16() = default;

    template<typename Type>
    BFloat16(Type value) {
      fromValue(value);
    }

=======
    BFloat16() : data(0) {}

    explicit BFloat16(float value) {
      fromValue(value);
    }

    explicit BFloat16(uint16_t x): data(x) {}

>>>>>>> 01c711c1c4b4675053955fcc8f1439453c707729
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
<<<<<<< HEAD
        return std::bit_cast<Float32>(x.data << 16);
    }

    Float32 toFloat32() const {
        return std::bit_cast<Float32>(this->data << 16);
    }

    Float64 toFloat64() const {
        return std::bit_cast<Float64>(UInt64(this->data) << 32);
    }

    friend inline BFloat16 operator+(BFloat16 a, BFloat16 b) {
        return BFloat16(static_cast<float>(a) + static_cast<float>(b));
    }

    friend inline BFloat16 operator-(BFloat16 a, BFloat16 b) {
        return BFloat16(static_cast<float>(a) - static_cast<float>(b));
    }

    friend inline BFloat16 operator*(BFloat16 a, BFloat16 b) {
        return BFloat16(static_cast<float>(a) * static_cast<float>(b));
    }

    friend inline BFloat16 operator/(BFloat16 a, BFloat16 b) {
        return BFloat16(static_cast<float>(a) / static_cast<float>(b));
    }

    friend inline BFloat16 operator-(BFloat16 a) {
        a.data ^= 0x8000;
        return a;
    }

    friend inline bool operator<(BFloat16 a, BFloat16 b) {
        return static_cast<float>(a) < static_cast<float>(b);
    }

    friend inline bool operator<=(BFloat16 a, BFloat16 b) {
        return static_cast<float>(a) <= static_cast<float>(b);
    }

    friend inline bool operator==(BFloat16 a, BFloat16 b) {
        return static_cast<float>(a) == static_cast<float>(b);
    }

    friend inline bool operator!=(BFloat16 a, BFloat16 b) {
        return static_cast<float>(a) != static_cast<float>(b);
    }

    friend inline bool operator>(BFloat16 a, BFloat16 b) {
        return static_cast<float>(a) > static_cast<float>(b);
    }

    friend inline bool operator>=(BFloat16 a, BFloat16 b) {
        return static_cast<float>(a) >= static_cast<float>(b);
    }

    friend inline BFloat16& operator+=(BFloat16& a, BFloat16 b) {
        a = a + b;
        return a;
    }

    friend inline BFloat16& operator-=(BFloat16& a, BFloat16 b) {
        a = a - b;
        return a;
    }

    friend inline BFloat16 operator++(BFloat16& a) {
        a += BFloat16(1);
        return a;
    }

    friend inline BFloat16 operator--(BFloat16& a) {
        a -= BFloat16(1);
        return a;
    }

    friend inline BFloat16 operator++(BFloat16& a, int) {
        BFloat16 temp = a;
        ++a;
        return temp;
    }

    friend inline BFloat16 operator--(BFloat16& a, int) {
        BFloat16 temp = a;
        --a;
        return temp;
    }

    friend inline BFloat16& operator*=(BFloat16& a, BFloat16 b) {
        a = a * b;
        return a;
    }

    friend inline BFloat16& operator/=(BFloat16& a, BFloat16 b) {
        a = a / b;
        return a;
    }

=======
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

>>>>>>> 01c711c1c4b4675053955fcc8f1439453c707729
    operator float() const {
        return toFloat32();
    }

<<<<<<< HEAD
    explicit operator bool() const {
        return static_cast<bool>(float(*this));
    }

    explicit operator short() const {
        return static_cast<short>(float(*this));
    }

    explicit operator int() const {
        return static_cast<int>(float(*this));
    }

    explicit operator long() const {
        return static_cast<long>(float(*this));
    }

    explicit operator char() const {
        return static_cast<char>(float(*this));
    }

    explicit operator signed char() const {
        return static_cast<signed char>(float(*this));
    }

    explicit operator unsigned char() const {
        return static_cast<unsigned char>(float(*this));
    }

    explicit operator unsigned short() const {
        return static_cast<unsigned short>(float(*this));
    }

    explicit operator unsigned int() const {
        return static_cast<unsigned int>(float(*this));
    }

    explicit operator unsigned long() const {
        return static_cast<unsigned long>(float(*this));
    }

    explicit operator unsigned long long() const {
        return static_cast<unsigned long long>(float(*this));
    }

    explicit operator long long() const {
        return static_cast<long long>(float(*this));
    }

    explicit operator double() const {
        return static_cast<double>(float(*this));
    }

    friend inline std::ostream& operator<<(std::ostream& os, const BFloat16& bf) {
        os << static_cast<float>(bf);
        return os;
    }

/// Arithmetic with floats

    friend inline float operator+(BFloat16 a, float b) {
        return static_cast<float>(a) + b;
    }
    friend inline float operator-(BFloat16 a, float b) {
        return static_cast<float>(a) - b;
    }
    friend inline float operator*(BFloat16 a, float b) {
        return static_cast<float>(a) * b;
    }
    friend inline float operator/(BFloat16 a, float b) {
        return static_cast<float>(a) / b;
    }

    friend inline float operator+(float a, BFloat16 b) {
        return a + static_cast<float>(b);
    }
    friend inline float operator-(float a, BFloat16 b) {
        return a - static_cast<float>(b);
    }
    friend inline float operator*(float a, BFloat16 b) {
        return a * static_cast<float>(b);
    }
    friend inline float operator/(float a, BFloat16 b) {
        return a / static_cast<float>(b);
    }

    friend inline float& operator+=(float& a, const BFloat16& b) {
        return a += static_cast<float>(b);
    }
    friend inline float& operator-=(float& a, const BFloat16& b) {
        return a -= static_cast<float>(b);
    }
    friend inline float& operator*=(float& a, const BFloat16& b) {
        return a *= static_cast<float>(b);
    }
    friend inline float& operator/=(float& a, const BFloat16& b) {
        return a /= static_cast<float>(b);
    }

=======
>>>>>>> 01c711c1c4b4675053955fcc8f1439453c707729
private:
    uint16_t data;
};

<<<<<<< HEAD
}

namespace std {

using DB::BFloat16;

inline bool signbit(const BFloat16& a) { return std::signbit(float(a)); }
inline bool isinf(const BFloat16& a) { return std::isinf(float(a)); }
inline bool isnan(const BFloat16& a) { return std::isnan(float(a)); }
inline bool isfinite(const BFloat16& a) { return std::isfinite(float(a)); }
inline BFloat16 abs(const BFloat16& a) { return BFloat16(std::abs(float(a))); }
inline BFloat16 exp(const BFloat16& a) { return BFloat16(std::exp(float(a))); }
inline BFloat16 log(const BFloat16& a) { return BFloat16(std::log(float(a))); }

inline BFloat16 log10(const BFloat16& a) {
  return BFloat16(std::log10(float(a)));
}

inline BFloat16 sqrt(const BFloat16& a) {
  return BFloat16(std::sqrt(float(a)));
}

inline BFloat16 pow(const BFloat16& a, const BFloat16& b) {
  return BFloat16(std::pow(float(a), float(b)));
}

inline BFloat16 sin(const BFloat16& a) { return BFloat16(std::sin(float(a))); }
inline BFloat16 cos(const BFloat16& a) { return BFloat16(std::cos(float(a))); }
inline BFloat16 tan(const BFloat16& a) { return BFloat16(std::tan(float(a))); }

inline BFloat16 tanh(const BFloat16& a) {
  return BFloat16(std::tanh(float(a)));
}

inline BFloat16 floor(const BFloat16& a) {
  return BFloat16(std::floor(float(a)));
}

inline BFloat16 ceil(const BFloat16& a) {
  return BFloat16(std::ceil(float(a)));
}

=======
>>>>>>> 01c711c1c4b4675053955fcc8f1439453c707729
}
