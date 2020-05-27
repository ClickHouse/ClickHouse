#pragma once

#include <stdio.h>
#include <stdint.h>
#include <limits.h>
#include <cstring>
#include <iomanip>
#include <Core/Types.h>

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

/**
 * Represents IEEE754-2008 Half-precision floating-point format (or float16)
 */
namespace DB
{

static constexpr const unsigned short FLOAT16_NAN = 0x7c01;

struct Float16 {
    unsigned short value;

    Float16() = default;
    explicit Float16(const unsigned short value_) : value(value_) { }

    explicit Float16(const float fl) {
        unsigned int fl32;
        unsigned short fl16;

        std::memcpy(&fl32, &fl, sizeof(unsigned int));
        fl16 = (fl32 >> 31) << 5;
        unsigned short exponent = (fl32 >> 23) & 0xff;
        exponent = (exponent - 0x70) & (static_cast<unsigned int>(static_cast<int>(0x70 - exponent) >> 4) >> 27);
        fl16 = (fl16 | exponent) << 10;
        fl16 |= (fl32 >> 13) & 0x3ff;
        value = fl16;
    }

    explicit Float16(const double fl) {
        value = Float16(static_cast<float>(fl)).getValue();
    }

    explicit Float16(const long int &l) {
        value = Float16(static_cast<double>(l)).getValue();
    }

    explicit Float16(const int i) {
        value = Float16(static_cast<float>(i)).getValue();
    }

    explicit Float16(const __int128 &i) {
        value = Float16(static_cast<float>(i)).getValue();
    }

    Float16(const Float16 &) = default;

    unsigned short getValue() const {
        return value;
    }

    bool sign() const {
        return !static_cast<bool>((0x1 << 15) & value);
    }

    unsigned short withoutSign() const {
        return value & 0x7fff;
    }

    bool isNull() const {
        return !static_cast<bool>(value & 0x7fff);
    }

    bool isInfinity() const {
        bool isNullFraction = (value << 6) == 0;
        return isNullFraction && (((value >> 10) & 0x1f) == 0x1f);
    }

    bool isNan() const {
        bool isNullFraction = (value << 6) == 0;
        return !isNullFraction && (((value >> 10) & 0x1f) == 0x1f);
    }


    unsigned short asShort() const { return value; }

    float asFloat() const {
        float fl;
        unsigned int fl32 = (value >> 15) << 8;
        unsigned int exponent = static_cast<unsigned int>((value >> 11) & 0x1f);
        if (exponent == static_cast<unsigned short>(0x1f)) {
            exponent = static_cast<unsigned int>(0x100);
        } else {
            exponent = (0x7f - (exponent - 0xf));
        }
        fl32 = (fl32 | exponent) << 22;
        fl32 |= (value & 0x3ff) << 13;
        std::memcpy( &fl, &fl32, sizeof( float ) );
        return fl;
    }

    bool inline operator== (const Float16 fl) const { return (sign() == fl.sign()) && (withoutSign() == fl.withoutSign()); }
    bool inline operator!= (const Float16 fl) const { return (sign() != fl.sign()) || (withoutSign() != fl.withoutSign()); }
    bool inline operator<  (const Float16 fl) const {
        return asFloat() < fl.asFloat();
    }
    bool inline operator<= (const Float16 fl) const {
        return asFloat() <= fl.asFloat();
    }
    bool inline operator>  (const Float16 fl) const {
        return asFloat() > fl.asFloat();
    }
    bool inline operator>= (const Float16 fl) const {
        return asFloat() >= fl.asFloat();
    }

    Float16 & operator= (const Float16 &fl) = default;

    Float16 & operator= (const float &f) {
        value = Float16(f).getValue();
        return *this;
    }

    Float16 & operator= (const double &d) {
        value = Float16(d).getValue();
        return *this;
    }

    Float16 & operator= (const bool &b) {
        if (b) {
            value = 0x3c00;
        } else {
            value = 0;
        }
        return *this;
    }

    Float16 inline operator-() const {
        unsigned short updatedValue = getValue() ^ (1 << 15);
        return Float16(updatedValue);
    }

    Float16 inline operator+(const Float16 &fl) const {
        if (isNull()) {
            return Float16(fl.getValue());
        }
        if (fl.isNull()) {
            return Float16(getValue());
        }
        unsigned short mantissa = value & 0x3ff;
        unsigned short flMantissa = fl.getValue() & 0x3ff;
        unsigned short exponent = (value >> 10) & 0x1f;
        unsigned short flExponent = (fl.getValue() >> 10) & 0x1f;
        while (exponent != flExponent) {
            if (exponent < flExponent) {
                exponent++;
                mantissa = mantissa >> 1;
                if (!mantissa) {
                    return Float16(fl.getValue());
                }
            } else {
                flExponent++;
                flMantissa = flMantissa >> 1;
                if (!flMantissa) {
                    return Float16(getValue());
                }
            }
        }
        bool isOverflow = false;
        bool resultingSign;
        unsigned short resultingMantissa;
        if ((!sign() && !fl.sign()) || (sign() && fl.sign())) {
            resultingSign = sign();
            resultingMantissa = mantissa + flMantissa;
            isOverflow = (resultingMantissa - mantissa) - flMantissa;
        } else if (!sign() && fl.sign()) {
            resultingSign = mantissa - flMantissa < 0;
            if (resultingSign) {
                resultingMantissa = flMantissa - mantissa;
            } else {
                resultingMantissa = mantissa - flMantissa;
            }
        } else if (sign() && !fl.sign()) {
            resultingSign = flMantissa - mantissa < 0;
            if (resultingSign) {
                resultingMantissa = mantissa - flMantissa;
            } else {
                resultingMantissa = flMantissa - mantissa;
            }
        }
        if (isOverflow) {
            resultingMantissa = resultingMantissa >> 1;
            exponent++;
            if (flExponent - exponent != 1) {
                // report overflow
                return Float16(FLOAT16_NAN); 
            }
        }
        flExponent = exponent;
        while (!static_cast<bool>(resultingMantissa >> 9)) {
            exponent--;
            resultingMantissa = resultingMantissa << 1;
            if (flExponent - exponent != 1) {
                // report underflow
                return Float16(FLOAT16_NAN);
            }
            flExponent--;
        }
        exponent = (exponent << 10) | resultingMantissa;
        if (resultingSign) {
            exponent |= (0x1 << 16);
        }
        return Float16(exponent);
    }

    Float16 inline operator-(const Float16 &fl) const {
        return Float16(getValue()) + Float16(static_cast<unsigned short>(static_cast<unsigned short>(0x1 << 15) ^ fl.getValue()));
    }

    Float16 inline operator*(const Float16 &fl) const {
        if (isNull() || fl.isNull()) {
            return Float16(static_cast<unsigned short>(0));
        }
        unsigned short resultingExponent;
        unsigned short exponentBias = 0x10;
        unsigned short exponent = (getValue() >> 10) & 0x1f;
        unsigned short flExponent = (fl.getValue() >> 10) & 0x1f;
        if (exponent > exponentBias) {
            resultingExponent = exponent - exponentBias + flExponent;
            if (resultingExponent - flExponent != exponent - exponentBias) {
                // report overflow
                return Float16(FLOAT16_NAN);
            }
        } else if (flExponent > exponentBias) {
            resultingExponent = flExponent - exponentBias + exponent;
            if (resultingExponent - exponent != flExponent - exponentBias) {
                // report overflow
                return Float16(FLOAT16_NAN);
            }
        } else {
            resultingExponent = exponent + flExponent - exponentBias;
            if (resultingExponent + exponentBias != exponent + flExponent) {
                // report underflow
                return Float16(FLOAT16_NAN);
            }
        }
        unsigned short resultingMantissa = (getValue() & 0x3ff) * (fl.getValue() & 0x3ff);
        unsigned short signMask = ((getValue() >> 15) ^ (fl.getValue() >> 15)) << 15;
        unsigned short resultingExponentCopy = resultingExponent;
        while (!static_cast<bool>(resultingMantissa >> 9)) {
            resultingExponent--;
            resultingMantissa = resultingMantissa << 1;
            if (resultingExponentCopy - resultingExponent != 1) {
                // report underflow
                return Float16(FLOAT16_NAN);
            }
            resultingExponentCopy--;
        }
        unsigned short resultValue = signMask | (resultingExponent << 10) | resultingMantissa;
        return Float16(resultValue);
    }

    Float16 inline operator/(const Float16 &fl) const {
        if (isNull()) {
            return Float16(static_cast<unsigned short>(0));
        }
        if (fl.isNull()) {
            return Float16(FLOAT16_NAN);
        }
        unsigned short resultingExponent;
        unsigned short exponentBias = 0x10;
        unsigned short exponent = (getValue() >> 10) & 0x1f;
        unsigned short flExponent = (fl.getValue() >> 10) & 0x1f;
        if (exponent > flExponent) {
            resultingExponent = exponent - flExponent + exponentBias;
            if (resultingExponent - exponentBias != exponent - flExponent) {
                // report overflow
                return Float16(FLOAT16_NAN);
            }
        } else if (exponentBias > flExponent) {
            resultingExponent = exponentBias - flExponent + exponent;
            if (resultingExponent - exponent != exponentBias - flExponent) {
                // report overflow
                return Float16(FLOAT16_NAN);
            }
        } else {
            resultingExponent = exponent + exponentBias - flExponent;
            if (resultingExponent + flExponent != exponent + exponentBias) {
                // report underflow
                return Float16(FLOAT16_NAN);
            }
        }
        unsigned short resultingMantissa = (getValue() & 0x3ff) / (fl.getValue() & 0x3ff);
        unsigned short signMask = ((getValue() >> 15) ^ (fl.getValue() >> 15)) << 15;
        unsigned short resultingExponentCopy = resultingExponent;
        while (!static_cast<bool>(resultingMantissa >> 9)) {
            resultingExponent--;
            resultingMantissa = resultingMantissa << 1;
            if (resultingExponentCopy - resultingExponent != 1) {
                // report underflow
                return Float16(FLOAT16_NAN);
            }
            resultingExponentCopy--;
        }
        unsigned short resultValue = signMask | (resultingExponent << 10) | resultingMantissa;
        return Float16(resultValue);
    }

    template <typename T> bool inline operator== (const T rhs) const { return *this == Float16(rhs); }
    template <typename T> bool inline operator!= (const T rhs) const { return *this != Float16(rhs); }
    template <typename T> bool inline operator>= (const T rhs) const { return *this >= Float16(rhs); }
    template <typename T> bool inline operator>  (const T rhs) const { return *this > Float16(rhs); }
    template <typename T> bool inline operator<= (const T rhs) const { return *this <= Float16(rhs); }
    template <typename T> bool inline operator<  (const T rhs) const { return *this <  Float16(rhs); }
    template <typename T> Float16 inline operator+ (const T rhs) { return *this + Float16(rhs); }
    template <typename T> Float16 inline operator- (const T rhs) { return *this - Float16(rhs); }
    template <typename T> Float16 inline operator* (const T rhs) { return *this * Float16(rhs); }
    template <typename T> Float16 inline operator/ (const T rhs) { return *this / Float16(rhs); }
    template <typename T> explicit operator T() const { return static_cast<T>(value); }
    explicit operator float() const { return asFloat(); }
    explicit operator double() const { return static_cast<double>(asFloat()); }
    explicit operator long int() const { return static_cast<long int>(asFloat()); }
};

template <typename T> bool inline operator== (T a, const Float16 b) { return Float16(a) == b; }
template <typename T> bool inline operator!= (T a, const Float16 b) { return Float16(a) != b; }
template <typename T> bool inline operator>= (T a, const Float16 b) { return Float16(a) >= b; }
template <typename T> bool inline operator>  (T a, const Float16 b) { return Float16(a) > b; }
template <typename T> bool inline operator<= (T a, const Float16 b) { return Float16(a) <= b; }
template <typename T> bool inline operator<  (T a, const Float16 b) { return Float16(a) < b; }
//template <typename T> Float16 inline operator+ (T a, const Float16 b) { return Float16(a) + b; }
//template <typename T> Float16 inline operator- (T a, const Float16 b) { return Float16(a) - b; }
//template <typename T> Float16 inline operator* (T a, const Float16 b) { return Float16(a) * b; }
//template <typename T> Float16 inline operator/ (T a, const Float16 b) { return Float16(a) / b; }
//template <typename T> Float16 inline operator+ (const Float16 a, T b) { return a + Float16(b); }
//template <typename T> Float16 inline operator- (const Float16 a, T b) { return a - Float16(b); }
//template <typename T> Float16 inline operator* (const Float16 a, T b) { return a * Float16(b); }
//template <typename T> Float16 inline operator/ (const Float16 a, T b) { return a / Float16(b); }

template <> inline constexpr bool IsNumber<Float16> = true;
template <> struct TypeName<Float16> { static const char * get() { return "Float16"; } };
template <> struct TypeId<Float16> { static constexpr const TypeIndex value = TypeIndex::Float16; };

}

template <> struct is_signed<DB::Float16>
{
    static constexpr bool value = false;
};

template <> struct is_unsigned<DB::Float16>
{
    static constexpr bool value = true;
};

template <> struct is_integral<DB::Float16>
{
    static constexpr bool value = true;
};

template <> struct is_arithmetic<DB::Float16>
{
    static constexpr bool value = false;
};

namespace std
{
template <>
struct hash<DB::Float16>
{
    size_t operator()(const DB::Float16 & u) const { return std::hash<DB::UInt32>()(static_cast<unsigned int>(u.getValue())); }
};

}