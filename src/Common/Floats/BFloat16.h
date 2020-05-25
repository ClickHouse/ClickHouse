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
 * Represents IEEE754 Brain Floating Point floating-point format (or bfloat16)
 */
namespace DB
{

static constexpr const unsigned short BFLOAT16_NAN = 0x7c01;

struct BFloat16 {
    unsigned short value;

    BFloat16() = default;
    explicit BFloat16(const unsigned short value_) : value(value_) { }

    explicit BFloat16(const float fl) {
        unsigned int fl32;
        unsigned short fl16;

        std::memcpy(&fl32, &fl, sizeof(unsigned int));
        fl16 = (fl32 >> 31) << 8;
        unsigned short exponent = (fl32 >> 23) & 0xff;
        fl16 = (fl16 | exponent) << 10;
        fl16 |= (fl32 >> 16) & 0x7f;
        value = fl16;
    }

    explicit BFloat16(const double fl) {
        value = BFloat16(static_cast<float>(fl)).getValue();
    }

    explicit BFloat16(const long int &l) {
        value = BFloat16(static_cast<double>(l)).getValue();
    }

    explicit BFloat16(const int i) {
        value = BFloat16(static_cast<float>(i)).getValue();
    }

    explicit BFloat16(const __int128 &i) {
        value = BFloat16(static_cast<float>(i)).getValue();
    }

    BFloat16(const BFloat16 &) = default;

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
        bool isNullFraction = (value << 9) == 0;
        return isNullFraction && (((value >> 7) & 0xff) == 0xff);
    }

    bool isNan() const {
        bool isNullFraction = (value << 9) == 0;
        return !isNullFraction && (((value >> 7) & 0xff) == 0xff);
    }


    unsigned short asShort() const { return value; }

    float asFloat() const {
        float fl;
        unsigned int fl32 = (value >> 15) << 8;
        unsigned int exponent = static_cast<unsigned int>((value >> 7) & 0xff);
        fl32 = (fl32 | exponent) << 22;
        fl32 |= (value & 0x7f) << 16;
        std::memcpy( &fl, &fl32, sizeof( float ) );
        return fl;
    }

    bool inline operator== (const BFloat16 fl) const { return (sign() == fl.sign()) && (withoutSign() == fl.withoutSign()); }
    bool inline operator!= (const BFloat16 fl) const { return (sign() != fl.sign()) || (withoutSign() != fl.withoutSign()); }
    bool inline operator<  (const BFloat16 fl) const {
        return asFloat() < fl.asFloat();
    }
    bool inline operator<= (const BFloat16 fl) const {
        return asFloat() <= fl.asFloat();
    }
    bool inline operator>  (const BFloat16 fl) const {
        return asFloat() > fl.asFloat();
    }
    bool inline operator>= (const BFloat16 fl) const {
        return asFloat() >= fl.asFloat();
    }

    BFloat16 & operator= (const BFloat16 &fl) = default;

    BFloat16 & operator= (const float &f) {
        value = BFloat16(f).getValue();
        return *this;
    }

    BFloat16 & operator= (const double &d) {
        value = BFloat16(d).getValue();
        return *this;
    }

    BFloat16 & operator= (const bool &b) {
        if (b) {
            value = 0x3f80;
        } else {
            value = 0;
        }
        return *this;
    }

    BFloat16 inline operator+(const BFloat16 &fl) const {
        if (isNull()) {
            return BFloat16(fl.getValue());
        }
        if (fl.isNull()) {
            return BFloat16(getValue());
        }
        unsigned short mantissa = value & 0x7f;
        unsigned short flMantissa = fl.getValue() & 0x7f;
        unsigned short exponent = (value >> 7) & 0xff;
        unsigned short flExponent = (fl.getValue() >> 7) & 0xff;
        while (exponent != flExponent) {
            if (exponent < flExponent) {
                exponent++;
                mantissa = mantissa >> 1;
                if (!mantissa) {
                    return BFloat16(fl.getValue());
                }
            } else {
                flExponent++;
                flMantissa = flMantissa >> 1;
                if (!flMantissa) {
                    return BFloat16(getValue());
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
                return BFloat16(BFLOAT16_NAN); 
            }
        }
        flExponent = exponent;
        while (!(bool)(resultingMantissa >> 6)) {
            exponent--;
            resultingMantissa = resultingMantissa << 1;
            if (flExponent - exponent != 1) {
                // report underflow
                return BFloat16(BFLOAT16_NAN);
            }
            flExponent--;
        }
        exponent = (exponent << 7) | resultingMantissa;
        if (resultingSign) {
            exponent |= (0x1 << 16);
        }
        return BFloat16(exponent);
    }

    BFloat16 inline operator-(const BFloat16 &fl) const {
        return BFloat16(getValue()) + BFloat16(static_cast<unsigned short>(static_cast<unsigned short>(0x1 << 15) ^ fl.getValue()));
    }

    BFloat16 inline operator*(const BFloat16 &fl) const {
        if (isNull() || fl.isNull()) {
            return BFloat16(static_cast<unsigned short>(0));
        }
        unsigned short resultingExponent;
        unsigned short exponentBias = 0x100;
        unsigned short exponent = (getValue() >> 7) & 0xff;
        unsigned short flExponent = (fl.getValue() >> 7) & 0xff;
        if (exponent > exponentBias) {
            resultingExponent = exponent - exponentBias + flExponent;
            if (resultingExponent - flExponent != exponent - exponentBias) {
                // report overflow
                return BFloat16(BFLOAT16_NAN);
            }
        } else if (flExponent > exponentBias) {
            resultingExponent = flExponent - exponentBias + exponent;
            if (resultingExponent - exponent != flExponent - exponentBias) {
                // report overflow
                return BFloat16(BFLOAT16_NAN);
            }
        } else {
            resultingExponent = exponent + flExponent - exponentBias;
            if (resultingExponent + exponentBias != exponent + flExponent) {
                // report underflow
                return BFloat16(BFLOAT16_NAN);
            }
        }
        unsigned short resultingMantissa = (getValue() & 0x7f) * (fl.getValue() & 0x7f);
        unsigned short signMask = ((getValue() >> 15) ^ (fl.getValue() >> 15)) << 15;
        unsigned short resultingExponentCopy = resultingExponent;
        while (!(bool)(resultingMantissa >> 6)) {
            resultingExponent--;
            resultingMantissa = resultingMantissa << 1;
            if (resultingExponentCopy - resultingExponent != 1) {
                // report underflow
                return BFloat16(BFLOAT16_NAN);
            }
            resultingExponentCopy--;
        }
        unsigned short resultValue = signMask | (resultingExponent << 7) | resultingMantissa;
        return BFloat16(resultValue);
    }

    BFloat16 inline operator/ (const BFloat16 &fl) const {
        if (isNull()) {
            return BFloat16(static_cast<unsigned short>(0));
        }
        if (fl.isNull()) {
            return BFloat16(BFLOAT16_NAN);
        }
        unsigned short resultingExponent;
        unsigned short exponentBias = 0x100;
        unsigned short exponent = (getValue() >> 7) & 0xff;
        unsigned short flExponent = (fl.getValue() >> 7) & 0xff;
        if (exponent > flExponent) {
            resultingExponent = exponent - flExponent + exponentBias;
            if (resultingExponent - exponentBias != exponent - flExponent) {
                // report overflow
                return BFloat16(BFLOAT16_NAN);
            }
        } else if (exponentBias > flExponent) {
            resultingExponent = exponentBias - flExponent + exponent;
            if (resultingExponent - exponent != exponentBias - flExponent) {
                // report overflow
                return BFloat16(BFLOAT16_NAN);
            }
        } else {
            resultingExponent = exponent + exponentBias - flExponent;
            if (resultingExponent + flExponent != exponent + exponentBias) {
                // report underflow
                return BFloat16(BFLOAT16_NAN);
            }
        }
        unsigned short resultingMantissa = (getValue() & 0x7f) / (fl.getValue() & 0x7f);
        unsigned short signMask = ((getValue() >> 15) ^ (fl.getValue() >> 15)) << 15;
        unsigned short resultingExponentCopy = resultingExponent;
        while (!static_cast<bool>(resultingMantissa >> 6)) {
            resultingExponent--;
            resultingMantissa = resultingMantissa << 1;
            if (resultingExponentCopy - resultingExponent != 1) {
                // report underflow
                return BFloat16(BFLOAT16_NAN);
            }
            resultingExponentCopy--;
        }
        unsigned short resultValue = signMask | (resultingExponent << 7) | resultingMantissa;
        return BFloat16(resultValue);
    }

    template <typename T> bool inline operator== (const T rhs) const { return *this == BFloat16(rhs); }
    template <typename T> bool inline operator!= (const T rhs) const { return *this != BFloat16(rhs); }
    template <typename T> bool inline operator>= (const T rhs) const { return *this >= BFloat16(rhs); }
    template <typename T> bool inline operator>  (const T rhs) const { return *this > BFloat16(rhs); }
    template <typename T> bool inline operator<= (const T rhs) const { return *this <= BFloat16(rhs); }
    template <typename T> bool inline operator<  (const T rhs) const { return *this <  BFloat16(rhs); }
    template <typename T> BFloat16 inline operator+ (const T rhs) { return *this + BFloat16(rhs); }
    template <typename T> BFloat16 inline operator- (const T rhs) { return *this - BFloat16(rhs); }
    template <typename T> BFloat16 inline operator* (const T rhs) { return *this * BFloat16(rhs); }
    template <typename T> BFloat16 inline operator/ (const T rhs) { return *this / BFloat16(rhs); }
    template <typename T> explicit operator T() const { return static_cast<T>(value); }
    explicit operator float() const { return asFloat(); }
    explicit operator double() const { return static_cast<double>(asFloat()); }
    explicit operator long int() const { return static_cast<long int>(asFloat()); }
};

template <typename T> bool inline operator== (T a, const BFloat16 b) { return BFloat16(a) == b; }
template <typename T> bool inline operator!= (T a, const BFloat16 b) { return BFloat16(a) != b; }
template <typename T> bool inline operator>= (T a, const BFloat16 b) { return BFloat16(a) >= b; }
template <typename T> bool inline operator>  (T a, const BFloat16 b) { return BFloat16(a) > b; }
template <typename T> bool inline operator<= (T a, const BFloat16 b) { return BFloat16(a) <= b; }
template <typename T> bool inline operator<  (T a, const BFloat16 b) { return BFloat16(a) < b; }
//template <typename T> BFloat16 inline operator+ (T a, const BFloat16 b) { return BFloat16(a) + b; }
//template <typename T> BFloat16 inline operator- (T a, const BFloat16 b) { return BFloat16(a) - b; }
//template <typename T> BFloat16 inline operator* (T a, const BFloat16 b) { return BFloat16(a) * b; }
//template <typename T> BFloat16 inline operator/ (T a, const BFloat16 b) { return BFloat16(a) / b; }
//template <typename T> BFloat16 inline operator+ (const BFloat16 a, T b) { return a + BFloat16(b); }
//template <typename T> BFloat16 inline operator- (const BFloat16 a, T b) { return a - BFloat16(b); }
//template <typename T> BFloat16 inline operator* (const BFloat16 a, T b) { return a * BFloat16(b); }
//template <typename T> BFloat16 inline operator/ (const BFloat16 a, T b) { return a / BFloat16(b); }

template <> inline constexpr bool IsNumber<BFloat16> = true;
template <> struct TypeName<BFloat16> { static const char * get() { return "BFloat16"; } };
template <> struct TypeId<BFloat16> { static constexpr const TypeIndex value = TypeIndex::BFloat16; };

}

template <> struct is_signed<DB::BFloat16>
{
    static constexpr bool value = false;
};

template <> struct is_unsigned<DB::BFloat16>
{
    static constexpr bool value = true;
};

template <> struct is_integral<DB::BFloat16>
{
    static constexpr bool value = true;
};

template <> struct is_arithmetic<DB::BFloat16>
{
    static constexpr bool value = false;
};

namespace std
{
template <>
struct hash<DB::BFloat16>
{
    size_t operator()(const DB::BFloat16 & u) const { return std::hash<DB::Int32>()(static_cast<unsigned int>(u.getValue())); }
};

}
