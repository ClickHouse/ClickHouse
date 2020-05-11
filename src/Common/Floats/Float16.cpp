#include <stdio.h>
#include <stdint.h>
#include <limits.h>
#include <cstring>
#include <iomanip>
#include <Core/Types.h>

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif


namespace DB
{

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
        exponent = (exponent - 0x70) & ((unsigned int)((int)(0x70 - exponent) >> 4) >> 27);
        fl16 = (fl16 | exponent) << 10;
        fl16 |= (fl32 >> 13) & 0x3ff;
        value = fl16;
    }

    bool sign() const {
        return (bool(~(1 << 16) & value));
    }

    unsigned short withoutSign() const {
        return value & (~(1 << 16));
    }

    unsigned short asShort() const { return value; }

    float asFloat() const {
        float fl;
        unsigned int fl32 = (value >> 15) << 8;
        unsigned int exponent = (unsigned int)((value >> 11) & 0x1f);
        if (exponent == (unsigned short) 0x1f) {
            exponent = (unsigned int) 0x100;
            fl32 = (fl32 | exponent) << 22;
        } else {
            exponent = (0x7f - (exponent - 0xf));
        }
        fl32 = (fl32 | exponent) << 22;
        fl32 |= (value & 0x3ff) << 13;
        std::memcpy( &fl, &fl32, sizeof( float ) );
        return fl;
    }

    bool inline operator== (const Float16 fl) const { return withoutSign() == fl.withoutSign(); }
    bool inline operator!= (const Float16 fl) const { return withoutSign() != fl.withoutSign(); }
    bool inline operator<  (const Float16 fl) const {
        return (sign() && !fl.sign()) && ((!sign() && fl.sign()) || (!fl.sign()*(withoutSign() > fl.withoutSign())));
    }
    bool inline operator<= (const Float16 fl) const {
        return (withoutSign() == fl.withoutSign()) || ((sign() && !fl.sign()) && ((!sign() && fl.sign()) || (!fl.sign()*(withoutSign() > fl.withoutSign()))));
    }
    bool inline operator>  (const Float16 fl) const {
        return (!sign() && fl.sign()) && ((sign() && !fl.sign()) || (!sign()*(withoutSign() < fl.withoutSign())));
    }
    bool inline operator>= (const Float16 fl) const {
        return (withoutSign() == fl.withoutSign()) || ((!sign() && fl.sign()) && ((sign() && !fl.sign()) || (!sign()*(withoutSign() < fl.withoutSign()))));
    }

    Float16 inline operator+(const Float16 fl) const {
        if (!sign() && !fl.sign()) {
            return positiveAddition(withoutSign(), fl.withoutSign());
        } else if (sign() && fl.sign()) {
            positiveAddition(withoutSign(), fl.withoutSign());
        return fl;
    }

    Float16 positiveAddition(const Float16 fl1, const Float16 fl2) const {
        return fl1;
    }

    Float16 positiveSubtraction(const Float16 fl1, const Float16 fl2) const {
        return fl1;
    }

    Float16 positiveMultiplication(const Float16 fl1, const Float16 fl2) const {
        return fl1;
    }

    Float16 positiveDivision(const Float16 fl1, const Float16 fl2) const {
        return fl1;
    }

    template <typename T> bool inline operator== (const T rhs) const { return *this == Float16(rhs); }
    template <typename T> bool inline operator!= (const T rhs) const { return *this != Float16(rhs); }
    template <typename T> bool inline operator>= (const T rhs) const { return *this >= Float16(rhs); }
    template <typename T> bool inline operator>  (const T rhs) const { return *this > Float16(rhs); }
    template <typename T> bool inline operator<= (const T rhs) const { return *this <= Float16(rhs); }
    template <typename T> bool inline operator<  (const T rhs) const { return *this <  Float16(rhs); }
    template <typename T> explicit operator T() const { return static_cast<T>(value); }
};

template <typename T> bool inline operator== (T a, const Float16 b) { return Float16(a) == b; }
template <typename T> bool inline operator!= (T a, const Float16 b) { return Float16(a) != b; }
template <typename T> bool inline operator>= (T a, const Float16 b) { return Float16(a) >= b; }
template <typename T> bool inline operator>  (T a, const Float16 b) { return Float16(a) > b; }
template <typename T> bool inline operator<= (T a, const Float16 b) { return Float16(a) <= b; }
template <typename T> bool inline operator<  (T a, const Float16 b) { return Float16(a) < b; }

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
