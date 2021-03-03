#pragma once

#include <tuple>
#include <iomanip>
#include <city.h>

#include <Core/Types.h>
#include <Common/hex.h>

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif


namespace DB
{

/// For aggregation by SipHash, UUID type or concatenation of several fields.
struct UInt128
{
/// Suppress gcc7 warnings: 'prev_key.DB::UInt128::low' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    /// This naming assumes little endian.
    UInt64 low;
    UInt64 high;

    UInt128() = default;
    explicit UInt128(const UInt64 low_, const UInt64 high_) : low(low_), high(high_) {}

    /// We need Int128 to UInt128 conversion or AccurateComparison will call greaterOp<Int128, UInt64> instead of greaterOp<Int128, UInt128>
    explicit UInt128(const Int128 rhs) : low(rhs), high(rhs >> 64) {}
    explicit UInt128(const Int64 rhs) : low(rhs), high() {}
    explicit UInt128(const Int32 rhs) : low(rhs), high() {}
    explicit UInt128(const Int16 rhs) : low(rhs), high() {}
    explicit UInt128(const Int8 rhs) : low(rhs), high() {}
    explicit UInt128(const UInt8 rhs) : low(rhs), high() {}
    explicit UInt128(const UInt16 rhs) : low(rhs), high() {}
    explicit UInt128(const UInt32 rhs) : low(rhs), high() {}
    explicit UInt128(const UInt64 rhs) : low(rhs), high() {}
    explicit UInt128(const Float32 rhs) : low(rhs), high() {}
    explicit UInt128(const Float64 rhs) : low(rhs), high() {}

    auto tuple() const { return std::tie(high, low); }

    String toHexString() const
    {
        String res(2 * sizeof(UInt128), 0);
        writeHexUIntLowercase(*this, res.data());
        return res;
    }

    bool inline operator== (const UInt128 rhs) const { return tuple() == rhs.tuple(); }
    bool inline operator!= (const UInt128 rhs) const { return tuple() != rhs.tuple(); }
    bool inline operator<  (const UInt128 rhs) const { return tuple() < rhs.tuple(); }
    bool inline operator<= (const UInt128 rhs) const { return tuple() <= rhs.tuple(); }
    bool inline operator>  (const UInt128 rhs) const { return tuple() > rhs.tuple(); }
    bool inline operator>= (const UInt128 rhs) const { return tuple() >= rhs.tuple(); }

    bool inline operator == (const Int128 rhs) const { return *this == UInt128(rhs, rhs >> 64) && rhs >= 0; }
    bool inline operator != (const Int128 rhs) const { return *this != UInt128(rhs, rhs >> 64) || rhs < 0; }
    bool inline operator >= (const Int128 rhs) const { return *this >= UInt128(rhs, rhs >> 64) || rhs < 0; }
    bool inline operator >  (const Int128 rhs) const { return *this >  UInt128(rhs, rhs >> 64) || rhs < 0; }
    bool inline operator <= (const Int128 rhs) const { return *this <= UInt128(rhs, rhs >> 64) && rhs >= 0; }
    bool inline operator <  (const Int128 rhs) const { return *this <  UInt128(rhs, rhs >> 64) && rhs >= 0; }

    bool inline operator >  (const Int256 rhs) const { return (rhs < 0) || ((Int256(high) << 64) + low) > rhs; }
    bool inline operator >  (const UInt256 rhs) const { return ((UInt256(high) << 64) + low) > rhs; }
    bool inline operator <  (const Int256 rhs) const { return (rhs >= 0) && ((Int256(high) << 64) + low) < rhs; }
    bool inline operator <  (const UInt256 rhs) const { return ((UInt256(high) << 64) + low) < rhs; }

    template <typename T> bool inline operator== (const T rhs) const { return *this == UInt128(rhs); }
    template <typename T> bool inline operator!= (const T rhs) const { return *this != UInt128(rhs); }
    template <typename T> bool inline operator>= (const T rhs) const { return *this >= UInt128(rhs); }
    template <typename T> bool inline operator>  (const T rhs) const { return *this >  UInt128(rhs); }
    template <typename T> bool inline operator<= (const T rhs) const { return *this <= UInt128(rhs); }
    template <typename T> bool inline operator<  (const T rhs) const { return *this <  UInt128(rhs); }

    template <typename T> explicit operator T() const
    {
        if constexpr (std::is_class_v<T>)
            return T();
        else
            return static_cast<T>(low);
    }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    UInt128 & operator= (const UInt64 rhs) { low = rhs; high = 0; return *this; }
};

template <typename T> bool inline operator == (T a, const UInt128 b) { return b.operator==(a); }
template <typename T> bool inline operator != (T a, const UInt128 b) { return b.operator!=(a); }
template <typename T> bool inline operator >= (T a, const UInt128 b) { return b <= a; }
template <typename T> bool inline operator >  (T a, const UInt128 b) { return b < a; }
template <typename T> bool inline operator <= (T a, const UInt128 b) { return b >= a; }
template <typename T> bool inline operator <  (T a, const UInt128 b) { return b > a; }

template <> inline constexpr bool IsNumber<UInt128> = true;
template <> struct TypeName<UInt128> { static constexpr const char * get() { return "UInt128"; } };
template <> struct TypeId<UInt128> { static constexpr const TypeIndex value = TypeIndex::UInt128; };

struct UInt128Hash
{
    size_t operator()(UInt128 x) const
    {
        return CityHash_v1_0_2::Hash128to64({x.low, x.high});
    }
};

#ifdef __SSE4_2__

struct UInt128HashCRC32
{
    size_t operator()(UInt128 x) const
    {
        UInt64 crc = -1ULL;
        crc = _mm_crc32_u64(crc, x.low);
        crc = _mm_crc32_u64(crc, x.high);
        return crc;
    }
};

#else

/// On other platforms we do not use CRC32. NOTE This can be confusing.
struct UInt128HashCRC32 : public UInt128Hash {};

#endif

struct UInt128TrivialHash
{
    size_t operator()(UInt128 x) const { return x.low; }
};


/** Used for aggregation, for putting a large number of constant-length keys in a hash table.
  */
struct DummyUInt256
{

/// Suppress gcc7 warnings: 'prev_key.DB::UInt256::a' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    UInt64 a;
    UInt64 b;
    UInt64 c;
    UInt64 d;

    bool operator== (const DummyUInt256 rhs) const
    {
        return a == rhs.a && b == rhs.b && c == rhs.c && d == rhs.d;

    /* So it's no better.
        return 0xFFFF == _mm_movemask_epi8(_mm_and_si128(
            _mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&a)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&rhs.a))),
            _mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&c)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&rhs.c)))));*/
    }

    bool operator!= (const DummyUInt256 rhs) const { return !operator==(rhs); }

    bool operator== (const UInt64 rhs) const { return a == rhs && b == 0 && c == 0 && d == 0; }
    bool operator!= (const UInt64 rhs) const { return !operator==(rhs); }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    DummyUInt256 & operator = (const UInt64 rhs) { a = rhs; b = 0; c = 0; d = 0; return *this; }
};

struct UInt256Hash
{
    size_t operator()(DummyUInt256 x) const
    {
        /// NOTE suboptimal
        return CityHash_v1_0_2::Hash128to64({CityHash_v1_0_2::Hash128to64({x.a, x.b}), CityHash_v1_0_2::Hash128to64({x.c, x.d})});
    }
};

#ifdef __SSE4_2__

struct UInt256HashCRC32
{
    size_t operator()(DummyUInt256 x) const
    {
        UInt64 crc = -1ULL;
        crc = _mm_crc32_u64(crc, x.a);
        crc = _mm_crc32_u64(crc, x.b);
        crc = _mm_crc32_u64(crc, x.c);
        crc = _mm_crc32_u64(crc, x.d);
        return crc;
    }
};

#else

/// We do not need to use CRC32 on other platforms. NOTE This can be confusing.
struct UInt256HashCRC32 : public UInt256Hash {};

#endif

}

template <> struct is_signed<DB::UInt128>
{
    static constexpr bool value = false;
};

template <> struct is_unsigned<DB::UInt128>
{
    static constexpr bool value = true;
};

template <> struct is_integer<DB::UInt128>
{
    static constexpr bool value = true;
};

// Operator +, -, /, *, % aren't implemented so it's not an arithmetic type
template <> struct is_arithmetic<DB::UInt128>
{
    static constexpr bool value = false;
};

/// Overload hash for type casting
namespace std
{
template <> struct hash<DB::UInt128>
{
    size_t operator()(const DB::UInt128 & u) const
    {
        return CityHash_v1_0_2::Hash128to64({u.low, u.high});
    }
};

}
