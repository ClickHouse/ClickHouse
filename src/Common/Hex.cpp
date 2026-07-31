#include <Common/Hex.h>
#include <Common/TargetSpecific.h>
#include <base/defines.h>
#include <base/extended_types.h>

#if USE_MULTITARGET_CODE
#define FAST_HEX_AVX 1
#define FAST_HEX_AVX2 1
#endif
#if defined(__aarch64__)
#define FAST_HEX_NEON 1
#endif
#define FAST_HEX_USE_NAMESPACE 1
#include <fast_hex/fast_hex_inline.hpp>


#if defined(__aarch64__)

DECLARE_DEFAULT_CODE(

template <typename Case>
static void encodeHexIntImpl(uint8_t * dst, const void * value, size_t num_bytes, Case c)
{
    switch (num_bytes)
    {
        case 8:
        {
            UInt64 v;
            memcpy(&v, value, 8);
            heks::encode_integral8(dst, v, c);
            return;
        }
        case 16:
        {
            UInt128 v;
            memcpy(&v, value, 16);
            heks::encode_integral16(dst, v, c);
            return;
        }
        case 32:
        {
            constexpr auto case_type = Case::value;
            const auto * src = static_cast<const uint8_t *>(value);
            heks::heks_detail::encodeHexNeon16_impl<case_type, heks::heks_detail::Reverse::Yes128>(dst, src + 16);
            heks::heks_detail::encodeHexNeon16_impl<case_type, heks::heks_detail::Reverse::Yes128>(dst + 32, src);
            return;
        }
        default:
            UNREACHABLE();
    }
}

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case)
{
    constexpr auto case_type = Case::value;
    heks::heks_detail::encodeHexNeon_impl<case_type>(dst, src, heks::RawLength{size});
}

static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    heks::decodeHexLUT(dst, src, heks::RawLength{size});
}

template <typename Case>
static void encodeHex16LEImpl(uint8_t * dst, const uint8_t * src, Case)
{
    constexpr auto case_type = Case::value;
    heks::heks_detail::encodeHexNeon16_impl<case_type, heks::heks_detail::Reverse::Yes128>(dst, src);
}

template <bool = true>
static void decodeHexStringsImpl(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
{
    UInt64 src_prev = 0;
    UInt64 dst_pos = 0;
    for (size_t i = 0; i < row_count; ++i)
    {
        UInt64 src_end = src_offsets[i];
        size_t hex_len = src_end - src_prev;
        if (hex_len & 1)
        {
            dst[dst_pos] = impl::unhexDigit(static_cast<char>(src[src_prev]));
            ++dst_pos;
            ++src_prev;
            --hex_len;
        }
        size_t raw_size = hex_len / 2;
        decodeHexStringImpl(dst + dst_pos, src + src_prev, raw_size);
        dst_pos += raw_size;
        dst_offsets[i] = dst_pos;
        src_prev = src_end;
    }
}

) // DECLARE_DEFAULT_CODE

#else

DECLARE_DEFAULT_CODE(

template <typename Case>
static void encodeHexIntImpl(uint8_t * dst, const void * value, size_t num_bytes, Case c)
{
    switch (num_bytes)
    {
        case 8:
        {
            UInt64 v;
            memcpy(&v, value, 8);
            heks::encode_integral_naive(dst, v, c);
            return;
        }
        case 16:
        {
            UInt128 v;
            memcpy(&v, value, 16);
            heks::encode_integral_naive(dst, v, c);
            return;
        }
        case 32:
        {
            UInt256 v;
            memcpy(&v, value, 32);
            heks::encode_integral_naive(dst, v, c);
            return;
        }
        default:
            UNREACHABLE();
    }
}

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case)
{
    constexpr auto case_type = Case::value;
    heks::heks_detail::encodeHexImpl<case_type>(dst, src, heks::RawLength{size});
}

static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    heks::decodeHexLUT4(dst, src, heks::RawLength{size});
}

template <typename Case>
static void encodeHex16LEImpl(uint8_t * dst, const uint8_t * src, Case c)
{
    UInt64 high;
    UInt64 low;
    memcpy(&high, src + 8, 8);
    memcpy(&low, src, 8);
    heks::encode_integral_naive(dst, high, c);
    heks::encode_integral_naive(dst + 16, low, c);
}

template <bool = true>
static void decodeHexStringsImpl(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
{
    UInt64 src_prev = 0;
    UInt64 dst_pos = 0;
    for (size_t i = 0; i < row_count; ++i)
    {
        UInt64 src_end = src_offsets[i];
        size_t hex_len = src_end - src_prev;
        if (hex_len & 1)
        {
            dst[dst_pos] = impl::unhexDigit(static_cast<char>(src[src_prev]));
            ++dst_pos;
            ++src_prev;
            --hex_len;
        }
        size_t raw_size = hex_len / 2;
        decodeHexStringImpl(dst + dst_pos, src + src_prev, raw_size);
        dst_pos += raw_size;
        dst_offsets[i] = dst_pos;
        src_prev = src_end;
    }
}

) // DECLARE_DEFAULT_CODE

#endif


DECLARE_X86_64_V3_SPECIFIC_CODE(

template <typename Case>
static void encodeHexIntImpl(uint8_t * dst, const void * value, size_t num_bytes, Case c)
{
    switch (num_bytes)
    {
        case 8:
        {
            UInt64 v;
            memcpy(&v, value, 8);
            heks::encode_integral8(dst, v, c);
            return;
        }
        case 16:
        {
            UInt128 v;
            memcpy(&v, value, 16);
            heks::encode_integral16(dst, v, c);
            return;
        }
        case 32:
        {
            constexpr auto case_type = Case::value;
            const auto * src = static_cast<const uint8_t *>(value);
            heks::heks_detail::encodeHex16Fast<case_type, heks::heks_detail::Reverse::Yes128>(dst, src + 16);
            heks::heks_detail::encodeHex16Fast<case_type, heks::heks_detail::Reverse::Yes128>(dst + 32, src);
            return;
        }
        default:
            UNREACHABLE();
    }
}

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case)
{
    constexpr auto case_type = Case::value;
    heks::heks_detail::encodeHexVecImpl<case_type>(dst, src, heks::RawLength{size});
}

static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    if (size >= 32)
        heks::decodeHexVec(dst, src, heks::RawLength{size});
    else
        heks::decodeHexLUT4(dst, src, heks::RawLength{size});
}

template <typename Case>
static void encodeHex16LEImpl(uint8_t * dst, const uint8_t * src, Case)
{
    constexpr auto case_type = Case::value;
    heks::heks_detail::encodeHex16Fast<case_type, heks::heks_detail::Reverse::Yes128>(dst, src);
}

template <bool = true>
static void decodeHexStringsImpl(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
{
    UInt64 src_prev = 0;
    UInt64 dst_pos = 0;
    for (size_t i = 0; i < row_count; ++i)
    {
        UInt64 src_end = src_offsets[i];
        size_t hex_len = src_end - src_prev;
        if (hex_len & 1)
        {
            dst[dst_pos] = impl::unhexDigit(static_cast<char>(src[src_prev]));
            ++dst_pos;
            ++src_prev;
            --hex_len;
        }
        size_t raw_size = hex_len / 2;
        decodeHexStringImpl(dst + dst_pos, src + src_prev, raw_size);
        dst_pos += raw_size;
        dst_offsets[i] = dst_pos;
        src_prev = src_end;
    }
}

) // DECLARE_X86_64_V3_SPECIFIC_CODE


#ifndef HEX_GTEST_UNIT_TEST

namespace DB
{

void encodeHexIntUpper(uint8_t * dst, const void * value, size_t num_bytes)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::encodeHexIntImpl(dst, value, num_bytes, heks::upper);
        return;
    }
#endif
    ::TargetSpecific::Default::encodeHexIntImpl(dst, value, num_bytes, heks::upper);
}

void encodeHexIntLower(uint8_t * dst, const void * value, size_t num_bytes)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::encodeHexIntImpl(dst, value, num_bytes, heks::lower);
        return;
    }
#endif
    ::TargetSpecific::Default::encodeHexIntImpl(dst, value, num_bytes, heks::lower);
}

void encodeHexStringUpper(uint8_t * dst, const uint8_t * src, size_t size)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::encodeHexStringImpl(dst, src, size, heks::upper);
        return;
    }
#endif
    ::TargetSpecific::Default::encodeHexStringImpl(dst, src, size, heks::upper);
}

void encodeHexStringLower(uint8_t * dst, const uint8_t * src, size_t size)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::encodeHexStringImpl(dst, src, size, heks::lower);
        return;
    }
#endif
    ::TargetSpecific::Default::encodeHexStringImpl(dst, src, size, heks::lower);
}

void decodeHexString(uint8_t * dst, const uint8_t * src, size_t size)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::decodeHexStringImpl(dst, src, size);
        return;
    }
#endif
    ::TargetSpecific::Default::decodeHexStringImpl(dst, src, size);
}

void decodeHexStrings(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::decodeHexStringsImpl(dst, src, src_offsets, dst_offsets, row_count);
        return;
    }
#endif
    ::TargetSpecific::Default::decodeHexStringsImpl(dst, src, src_offsets, dst_offsets, row_count);
}

void encodeHex16LEUpper(uint8_t * dst, const uint8_t * src)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::encodeHex16LEImpl(dst, src, heks::upper);
        return;
    }
#endif
    ::TargetSpecific::Default::encodeHex16LEImpl(dst, src, heks::upper);
}

void encodeHex16LELower(uint8_t * dst, const uint8_t * src)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::encodeHex16LEImpl(dst, src, heks::lower);
        return;
    }
#endif
    ::TargetSpecific::Default::encodeHex16LEImpl(dst, src, heks::lower);
}

}

#endif // HEX_GTEST_UNIT_TEST
