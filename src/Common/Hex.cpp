#include <Common/Hex.h>
#include <Common/TargetSpecific.h>
#include <base/defines.h>
#include <base/extended_types.h>

/// Each ISA header is empty where the compiler can't target that ISA. The AVX2, AVX-512 and SVE functions carry
/// their own target attributes, so they are available whatever the build flags; calling one is only valid in code
/// that requires the ISA, either through the build's baseline or through the runtime dispatch below.
#include <fast_hex/fast_hex.hpp>
#include <fast_hex/fast_hex_arm_neon.hpp>
#include <fast_hex/fast_hex_arm_sve.hpp>
#include <fast_hex/fast_hex_x86_64_icelake.hpp>
#include <fast_hex/fast_hex_x86_64_v4.hpp>


/// `encodeHex16LE*` take the in-memory representation of `CityHash_v1_0_2::uint128`: `low64`, then `high64`, each
/// a native integer. As one 128-bit integer it is encoded `high64` first on any byte order.
static inline unsigned __int128 cityHash128AsInteger(const uint8_t * src)
{
    UInt64 low;
    UInt64 high;
    memcpy(&low, src, 8);
    memcpy(&high, src + 8, 8);
    return static_cast<unsigned __int128>(high) << 64 | low;
}

/// The per-row loop of `decodeHexStrings` around the `decodeHexStringImpl` of one target. Each target's
/// `decodeHexStringsImpl` inlines it, so that `decode` is called under that target's attributes. `decode` is forced
/// inline, and so is the `heks` decoder it calls: by size, clang keeps them out of line, and the call per row then made
/// the AVX-512 loop slower than the AVX2 one for 8-byte rows.
template <auto decode>
ALWAYS_INLINE static inline void
decodeHexStringsLoop(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
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
        [[clang::always_inline]] decode(dst + dst_pos, src + src_prev, raw_size);
        dst_pos += raw_size;
        dst_offsets[i] = dst_pos;
        src_prev = src_end;
    }
}


#if defined(FAST_HEX_NEON)

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
            heks::encodeHexIntNeon(dst, v, c);
            return;
        }
        case 16:
        {
            UInt128 v;
            memcpy(&v, value, 16);
            heks::encodeHexIntNeon(dst, v, c);
            return;
        }
        case 32:
        {
            UInt256 v;
            memcpy(&v, value, 32);
            heks::encodeHexIntNeon(dst, v, c);
            return;
        }
        default:
            UNREACHABLE();
    }
}

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case c)
{
    heks::encodeHexNeon(dst, src, heks::RawLength{size}, c);
}

/// Below 4 bytes `decodeHexNeon` is scalar too, and the table is the faster scalar decoder.
static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    if (size < 4)
        heks::decodeHexLUT4(dst, src, heks::RawLength{size});
    else
        heks::decodeHexNeon(dst, src, heks::RawLength{size});
}

template <typename Case>
static void encodeHex16LEImpl(uint8_t * dst, const uint8_t * src, Case c)
{
    heks::encodeHexIntNeon(dst, cityHash128AsInteger(src), c);
}

) // DECLARE_DEFAULT_CODE

#elif defined(__AVX2__)

/// The baseline of this build already guarantees AVX2 (`-DX86_ARCH_LEVEL=3` or higher), so the
/// default code can use the vector paths directly. This is what keeps the accelerated backend
/// on `-DX86_ARCH_LEVEL=4` builds, where `src/CMakeLists.txt` disables multi-target dispatch and
/// the target-specific code below is therefore not compiled at all.
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
            heks::encodeHexIntAvx2(dst, v, c);
            return;
        }
        case 16:
        {
            UInt128 v;
            memcpy(&v, value, 16);
            heks::encodeHexIntAvx2(dst, v, c);
            return;
        }
        case 32:
        {
            UInt256 v;
            memcpy(&v, value, 32);
            heks::encodeHexIntAvx2(dst, v, c);
            return;
        }
        default:
            UNREACHABLE();
    }
}

template <typename Case>
static void encodeHex16LEImpl(uint8_t * dst, const uint8_t * src, Case c)
{
    heks::encodeHexIntAvx2(dst, cityHash128AsInteger(src), c);
}

) // DECLARE_DEFAULT_CODE

#if defined(__AVX512F__) && defined(__AVX512BW__) && defined(__AVX512CD__) && defined(__AVX512DQ__) && defined(__AVX512VL__)

/// `-DX86_ARCH_LEVEL=4`: the baseline has everything `encodeHexAvx512` and `decodeHexAvx512` need.
DECLARE_DEFAULT_CODE(

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case c)
{
    heks::encodeHexAvx512(dst, src, heks::RawLength{size}, c);
}

static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    if (size < 8)
        heks::decodeHexLUT4(dst, src, heks::RawLength{size});
    else
        [[clang::always_inline]] heks::decodeHexAvx512(dst, src, heks::RawLength{size});
}

) // DECLARE_DEFAULT_CODE

#else

DECLARE_DEFAULT_CODE(

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case c)
{
    heks::encodeHexAvx2(dst, src, heks::RawLength{size}, c);
}

static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    if (size < 8)
        heks::decodeHexLUT4(dst, src, heks::RawLength{size});
    else
        [[clang::always_inline]] heks::decodeHexAvx2(dst, src, heks::RawLength{size});
}

) // DECLARE_DEFAULT_CODE

#endif

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
            heks::encodeHexInt(dst, v, c);
            return;
        }
        case 16:
        {
            UInt128 v;
            memcpy(&v, value, 16);
            heks::encodeHexInt(dst, v, c);
            return;
        }
        case 32:
        {
            UInt256 v;
            memcpy(&v, value, 32);
            heks::encodeHexInt(dst, v, c);
            return;
        }
        default:
            UNREACHABLE();
    }
}

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case c)
{
    heks::encodeHex(dst, src, heks::RawLength{size}, c);
}

/// The compiler vectorizes `decodeHex` for the build's baseline, and the table is faster for short inputs.
static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    if (size < 16)
        heks::decodeHexLUT4(dst, src, heks::RawLength{size});
    else
        heks::decodeHex(dst, src, heks::RawLength{size});
}

template <typename Case>
static void encodeHex16LEImpl(uint8_t * dst, const uint8_t * src, Case c)
{
    heks::encodeHexInt(dst, cityHash128AsInteger(src), c);
}

) // DECLARE_DEFAULT_CODE

#endif

DECLARE_DEFAULT_CODE(

template <bool = true>
static void decodeHexStringsImpl(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
{
    decodeHexStringsLoop<decodeHexStringImpl>(dst, src, src_offsets, dst_offsets, row_count);
}

) // DECLARE_DEFAULT_CODE


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
            heks::encodeHexIntAvx2(dst, v, c);
            return;
        }
        case 16:
        {
            UInt128 v;
            memcpy(&v, value, 16);
            heks::encodeHexIntAvx2(dst, v, c);
            return;
        }
        case 32:
        {
            UInt256 v;
            memcpy(&v, value, 32);
            heks::encodeHexIntAvx2(dst, v, c);
            return;
        }
        default:
            UNREACHABLE();
    }
}

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case c)
{
    heks::encodeHexAvx2(dst, src, heks::RawLength{size}, c);
}

static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    if (size < 8)
        heks::decodeHexLUT4(dst, src, heks::RawLength{size});
    else
        [[clang::always_inline]] heks::decodeHexAvx2(dst, src, heks::RawLength{size});
}

template <typename Case>
static void encodeHex16LEImpl(uint8_t * dst, const uint8_t * src, Case c)
{
    heks::encodeHexIntAvx2(dst, cityHash128AsInteger(src), c);
}

template <bool = true>
static void decodeHexStringsImpl(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
{
    decodeHexStringsLoop<decodeHexStringImpl>(dst, src, src_offsets, dst_offsets, row_count);
}

) // DECLARE_X86_64_V3_SPECIFIC_CODE


/// Only strings: `heks` has no AVX-512 integer encoders, so integers use the `x86_64_v3` code.
DECLARE_X86_64_V4_SPECIFIC_CODE(

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case c)
{
    heks::encodeHexAvx512(dst, src, heks::RawLength{size}, c);
}

static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    if (size < 8)
        heks::decodeHexLUT4(dst, src, heks::RawLength{size});
    else
        [[clang::always_inline]] heks::decodeHexAvx512(dst, src, heks::RawLength{size});
}

template <bool = true>
static void decodeHexStringsImpl(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
{
    decodeHexStringsLoop<decodeHexStringImpl>(dst, src, src_offsets, dst_offsets, row_count);
}

) // DECLARE_X86_64_V4_SPECIFIC_CODE


DECLARE_X86_ICELAKE_SPECIFIC_CODE(

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case c)
{
    heks::encodeHexAvx512Vbmi(dst, src, heks::RawLength{size}, c);
}

static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    if (size < 8)
        heks::decodeHexLUT4(dst, src, heks::RawLength{size});
    else
        [[clang::always_inline]] heks::decodeHexAvx512Vbmi(dst, src, heks::RawLength{size});
}

template <bool = true>
static void decodeHexStringsImpl(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
{
    decodeHexStringsLoop<decodeHexStringImpl>(dst, src, src_offsets, dst_offsets, row_count);
}

) // DECLARE_X86_ICELAKE_SPECIFIC_CODE


/// Only strings: `heks` has no SVE integer encoders, as the NEON ones already take the value in one or two vectors.
/// Up to 32 bytes the SVE functions run the NEON code, so the thresholds are the default code's.
DECLARE_ARM_SVE_SPECIFIC_CODE(

template <typename Case>
static void encodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size, Case c)
{
    heks::encodeHexSve(dst, src, heks::RawLength{size}, c);
}

static void decodeHexStringImpl(uint8_t * dst, const uint8_t * src, size_t size)
{
    if (size < 4)
        heks::decodeHexLUT4(dst, src, heks::RawLength{size});
    else
        [[clang::always_inline]] heks::decodeHexSve(dst, src, heks::RawLength{size});
}

template <bool = true>
static void decodeHexStringsImpl(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
{
    decodeHexStringsLoop<decodeHexStringImpl>(dst, src, src_offsets, dst_offsets, row_count);
}

) // DECLARE_ARM_SVE_SPECIFIC_CODE


#ifndef HEX_GTEST_UNIT_TEST

namespace DB
{

void encodeHexIntUpper(uint8_t * dst, const void * value, size_t num_bytes)
{
#if USE_X86_64_MULTITARGET_CODE
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
#if USE_X86_64_MULTITARGET_CODE
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
#if USE_X86_64_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_icelake))
    {
        ::TargetSpecific::x86_64_icelake::encodeHexStringImpl(dst, src, size, heks::upper);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        ::TargetSpecific::x86_64_v4::encodeHexStringImpl(dst, src, size, heks::upper);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::encodeHexStringImpl(dst, src, size, heks::upper);
        return;
    }
#elif USE_ARM_MULTITARGET_CODE
    if (isArchSupported(TargetArch::ARM_SVE))
    {
        ::TargetSpecific::ARM_SVE::encodeHexStringImpl(dst, src, size, heks::upper);
        return;
    }
#endif
    ::TargetSpecific::Default::encodeHexStringImpl(dst, src, size, heks::upper);
}

void encodeHexStringLower(uint8_t * dst, const uint8_t * src, size_t size)
{
#if USE_X86_64_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_icelake))
    {
        ::TargetSpecific::x86_64_icelake::encodeHexStringImpl(dst, src, size, heks::lower);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        ::TargetSpecific::x86_64_v4::encodeHexStringImpl(dst, src, size, heks::lower);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::encodeHexStringImpl(dst, src, size, heks::lower);
        return;
    }
#elif USE_ARM_MULTITARGET_CODE
    if (isArchSupported(TargetArch::ARM_SVE))
    {
        ::TargetSpecific::ARM_SVE::encodeHexStringImpl(dst, src, size, heks::lower);
        return;
    }
#endif
    ::TargetSpecific::Default::encodeHexStringImpl(dst, src, size, heks::lower);
}

void decodeHexString(uint8_t * dst, const uint8_t * src, size_t size)
{
#if USE_X86_64_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_icelake))
    {
        ::TargetSpecific::x86_64_icelake::decodeHexStringImpl(dst, src, size);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        ::TargetSpecific::x86_64_v4::decodeHexStringImpl(dst, src, size);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::decodeHexStringImpl(dst, src, size);
        return;
    }
#elif USE_ARM_MULTITARGET_CODE
    if (isArchSupported(TargetArch::ARM_SVE))
    {
        ::TargetSpecific::ARM_SVE::decodeHexStringImpl(dst, src, size);
        return;
    }
#endif
    ::TargetSpecific::Default::decodeHexStringImpl(dst, src, size);
}

void decodeHexStrings(uint8_t * dst, const uint8_t * src, const UInt64 * src_offsets, UInt64 * dst_offsets, size_t row_count)
{
#if USE_X86_64_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_icelake))
    {
        ::TargetSpecific::x86_64_icelake::decodeHexStringsImpl(dst, src, src_offsets, dst_offsets, row_count);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        ::TargetSpecific::x86_64_v4::decodeHexStringsImpl(dst, src, src_offsets, dst_offsets, row_count);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        ::TargetSpecific::x86_64_v3::decodeHexStringsImpl(dst, src, src_offsets, dst_offsets, row_count);
        return;
    }
#elif USE_ARM_MULTITARGET_CODE
    if (isArchSupported(TargetArch::ARM_SVE))
    {
        ::TargetSpecific::ARM_SVE::decodeHexStringsImpl(dst, src, src_offsets, dst_offsets, row_count);
        return;
    }
#endif
    ::TargetSpecific::Default::decodeHexStringsImpl(dst, src, src_offsets, dst_offsets, row_count);
}

void encodeHex16LEUpper(uint8_t * dst, const uint8_t * src)
{
#if USE_X86_64_MULTITARGET_CODE
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
#if USE_X86_64_MULTITARGET_CODE
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
