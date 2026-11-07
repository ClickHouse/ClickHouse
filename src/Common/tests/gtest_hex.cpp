#include <Common/Hex.h>
#include <base/Decimal_fwd.h>
#include <base/extended_types.h>
#include <base/scope_guard.h>
#include <base/types.h>
#include <city.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>


using namespace std::string_view_literals;

template <bool Lower, typename T>
static std::string hexer(T val)
{
    std::string res(2 * sizeof(T), ' ');
    Lower ? writeHexUIntLowercase(val, res.data()) : writeHexUIntUppercase(val, res.data());
    return res;
}

TEST(HexTest, HexingIntegral)
{
    ASSERT_EQ("1f", hexer<true>(UInt8{0x1f}));
    ASSERT_EQ("1F", hexer<false>(UInt8{0x1f}));
    ASSERT_EQ("012f", hexer<true>(UInt16{0x012f}));
    ASSERT_EQ("012F", hexer<false>(UInt16{0x012f}));
    ASSERT_EQ("0123456f", hexer<true>(UInt32{0x0123456f}));
    ASSERT_EQ("0123456F", hexer<false>(UInt32{0x0123456F}));
    ASSERT_EQ("0123456789abcdef", hexer<true>(UInt64{0x0123456789abcdef}));
    ASSERT_EQ("0123456789ABCDEF", hexer<false>(UInt64{0x0123456789ABCDEF}));
    UInt128 u128 = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
    ASSERT_EQ("102030405060708090a0b0c0d0e0f000", hexer<true>(u128));
    ASSERT_EQ("102030405060708090A0B0C0D0E0F000", hexer<false>(u128));
    UInt256 u256{u128};
    u256 = (u256 << 128) | (UInt128{0x0011223344556677} << 64) | 0x8899aabbccddeeff;
    ASSERT_EQ("102030405060708090a0b0c0d0e0f00000112233445566778899aabbccddeeff", hexer<true>(u256));
    ASSERT_EQ("102030405060708090A0B0C0D0E0F00000112233445566778899AABBCCDDEEFF", hexer<false>(u256));
}

TEST(HexTest, HexingWideInteger512)
{
    using UInt512 = wide::integer<512, unsigned>;

    /// Build a 512-bit value from four 128-bit chunks.
    UInt128 chunk0 = (UInt128{0x0123456789abcdef} << 64) | 0xfedcba9876543210;
    UInt128 chunk1 = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
    UInt128 chunk2 = (UInt128{0x0011223344556677} << 64) | 0x8899aabbccddeeff;
    UInt128 chunk3 = (UInt128{0xdeadbeefcafebabe} << 64) | 0x1337c0deface1234;

    UInt512 val{chunk0};
    val = (val << 128) | UInt512{chunk1};
    val = (val << 128) | UInt512{chunk2};
    val = (val << 128) | UInt512{chunk3};

    std::string expected_lower =
        "0123456789abcdeffedcba9876543210"
        "102030405060708090a0b0c0d0e0f000"
        "00112233445566778899aabbccddeeff"
        "deadbeefcafebabe1337c0deface1234";

    std::string expected_upper =
        "0123456789ABCDEFFEDCBA9876543210"
        "102030405060708090A0B0C0D0E0F000"
        "00112233445566778899AABBCCDDEEFF"
        "DEADBEEFCAFEBABE1337C0DEFACE1234";

    ASSERT_EQ(expected_lower, hexer<true>(val));
    ASSERT_EQ(expected_upper, hexer<false>(val));

    auto decoded = unhexUInt<UInt512>(expected_lower.data());
    ASSERT_EQ(val, decoded);
    decoded = unhexUInt<UInt512>(expected_upper.data());
    ASSERT_EQ(val, decoded);
}

TEST(HexTest, Hexing)
{
    constexpr auto input = "\x10\x20\x30\x40\x50\x60\x70\x80\x90\xA0\xB0\xC0\xD0\xE0\xF0"sv;
    ASSERT_EQ("102030405060708090a0b0c0d0e0f0", hexString(input.data(), input.size()));
}

template <typename T>
static T unhexer(std::string_view s)
{
    // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
    return unhexUInt<T>(s.data());
}

TEST(HexTest, UnhexingIntegral)
{
    ASSERT_EQ(UInt8{0x1f}, unhexer<UInt8>("1f"));
    ASSERT_EQ(UInt8{0x1f}, unhexer<UInt8>("1F"));

    ASSERT_EQ(UInt16{0x012f}, unhexer<UInt16>("012f"));
    ASSERT_EQ(UInt16{0x012f}, unhexer<UInt16>("012F"));

    ASSERT_EQ(UInt32{0x0123456f}, unhexer<UInt32>("0123456f"));
    ASSERT_EQ(UInt32{0x0123456F}, unhexer<UInt32>("0123456F"));

    ASSERT_EQ(UInt64{0x0123456789abcdef}, unhexer<UInt64>("0123456789abcdef"));
    ASSERT_EQ(UInt64{0x0123456789ABCDEF}, unhexer<UInt64>("0123456789ABCDEF"));

    {
        UInt128 u128 = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
        ASSERT_EQ(u128, unhexer<UInt128>("102030405060708090a0b0c0d0e0f000"));
        ASSERT_EQ(u128, unhexer<UInt128>("102030405060708090A0B0C0D0E0F000"));
    }

    {
        UInt128 hi = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
        UInt128 lo = (UInt128{0x0011223344556677} << 64) | 0x8899aabbccddeeff;
        UInt256 u256{hi};
        u256 = (u256 << 128) | lo;

        ASSERT_EQ(u256, unhexer<UInt256>("102030405060708090a0b0c0d0e0f00000112233445566778899aabbccddeeff"));
        ASSERT_EQ(u256, unhexer<UInt256>("102030405060708090A0B0C0D0E0F00000112233445566778899AABBCCDDEEFF"));
    }
}

/// Nibble-by-nibble reference decoder: an invalid character contributes `unhex(c) == 0xff`.
static UInt64 referenceUnhexUInt64(std::string_view s)
{
    UInt64 res = 0;
    for (char c : s)
        res = (res << 4) + unhex(c);
    return res;
}

/// `unhexUInt` is used by parsers of externally supplied strings (`traceparent` headers, file cache keys,
/// backup checksums) which validate nothing but the length of their input. Decoding a malformed string
/// therefore has to stay deterministic and independent of the architecture and of the SIMD backend.
TEST(HexTest, UnhexingIntegralInvalidInput)
{
    /// 'g' and '/' are not hex digits.
    for (std::string_view s : {"g123456789abcdef"sv, "0123456789abcdeg"sv, "gggggggggggggggg"sv, "0/1/2/3/4/5/6/7/"sv})
    {
        SCOPED_TRACE(s);
        ASSERT_EQ(referenceUnhexUInt64(s), unhexer<UInt64>(s));
    }

    {
        constexpr auto high = "102030405060708g"sv;
        constexpr auto low = "/0a0b0c0d0e0f000"sv;
        const std::string both = std::string{high} + std::string{low};

        UInt128 expected = UInt128{referenceUnhexUInt64(high)} << 64;
        expected += referenceUnhexUInt64(low);
        ASSERT_EQ(expected, unhexer<UInt128>(both));
    }
}

TEST(HexTest, UnhexSingleDigit)
{
    ASSERT_EQ(0, unhex('0'));
    ASSERT_EQ(9, unhex('9'));
    ASSERT_EQ(10, unhex('a'));
    ASSERT_EQ(15, unhex('f'));
    ASSERT_EQ(10, unhex('A'));
    ASSERT_EQ(15, unhex('F'));
}

TEST(HexTest, Unhex2)
{
    ASSERT_EQ(0x00, unhex2("00"));
    ASSERT_EQ(0xFF, unhex2("FF"));
    ASSERT_EQ(0xff, unhex2("ff"));
    ASSERT_EQ(0xAB, unhex2("AB"));
    ASSERT_EQ(0xab, unhex2("ab"));
    ASSERT_EQ(0x1F, unhex2("1F"));
}

TEST(HexTest, Unhex4)
{
    ASSERT_EQ(0x0000, unhex4("0000"));
    ASSERT_EQ(0xFFFF, unhex4("FFFF"));
    ASSERT_EQ(0xffff, unhex4("ffff"));
    ASSERT_EQ(0x1234, unhex4("1234"));
    ASSERT_EQ(0xABCD, unhex4("ABCD"));
    ASSERT_EQ(0xabcd, unhex4("abcd"));
}

TEST(HexTest, HexDigit)
{
    ASSERT_EQ('0', hexDigitUppercase(0));
    ASSERT_EQ('9', hexDigitUppercase(9));
    ASSERT_EQ('A', hexDigitUppercase(10));
    ASSERT_EQ('F', hexDigitUppercase(15));

    ASSERT_EQ('0', hexDigitLowercase(0));
    ASSERT_EQ('9', hexDigitLowercase(9));
    ASSERT_EQ('a', hexDigitLowercase(10));
    ASSERT_EQ('f', hexDigitLowercase(15));
}

TEST(HexTest, WriteHexByte)
{
    char buf[2];
    writeHexByteUppercase(0xAB, buf);
    ASSERT_EQ("AB", std::string_view(buf, 2));

    writeHexByteLowercase(0xAB, buf);
    ASSERT_EQ("ab", std::string_view(buf, 2));

    writeHexByteUppercase(0x00, buf);
    ASSERT_EQ("00", std::string_view(buf, 2));

    writeHexByteLowercase(0xFF, buf);
    ASSERT_EQ("ff", std::string_view(buf, 2));
}

TEST(HexTest, WriteBinByte)
{
    char buf[8];
    writeBinByte(0x00, buf);
    ASSERT_EQ("00000000", std::string_view(buf, 8));

    writeBinByte(0xFF, buf);
    ASSERT_EQ("11111111", std::string_view(buf, 8));

    writeBinByte(0xA5, buf);
    ASSERT_EQ("10100101", std::string_view(buf, 8));
}

TEST(HexTest, HexStringUppercase)
{
    constexpr auto input = "\x10\x20\x30\x40\x50\x60\x70\x80\x90\xA0\xB0\xC0\xD0\xE0\xF0"sv;

    std::string upper(input.size() * 2, '\0');
    hexString<false>(reinterpret_cast<UInt8 *>(upper.data()), input.data(), input.size());
    ASSERT_EQ("102030405060708090A0B0C0D0E0F0", upper);

    std::string lower(input.size() * 2, '\0');
    hexString<true>(reinterpret_cast<UInt8 *>(lower.data()), input.data(), input.size());
    ASSERT_EQ("102030405060708090a0b0c0d0e0f0", lower);
}

TEST(HexTest, HexStringRoundtrip)
{
    /// Exercise scalar path (< 16 bytes) and SIMD paths (>= 16 and >= 32 bytes).
    for (size_t size : {0, 1, 7, 15, 16, 17, 31, 32, 33, 64, 100})
    {
        std::vector<uint8_t> original(size);
        for (size_t i = 0; i < size; ++i)
            original[i] = static_cast<uint8_t>(i * 37 + 13);

        std::string encoded = hexString(original.data(), original.size());
        ASSERT_EQ(encoded.size(), size * 2);

        std::vector<uint8_t> decoded(size);
        unHexString(reinterpret_cast<UInt8 *>(decoded.data()), reinterpret_cast<const UInt8 *>(encoded.data()), size);
        ASSERT_EQ(original, decoded) << "roundtrip failed for size=" << size;
    }
}

TEST(HexTest, GetHexUInt)
{
    ASSERT_EQ("0123456789ABCDEF", getHexUIntUppercase(UInt64{0x0123456789ABCDEF}));
    ASSERT_EQ("0123456789abcdef", getHexUIntLowercase(UInt64{0x0123456789abcdef}));
}

TEST(HexTest, CityHashUInt128Roundtrip)
{
    CityHash_v1_0_2::uint128 original;
    original.high64 = 0x0123456789ABCDEF;
    original.low64  = 0xFEDCBA9876543210;

    /// Encode upper and lower, verify hex string has high64 first.
    auto upper = getHexUIntUppercase(original);
    ASSERT_EQ("0123456789ABCDEFFEDCBA9876543210", upper);

    auto lower = getHexUIntLowercase(original);
    ASSERT_EQ("0123456789abcdeffedcba9876543210", lower);

    /// Roundtrip: unhex the encoded string back and compare.
    auto decoded = unhexUInt<CityHash_v1_0_2::uint128>(upper.data());
    ASSERT_EQ(original.high64, decoded.high64);
    ASSERT_EQ(original.low64, decoded.low64);
}


/// ============================================================
/// Target-specific tests: exercise each architecture path
/// independently, so the scalar path is validated even on
/// machines that support AVX2 (and vice versa).
/// ============================================================

#define HEX_GTEST_UNIT_TEST
#include "Common/Hex.cpp" // NOLINT(bugprone-suspicious-include)

namespace
{

struct DefaultHexTrait
{
    static void skipIfUnsupported() { }

    template <typename Case>
    static void encodeString(uint8_t * dst, const uint8_t * src, size_t size, Case c)
    {
        TargetSpecific::Default::encodeHexStringImpl(dst, src, size, c);
    }

    static void decodeString(uint8_t * dst, const uint8_t * src, size_t size)
    {
        TargetSpecific::Default::decodeHexStringImpl(dst, src, size);
    }

    static void encodeInt(uint8_t * dst, const void * value, size_t num_bytes)
    {
        TargetSpecific::Default::encodeHexIntImpl(dst, value, num_bytes, heks::lower);
    }

    static void encodeHex16LE(uint8_t * dst, const uint8_t * src)
    {
        TargetSpecific::Default::encodeHex16LEImpl(dst, src, heks::lower);
    }
};

#if USE_X86_64_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))

struct X86V3HexTrait
{
    static void skipIfUnsupported()
    {
        if (!DB::isArchSupported(DB::TargetArch::x86_64_v3))
            GTEST_SKIP() << "x86_64_v3 (AVX2) not supported on this host";
    }

    template <typename Case>
    static void encodeString(uint8_t * dst, const uint8_t * src, size_t size, Case c)
    {
        TargetSpecific::x86_64_v3::encodeHexStringImpl(dst, src, size, c);
    }

    static void decodeString(uint8_t * dst, const uint8_t * src, size_t size)
    {
        TargetSpecific::x86_64_v3::decodeHexStringImpl(dst, src, size);
    }

    static void encodeInt(uint8_t * dst, const void * value, size_t num_bytes)
    {
        TargetSpecific::x86_64_v3::encodeHexIntImpl(dst, value, num_bytes, heks::lower);
    }

    static void encodeHex16LE(uint8_t * dst, const uint8_t * src)
    {
        TargetSpecific::x86_64_v3::encodeHex16LEImpl(dst, src, heks::lower);
    }
};

/// The `x86_64_v4` and `x86_64_icelake` targets have only string code: integers use the `x86_64_v3` code there,
/// as in the dispatchers of `Hex.cpp`.
struct X86V4HexTrait : X86V3HexTrait
{
    static void skipIfUnsupported()
    {
        if (!DB::isArchSupported(DB::TargetArch::x86_64_v4))
            GTEST_SKIP() << "x86_64_v4 (AVX-512) not supported on this host";
    }

    template <typename Case>
    static void encodeString(uint8_t * dst, const uint8_t * src, size_t size, Case c)
    {
        TargetSpecific::x86_64_v4::encodeHexStringImpl(dst, src, size, c);
    }

    static void decodeString(uint8_t * dst, const uint8_t * src, size_t size)
    {
        TargetSpecific::x86_64_v4::decodeHexStringImpl(dst, src, size);
    }
};

struct X86IcelakeHexTrait : X86V3HexTrait
{
    static void skipIfUnsupported()
    {
        if (!DB::isArchSupported(DB::TargetArch::x86_64_icelake))
            GTEST_SKIP() << "x86_64_icelake (AVX-512 VBMI) not supported on this host";
    }

    template <typename Case>
    static void encodeString(uint8_t * dst, const uint8_t * src, size_t size, Case c)
    {
        TargetSpecific::x86_64_icelake::encodeHexStringImpl(dst, src, size, c);
    }

    static void decodeString(uint8_t * dst, const uint8_t * src, size_t size)
    {
        TargetSpecific::x86_64_icelake::decodeHexStringImpl(dst, src, size);
    }
};

#endif

#if USE_ARM_MULTITARGET_CODE

struct ArmSveHexTrait : DefaultHexTrait
{
    static void skipIfUnsupported()
    {
        if (!DB::isArchSupported(DB::TargetArch::ARM_SVE))
            GTEST_SKIP() << "ARM_SVE not supported on this host";
    }

    template <typename Case>
    static void encodeString(uint8_t * dst, const uint8_t * src, size_t size, Case c)
    {
        TargetSpecific::ARM_SVE::encodeHexStringImpl(dst, src, size, c);
    }

    static void decodeString(uint8_t * dst, const uint8_t * src, size_t size)
    {
        TargetSpecific::ARM_SVE::decodeHexStringImpl(dst, src, size);
    }
};

#endif

/// Byte-at-a-time reference encoder, independent of `heks`.
std::string referenceHexString(const uint8_t * src, size_t size, bool upper)
{
    const char * digits = upper ? "0123456789ABCDEF" : "0123456789abcdef";
    std::string res;
    res.reserve(size * 2);
    for (size_t i = 0; i < size; ++i)
    {
        res += digits[src[i] >> 4];
        res += digits[src[i] & 0xf];
    }
    return res;
}

} // namespace


/// Encode and decode one buffer size through one implementation and compare it with the byte-at-a-time
/// reference: lower and upper case, plus upper-case at every third digit, which falls on both the high and
/// the low digit of a byte. Each buffer starts one byte into its allocation, so it is unaligned. The inputs
/// are allocated with their exact size, so that ASan catches a read past the end, and the guard bytes after
/// each output must stay untouched.
template <typename Trait>
void checkHexStringRoundtrip(size_t size)
{
    SCOPED_TRACE("size=" + std::to_string(size));

    constexpr size_t guard = 64;
    constexpr uint8_t sentinel = 0xa5;
    auto as_string = [](const uint8_t * p, size_t n) { return std::string(reinterpret_cast<const char *>(p), n); };
    auto guard_intact = [](const uint8_t * end) { return std::all_of(end, end + guard, [](uint8_t b) { return b == sentinel; }); };

    std::vector<uint8_t> original_buf(1 + size);
    const uint8_t * original = original_buf.data() + 1;
    for (size_t i = 0; i < size; ++i)
        original_buf[1 + i] = static_cast<uint8_t>(i * 37 + 13);

    std::vector<uint8_t> lower_buf(1 + size * 2 + guard, sentinel);
    uint8_t * lower = lower_buf.data() + 1;
    Trait::encodeString(lower, original, size, heks::lower);
    ASSERT_EQ(referenceHexString(original, size, false), as_string(lower, size * 2));
    ASSERT_TRUE(guard_intact(lower + size * 2));

    std::vector<uint8_t> upper_buf(1 + size * 2 + guard, sentinel);
    uint8_t * upper = upper_buf.data() + 1;
    Trait::encodeString(upper, original, size, heks::upper);
    ASSERT_EQ(referenceHexString(original, size, true), as_string(upper, size * 2));
    ASSERT_TRUE(guard_intact(upper + size * 2));

    std::string mixed = as_string(lower, size * 2);
    for (size_t i = 0; i < mixed.size(); i += 3)
        mixed[i] = static_cast<char>(upper[i]);

    for (const std::string & encoded : {as_string(lower, size * 2), as_string(upper, size * 2), mixed})
    {
        std::vector<uint8_t> input_buf(1 + size * 2);
        memcpy(input_buf.data() + 1, encoded.data(), size * 2);

        std::vector<uint8_t> decoded_buf(1 + size + guard, sentinel);
        uint8_t * decoded = decoded_buf.data() + 1;
        Trait::decodeString(decoded, input_buf.data() + 1, size);
        ASSERT_EQ(as_string(original, size), as_string(decoded, size)) << "decoding " << encoded;
        ASSERT_TRUE(guard_intact(decoded + size));
    }
}


template <typename T>
class HexMultiArchTest : public ::testing::Test
{
protected:
    void SetUp() override { T::skipIfUnsupported(); }
};

using HexImplementations = ::testing::Types<
    DefaultHexTrait
#if USE_X86_64_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))
    , X86V3HexTrait
    , X86V4HexTrait
    , X86IcelakeHexTrait
#endif
#if USE_ARM_MULTITARGET_CODE
    , ArmSveHexTrait
#endif
    >;

TYPED_TEST_SUITE(HexMultiArchTest, HexImplementations);


TYPED_TEST(HexMultiArchTest, StringEncodeDecodeRoundtrip)
{
    /// Every size up to 200 covers each short-input path, whole 64-byte AVX-512 blocks and the overlapping tails.
    for (size_t size = 0; size <= 200; ++size)
        checkHexStringRoundtrip<TypeParam>(size);
}

TYPED_TEST(HexMultiArchTest, IntegralUInt64)
{
    UInt64 val = 0x0123456789ABCDEF;
    uint8_t hex_buf[16];
    TypeParam::encodeInt(hex_buf, &val, 8);
    ASSERT_EQ("0123456789abcdef", std::string_view(reinterpret_cast<const char *>(hex_buf), 16));

    /// `unhexUInt` is architecture-independent by design, so decoding is not part of the per-arch traits.
    ASSERT_EQ(val, unhexUInt<UInt64>(reinterpret_cast<const char *>(hex_buf)));
}

TYPED_TEST(HexMultiArchTest, IntegralUInt128)
{
    UInt128 val = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
    uint8_t hex_buf[32];
    TypeParam::encodeInt(hex_buf, &val, 16);
    ASSERT_EQ("102030405060708090a0b0c0d0e0f000", std::string_view(reinterpret_cast<const char *>(hex_buf), 32));
}

TYPED_TEST(HexMultiArchTest, IntegralUInt256)
{
    UInt128 hi = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
    UInt256 val{hi};
    val = (val << 128) | ((UInt128{0x0011223344556677} << 64) | 0x8899aabbccddeeff);
    uint8_t hex_buf[64];
    TypeParam::encodeInt(hex_buf, &val, 32);
    ASSERT_EQ(
        "102030405060708090a0b0c0d0e0f00000112233445566778899aabbccddeeff",
        std::string_view(reinterpret_cast<const char *>(hex_buf), 64));
}

TYPED_TEST(HexMultiArchTest, EncodeHex16LE)
{
    /// CityHash uint128 layout: low64 at bytes [0..7], high64 at bytes [8..15].
    uint8_t input[16];
    UInt64 low = 0xFEDCBA9876543210;
    UInt64 high = 0x0123456789ABCDEF;
    memcpy(input, &low, 8);
    memcpy(input + 8, &high, 8);

    uint8_t hex_buf[32];
    TypeParam::encodeHex16LE(hex_buf, input);

    /// Output should be hex(high64) followed by hex(low64), both MSB-first.
    ASSERT_EQ("0123456789abcdeffedcba9876543210", std::string_view(reinterpret_cast<const char *>(hex_buf), 32));
}


/// SVE instructions produce the same result at every vector length, but the number of loop iterations
/// and the tails depend on the length the thread runs with. Run the `ARM_SVE` code at every vector
/// length the hardware supports and compare it against the byte-at-a-time reference, so a bug in the
/// VL-dependent part cannot hide behind the one vector length the host happened to boot with.
#if USE_ARM_MULTITARGET_CODE && defined(__linux__)

#    include <sys/prctl.h>

TEST(HexSveVectorLengths, AllVectorLengths)
{
    ArmSveHexTrait::skipIfUnsupported();

    /// `prctl` takes and returns the vector length in bytes. The scope guard restores the original
    /// length even when an assertion below fails.
    const int original_vl = prctl(PR_SVE_GET_VL);
    ASSERT_GT(original_vl, 0);
    SCOPE_EXIT({ [[maybe_unused]] int restore_vl = prctl(PR_SVE_SET_VL, original_vl); });

    size_t previous_vl = 0;
    for (int requested_vl = 16; requested_vl <= 256; requested_vl += 16) /// 128 to 2048 bits
    {
        errno = 0;
        const int vl = prctl(PR_SVE_SET_VL, requested_vl);
        const int saved_errno = errno;
        ASSERT_GT(vl, 0) << "prctl(PR_SVE_SET_VL) failed: " << strerror(saved_errno);
        const size_t vl_bytes = static_cast<size_t>(vl & PR_SVE_VL_LEN_MASK);
        if (vl_bytes == previous_vl)
            continue; /// The hardware caps the request at its maximum vector length.
        previous_vl = vl_bytes;

        SCOPED_TRACE("SVE vector length = " + std::to_string(vl_bytes * 8) + " bits");

        /// Sizes around the boundaries of the SVE loop, which starts at 32 bytes and steps by the
        /// vector length, plus the short-input paths below it.
        for (size_t size : {0, 1, 2, 3, 4, 7, 15, 16, 31, 32, 33, 63, 64, 65})
            checkHexStringRoundtrip<ArmSveHexTrait>(size);
        for (size_t k : {1, 2})
            for (size_t delta : {vl_bytes * k + 31, vl_bytes * k + 32, vl_bytes * k + 33})
                checkHexStringRoundtrip<ArmSveHexTrait>(delta);
    }
}

#endif
