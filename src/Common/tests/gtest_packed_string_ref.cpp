#include <array>
#include <bit>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include <base/PackedStringRef.h>
#include <base/defines.h>

#include <gtest/gtest.h>

namespace
{

/// `PackedStringRef::build` may read whole words starting at the string, so give it
/// generous readable slack after the payload (`ColumnString::Chars` guarantees this
/// in production via `PaddedPODArray` right padding).
struct PaddedString
{
    explicit PaddedString(std::string_view s)
    {
        chassert(s.size() <= sizeof(buf) - 16);
        std::memcpy(buf.data(), s.data(), s.size());
        size = s.size();
    }

    const char * data() const { return buf.data(); }

    std::array<char, 64> buf{};
    size_t size = 0;
};

struct FixedHash
{
    uint32_t value;
    uint32_t operator()(const char *, size_t) const { return value; }
};

std::array<uint8_t, 16> rawBytesOf(PackedStringRef ref)
{
    std::array<uint8_t, 16> bytes{};
    std::memcpy(bytes.data(), &ref, sizeof(ref));
    return bytes;
}

/// Reference byte image of the small encoding, built directly from the documented layout.
std::array<uint8_t, 16> smallImage(uint32_t hash, std::string_view s)
{
    std::array<uint8_t, 16> bytes{};
    std::memcpy(bytes.data(), &hash, sizeof(hash));
    std::memcpy(bytes.data() + 4, s.data(), s.size());
    bytes[15] = static_cast<uint8_t>(s.size() << 3);
    return bytes;
}

}

TEST(PackedStringRef, EmptyIsAllZeros)
{
    PackedStringRef ref = PackedStringRef::build(nullptr, 0, FixedHash{0xFFFFFFFF});
    EXPECT_EQ(ref.low, 0u);
    EXPECT_EQ(ref.high, 0u);
    EXPECT_TRUE(ref.isEmpty());
    EXPECT_EQ(static_cast<std::string_view>(ref), std::string_view{});
}

TEST(PackedStringRef, SmallLayout)
{
    if constexpr (std::endian::native != std::endian::little)
        GTEST_SKIP() << "byte image checks assume the little-endian build path";

    const uint32_t hash = 0xAABBCCDD;
    for (size_t len = 1; len <= PackedStringRef::MAX_SMALL_LEN; ++len)
    {
        const std::string s = std::string("abcdefghijk").substr(0, len);
        const PaddedString padded(s);
        PackedStringRef ref = PackedStringRef::build(padded.data(), padded.size, FixedHash{hash});

        EXPECT_TRUE(ref.isSmall()) << "len=" << len;
        EXPECT_EQ(ref.getHash(), hash) << "len=" << len;
        EXPECT_EQ(static_cast<std::string_view>(ref), std::string_view(s)) << "len=" << len;
        EXPECT_EQ(rawBytesOf(ref), smallImage(hash, s)) << "len=" << len;
    }
}

TEST(PackedStringRef, SmallWithEmbeddedAndTrailingZeroBytes)
{
    using namespace std::literals;
    /// Unlike StringHashTable's fixed-size keys, the packed encoding stores an explicit
    /// length, so content zero bytes (including trailing ones) are representable inline.
    for (std::string_view s : {"a\0b"sv, "\0"sv, "ab\0\0"sv, "\0\0\0\0\0\0\0\0\0\0\0"sv})
    {
        const PaddedString padded(s);
        PackedStringRef ref = PackedStringRef::build(padded.data(), padded.size, FixedHash{0x12345678});
        EXPECT_TRUE(ref.isSmall());
        EXPECT_EQ(static_cast<std::string_view>(ref), s);
    }
}

TEST(PackedStringRef, MediumLayout)
{
    const uint32_t hash = 0x11223344;
    for (size_t len : {12ul, 25ul, 48ul, 1000ul})
    {
        const std::string s(len, 'x');
        PackedStringRef ref = PackedStringRef::build(s.data(), s.size(), FixedHash{hash});

        EXPECT_TRUE(ref.isMedium()) << "len=" << len;
        EXPECT_EQ(ref.getHash(), hash) << "len=" << len;
        EXPECT_EQ(ref.getMediumSize(), len) << "len=" << len;
        EXPECT_EQ(ref.getMediumPtr(), s.data()) << "len=" << len;
        EXPECT_EQ(static_cast<std::string_view>(ref), std::string_view(s)) << "len=" << len;
    }
}

TEST(PackedStringRef, LargeLayout)
{
    /// `build` does not read the content or invoke the hash functor for large strings,
    /// so an oversized length with a small real buffer is safe here.
    const char * ptr = reinterpret_cast<const char *>(0x0000123456789ABCull);
    const size_t len = (1ull << 32) + 5;
    PackedStringRef ref = PackedStringRef::build(ptr, len, FixedHash{0xDEAD});

    EXPECT_TRUE(ref.isLarge());
    EXPECT_EQ(ref.getLargeSize(), len);
    EXPECT_EQ(ref.getLargePtr(), ptr);
    /// The low 32 bits of the length act as the hash surrogate.
    EXPECT_EQ(ref.getHash(), static_cast<uint32_t>(len));
}

TEST(PackedStringRef, EqualitySmall)
{
    const PaddedString a1("aa");
    const PaddedString a2("aa");
    const PaddedString b("bb");
    const PaddedString a_longer("aaa");

    PackedStringRef ref_a1 = PackedStringRef::build(a1.data(), a1.size, FixedHash{7});
    PackedStringRef ref_a2 = PackedStringRef::build(a2.data(), a2.size, FixedHash{7});
    PackedStringRef ref_b = PackedStringRef::build(b.data(), b.size, FixedHash{7});
    PackedStringRef ref_a_longer = PackedStringRef::build(a_longer.data(), a_longer.size, FixedHash{7});

    EXPECT_TRUE(ref_a1 == ref_a2);
    /// Same (forced) hash, different payload or length: must compare unequal.
    EXPECT_FALSE(ref_a1 == ref_b);
    EXPECT_FALSE(ref_a1 == ref_a_longer);
    EXPECT_FALSE(ref_a1 == PackedStringRef{});
}

TEST(PackedStringRef, EqualityAcrossSmallBoundary)
{
    const std::string seven(7, 'a');
    const std::string eight(8, 'a');
    const std::string eleven(11, 'a');
    const std::string twelve(12, 'a');

    const PaddedString p7(seven);
    const PaddedString p8(eight);
    const PaddedString p11(eleven);
    const PaddedString p12(twelve);

    PackedStringRef r7 = PackedStringRef::build(p7.data(), p7.size, FixedHash{7});
    PackedStringRef r8 = PackedStringRef::build(p8.data(), p8.size, FixedHash{7});
    PackedStringRef r11 = PackedStringRef::build(p11.data(), p11.size, FixedHash{7});
    PackedStringRef r12 = PackedStringRef::build(p12.data(), p12.size, FixedHash{7});

    EXPECT_FALSE(r7 == r8);
    EXPECT_FALSE(r11 == r12);
    EXPECT_TRUE(r11 == PackedStringRef::build(p11.data(), p11.size, FixedHash{7}));
}

TEST(PackedStringRef, EqualityMedium)
{
    const std::string content(25, 'y');
    /// Deliberately a distinct buffer with the same content, not a reference to `content`.
    const std::string copy(content.data(), content.size());
    const std::string other = std::string(24, 'y') + 'z';

    PackedStringRef ref = PackedStringRef::build(content.data(), content.size(), FixedHash{42});
    PackedStringRef ref_same_buffer = PackedStringRef::build(content.data(), content.size(), FixedHash{42});
    PackedStringRef ref_copy = PackedStringRef::build(copy.data(), copy.size(), FixedHash{42});
    PackedStringRef ref_other = PackedStringRef::build(other.data(), other.size(), FixedHash{42});

    /// Same pointer: decided by the word compare.
    EXPECT_TRUE(ref == ref_same_buffer);
    /// Different pointers, same content: decided by the content comparison.
    EXPECT_TRUE(ref == ref_copy);
    /// Forced hash collision with different content: content comparison must reject.
    EXPECT_FALSE(ref == ref_other);
}

TEST(PackedStringRef, EqualityMediumVersusLargeLowCollision)
{
    /// Craft a medium value and a large value whose `low` words coincide:
    /// medium: hash = 0xDEADBEEF, len = 12 -> low = 0xDEADBEEF | 12 << 32
    /// large: len = (12 << 32) | 0xDEADBEEF -> same low word.
    const std::string content(12, 'q');
    PackedStringRef medium = PackedStringRef::build(content.data(), content.size(), FixedHash{0xDEADBEEF});

    const size_t large_len = (12ull << 32) | 0xDEADBEEFull;
    PackedStringRef large = PackedStringRef::build(content.data(), large_len, FixedHash{0});

    ASSERT_EQ(medium.low, large.low);
    EXPECT_FALSE(medium == large);
    EXPECT_FALSE(large == medium);
}

namespace
{

std::array<uint8_t, 20> rawBytesOf(PackedStringRef20 ref)
{
    std::array<uint8_t, 20> bytes{};
    std::memcpy(bytes.data(), &ref, sizeof(ref));
    return bytes;
}

}

TEST(PackedStringRef20, EmptyIsAllZeros)
{
    const PackedStringRef20 ref = PackedStringRef20::build(nullptr, 0, FixedHash{0xFFFFFFFF});
    EXPECT_EQ(rawBytesOf(ref), (std::array<uint8_t, 20>{}));
    EXPECT_TRUE(ref.isEmpty());
    EXPECT_EQ(ref.heapSize(), 0u);
    EXPECT_EQ(static_cast<std::string_view>(ref), std::string_view{});
}

TEST(PackedStringRef20, SmallLayout)
{
    using namespace std::literals;
    const uint32_t hash = 0xAABBCCDD;
    /// Content zero bytes, trailing ones included, are inline too: the length is explicit.
    std::vector<std::string_view> strings = {"\0"sv, "ab\0\0"sv, std::string_view("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 15)};
    const std::string_view alphabet = "abcdefghijklmno";
    for (size_t len = 1; len <= PackedStringRef20::MAX_SMALL_LEN; ++len)
        strings.push_back(alphabet.substr(0, len));

    for (const std::string_view s : strings)
    {
        const PaddedString padded(s);
        const PackedStringRef20 ref = PackedStringRef20::build(padded.data(), padded.size, FixedHash{hash});

        std::array<uint8_t, 20> image{};
        std::memcpy(image.data(), &hash, sizeof(hash));
        std::memcpy(image.data() + 4, s.data(), s.size());
        image[19] = static_cast<uint8_t>(s.size());

        EXPECT_EQ(rawBytesOf(ref), image) << "len=" << s.size();
        EXPECT_FALSE(ref.isEmpty()) << "len=" << s.size();
        EXPECT_EQ(ref.heapSize(), 0u) << "len=" << s.size();
        EXPECT_EQ(ref.getHash(), hash) << "len=" << s.size();
        EXPECT_EQ(static_cast<std::string_view>(ref), s) << "len=" << s.size();
    }
}

TEST(PackedStringRef20, HeapLayout)
{
    const uint32_t hash = 0x11223344;
    for (size_t len : {16ul, 25ul, 1000ul})
    {
        const std::string s(len, 'x');
        PackedStringRef20 ref = PackedStringRef20::build(s.data(), s.size(), FixedHash{hash});

        EXPECT_FALSE(ref.isEmpty()) << "len=" << len;
        EXPECT_EQ(ref.getHash(), hash) << "len=" << len;
        EXPECT_EQ(ref.heapSize(), len) << "len=" << len;
        EXPECT_EQ(ref.getHeapPtr(), s.data()) << "len=" << len;
        EXPECT_EQ(static_cast<std::string_view>(ref), std::string_view(s)) << "len=" << len;

        const std::string copy(s.data(), s.size());
        ref.setHeapPointer(copy.data());
        EXPECT_EQ(ref.getHeapPtr(), copy.data()) << "len=" << len;
        EXPECT_EQ(ref.heapSize(), len) << "len=" << len;
    }
}

TEST(PackedStringRef20, Equality)
{
    const std::string fifteen(15, 'a');
    const std::string sixteen(16, 'a');
    const PaddedString p15(fifteen);
    const PaddedString p15_other(std::string(14, 'a') + 'b');
    const PaddedString p14(std::string(14, 'a'));

    /// Small strings with the same (forced) hash: equal only with the same bytes and length.
    const PackedStringRef20 r15 = PackedStringRef20::build(p15.data(), p15.size, FixedHash{7});
    EXPECT_TRUE(r15 == PackedStringRef20::build(p15.data(), p15.size, FixedHash{7}));
    EXPECT_FALSE(r15 == PackedStringRef20::build(p15_other.data(), p15_other.size, FixedHash{7}));
    EXPECT_FALSE(r15 == PackedStringRef20::build(p14.data(), p14.size, FixedHash{7}));
    EXPECT_FALSE(r15 == PackedStringRef20::build(sixteen.data(), sixteen.size(), FixedHash{7}));
    EXPECT_FALSE(r15 == PackedStringRef20{});

    /// Heap strings: the same buffer is decided by the words, a copy and a collision by the bytes.
    const std::string content(25, 'y');
    const std::string copy(content.data(), content.size());
    const std::string other = std::string(24, 'y') + 'z';
    const PackedStringRef20 heap = PackedStringRef20::build(content.data(), content.size(), FixedHash{42});
    EXPECT_TRUE(heap == PackedStringRef20::build(content.data(), content.size(), FixedHash{42}));
    EXPECT_TRUE(heap == PackedStringRef20::build(copy.data(), copy.size(), FixedHash{42}));
    EXPECT_FALSE(heap == PackedStringRef20::build(other.data(), other.size(), FixedHash{42}));
    EXPECT_FALSE(heap == PackedStringRef20::build(content.data(), content.size() - 1, FixedHash{42}));
}
