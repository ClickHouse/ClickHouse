#include <algorithm>
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

template <typename Ref>
std::array<uint8_t, 16> rawBytesOf(Ref ref)
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

/// Fills the bytes past the end of the string with garbage: `buildShort` must zero them.
struct GarbagePaddedString
{
    explicit GarbagePaddedString(std::string_view s)
    {
        chassert(s.size() <= sizeof(buf) - 16);
        buf.fill('#');
        std::memcpy(buf.data(), s.data(), s.size());
        size = s.size();
    }

    const char * data() const { return buf.data(); }

    std::array<char, 64> buf{};
    size_t size = 0;
};

std::array<uint8_t, 16> unhashedSmallImage(std::string_view s)
{
    std::array<uint8_t, 16> bytes{};
    std::memcpy(bytes.data(), s.data(), s.size());
    bytes[15] = static_cast<uint8_t>(s.size() << 3);
    return bytes;
}

UnhashedPackedStringRef buildUnhashed(std::string_view s)
{
    const GarbagePaddedString padded(s);
    return UnhashedPackedStringRef::build(padded.data(), padded.size);
}

template <typename Ref>
concept HasHash = requires(Ref ref) { ref.getHash(); };

template <typename Ref>
concept HasMediumEncoding = requires(Ref ref) { ref.isMedium(); };

/// The unhashed layout has no hash and no medium encoding.
static_assert(HasHash<PackedStringRef> && HasMediumEncoding<PackedStringRef>);
static_assert(!HasHash<UnhashedPackedStringRef> && !HasMediumEncoding<UnhashedPackedStringRef>);

static_assert(sizeof(PackedStringRef) == 16 && alignof(PackedStringRef) == 8);
static_assert(sizeof(UnhashedPackedStringRef) == 16 && alignof(UnhashedPackedStringRef) == 4);

}

TEST(UnhashedPackedStringRef, EmptyIsAllZeros)
{
    UnhashedPackedStringRef ref = buildUnhashed("");
    EXPECT_EQ(ref.low, 0u);
    EXPECT_EQ(ref.high, 0u);
    EXPECT_TRUE(ref.isEmpty());
    EXPECT_EQ(ref.size(), 0u);
    EXPECT_EQ(static_cast<std::string_view>(ref), std::string_view{});
}

TEST(UnhashedPackedStringRef, SmallLayout)
{
    static_assert(UnhashedPackedStringRef::MAX_SMALL_LEN == 15);

    const std::string alphabet = "abcdefghijklmno";
    for (size_t len = 1; len <= UnhashedPackedStringRef::MAX_SMALL_LEN; ++len)
    {
        const std::string s = alphabet.substr(0, len);
        UnhashedPackedStringRef ref = buildUnhashed(s);

        EXPECT_TRUE(ref.isSmall()) << "len=" << len;
        EXPECT_FALSE(ref.isLarge()) << "len=" << len;
        EXPECT_EQ(ref.getSmallSize(), len) << "len=" << len;
        EXPECT_EQ(static_cast<std::string_view>(ref), std::string_view(s)) << "len=" << len;
        EXPECT_EQ(rawBytesOf(ref), unhashedSmallImage(s)) << "len=" << len;
    }
}

TEST(UnhashedPackedStringRef, ZeroBytesAreContent)
{
    using namespace std::literals;
    /// The length is in the tag byte, so a trailing zero byte makes a different value.
    const std::array<std::string_view, 8> strings
        = {"a"sv, "a\0"sv, "a\0\0"sv, "\0"sv, "\0\0"sv, "abcdefghijklmn"sv, "abcdefghijklmn\0"sv, "abcdefghijklmnX"sv};

    for (size_t i = 0; i < strings.size(); ++i)
    {
        UnhashedPackedStringRef ref = buildUnhashed(strings[i]);
        EXPECT_TRUE(ref.isSmall()) << "i=" << i;
        EXPECT_EQ(static_cast<std::string_view>(ref), strings[i]) << "i=" << i;
        EXPECT_EQ(rawBytesOf(ref), unhashedSmallImage(strings[i])) << "i=" << i;

        for (size_t j = 0; j < i; ++j)
            EXPECT_NE(rawBytesOf(ref), rawBytesOf(buildUnhashed(strings[j]))) << "i=" << i << " j=" << j;
    }
}

TEST(UnhashedPackedStringRef, AllOnesSmallStringIsNotLarge)
{
    const std::string s(UnhashedPackedStringRef::MAX_SMALL_LEN, '\xFF');
    UnhashedPackedStringRef ref = buildUnhashed(s);

    EXPECT_TRUE(ref.isSmall());
    EXPECT_FALSE(ref.isLarge());
    EXPECT_EQ(ref.getSmallSize(), s.size());
    EXPECT_EQ(static_cast<std::string_view>(ref), std::string_view(s));
    EXPECT_EQ(rawBytesOf(ref), unhashedSmallImage(s));
}

TEST(UnhashedPackedStringRef, LargeLayout)
{
    for (size_t len : {16ul, 17ul, 100ul, 100000ul})
    {
        const std::string s(len, 'x');
        UnhashedPackedStringRef ref = UnhashedPackedStringRef::buildLong(s.data(), s.size());

        EXPECT_TRUE(ref.isLarge()) << "len=" << len;
        EXPECT_FALSE(ref.isSmall()) << "len=" << len;
        EXPECT_EQ(ref.getLargeSize(), len) << "len=" << len;
        EXPECT_EQ(ref.getLargePtr(), s.data()) << "len=" << len;
        EXPECT_EQ(ref.heapSize(), len) << "len=" << len;
        EXPECT_EQ(ref.getHeapPtr(), s.data()) << "len=" << len;
        EXPECT_EQ(ref.size(), len) << "len=" << len;
        EXPECT_EQ(static_cast<std::string_view>(ref), std::string_view(s)) << "len=" << len;
    }

    /// `buildLong` does not read the content of the string.
    const char * ptr = reinterpret_cast<const char *>(0x0000123456789ABCull);
    const size_t len = (1ull << 32) + 5;
    UnhashedPackedStringRef ref = UnhashedPackedStringRef::buildLong(ptr, len);
    EXPECT_TRUE(ref.isLarge());
    EXPECT_EQ(ref.getLargeSize(), len);
    EXPECT_EQ(ref.getLargePtr(), ptr);
}

TEST(UnhashedPackedStringRef, EqualityOfRefs)
{
    /// Small refs are equal as bytes if and only if their strings are equal.
    EXPECT_EQ(rawBytesOf(buildUnhashed("abc")), rawBytesOf(buildUnhashed("abc")));
    EXPECT_NE(rawBytesOf(buildUnhashed("abc")), rawBytesOf(buildUnhashed("abd")));

    /// A large ref holds the address of its string, so equal strings at different addresses differ.
    const std::string first(20, 'x');
    const std::string second(20, 'x');
    EXPECT_EQ(rawBytesOf(UnhashedPackedStringRef::buildLong(first.data(), first.size())),
        rawBytesOf(UnhashedPackedStringRef::buildLong(first.data(), first.size())));
    EXPECT_NE(rawBytesOf(UnhashedPackedStringRef::buildLong(first.data(), first.size())),
        rawBytesOf(UnhashedPackedStringRef::buildLong(second.data(), second.size())));
}

TEST(UnhashedPackedStringRef, SizeAndCopyTo)
{
    for (size_t len : {0ul, 1ul, 7ul, 15ul, 16ul, 40ul})
    {
        /// The buffers are as small as the contract allows, so that AddressSanitizer reports any wider access.
        /// A small string needs 16 bytes at both ends, the empty string too.
        const std::vector<char> src(std::max<size_t>(len, 16), 'q');
        const UnhashedPackedStringRef ref = UnhashedPackedStringRef::build(src.data(), len);
        EXPECT_EQ(ref.size(), len) << "len=" << len;

        std::vector<char8_t> dst(std::max<size_t>(len, 16));
        const char8_t * end = ref.copyTo(dst.data());
        EXPECT_EQ(static_cast<size_t>(end - dst.data()), len) << "len=" << len;
        EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(dst.data()), len), std::string_view(src.data(), len)) << "len=" << len;
    }
}

TEST(UnhashedPackedStringRef, FourByteAligned)
{
    /// A row holds the value at an offset that is a multiple of 4 and not of 8.
    alignas(8) std::array<char, 40> row{};
    const UnhashedPackedStringRef ref = buildUnhashed("abc");
    std::memcpy(row.data() + 4, &ref, sizeof(ref));

    const auto * stored = reinterpret_cast<const UnhashedPackedStringRef *>(row.data() + 4);
    EXPECT_EQ(stored->size(), 3u);
    EXPECT_EQ(static_cast<std::string_view>(*stored), std::string_view("abc"));
}

TEST(UnhashedPackedStringRef, BuildChoosesTheLayoutByLength)
{
    const GarbagePaddedString small("abcdefghijklmno");
    EXPECT_EQ(
        rawBytesOf(UnhashedPackedStringRef::build(small.data(), small.size)),
        rawBytesOf(UnhashedPackedStringRef::buildShort(small.data(), small.size)));

    const std::string large(16, 'x');
    EXPECT_EQ(
        rawBytesOf(UnhashedPackedStringRef::build(large.data(), large.size())),
        rawBytesOf(UnhashedPackedStringRef::buildLong(large.data(), large.size())));
}

TEST(UnhashedPackedStringRef, Equality)
{
    const std::string first = std::string(20, 'x');
    const std::string same_content = std::string(20, 'x');
    const std::string other_content = std::string(19, 'x') + "y";
    const std::string longer = std::string(21, 'x');

    auto large = [](const std::string & s) { return UnhashedPackedStringRef::buildLong(s.data(), s.size()); };

    EXPECT_TRUE(buildUnhashed("") == buildUnhashed(""));
    EXPECT_TRUE(buildUnhashed("abc") == buildUnhashed("abc"));
    EXPECT_FALSE(buildUnhashed("abc") == buildUnhashed("abd"));
    EXPECT_FALSE(buildUnhashed("abc") == buildUnhashed(std::string_view("abc\0", 4)));
    EXPECT_FALSE(buildUnhashed("") == buildUnhashed("a"));

    EXPECT_TRUE(large(first) == large(first));
    /// The same bytes at another address.
    EXPECT_TRUE(large(first) == large(same_content));
    EXPECT_FALSE(large(first) == large(other_content));
    EXPECT_FALSE(large(first) == large(longer));

    EXPECT_FALSE(buildUnhashed("abc") == large(first));
    EXPECT_FALSE(large(first) == buildUnhashed(std::string(15, 'x')));
}

TEST(UnhashedPackedStringRef, HeapPointer)
{
    EXPECT_EQ(buildUnhashed("abc").heapSize(), 0u);
    EXPECT_EQ(buildUnhashed("").heapSize(), 0u);

    const std::string original(40, 'q');
    const std::string copy(original.data(), original.size());
    UnhashedPackedStringRef ref = UnhashedPackedStringRef::buildLong(original.data(), original.size());
    ASSERT_EQ(ref.getHeapPtr(), original.data());

    /// Rebinding to a copy keeps the length and the tag, and the value still equals the one that viewed the original.
    const UnhashedPackedStringRef before = ref;
    ref.setHeapPointer(copy.data());
    EXPECT_EQ(ref.getHeapPtr(), copy.data());
    EXPECT_EQ(ref.getLargePtr(), copy.data());
    EXPECT_EQ(ref.heapSize(), original.size());
    EXPECT_TRUE(ref.isLarge());
    EXPECT_TRUE(ref == before);
}

TEST(UnhashedPackedStringRef, StringView)
{
    EXPECT_EQ(static_cast<std::string_view>(buildUnhashed("")), std::string_view{});

    /// A small string is viewed in the value, and a large one where it is.
    const UnhashedPackedStringRef small = buildUnhashed("abc");
    const std::string_view small_view = static_cast<std::string_view>(small);
    EXPECT_EQ(small_view, std::string_view("abc"));
    EXPECT_EQ(small_view.data(), reinterpret_cast<const char *>(&small));

    const std::string large_string(30, 'z');
    const UnhashedPackedStringRef large = UnhashedPackedStringRef::buildLong(large_string.data(), large_string.size());
    EXPECT_EQ(static_cast<std::string_view>(large).data(), large_string.data());
    EXPECT_EQ(static_cast<std::string_view>(large).size(), large_string.size());
}
