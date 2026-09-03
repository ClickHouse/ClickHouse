#include "config.h"

#if USE_SSL

#include <gtest/gtest.h>

#include <cstring>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <openssl/evp.h>

#define MD5_GTEST_UNIT_TEST
#include "Functions/FunctionMD5.cpp" // NOLINT(bugprone-suspicious-include)

namespace
{

using StrChars = DB::ColumnString::Chars;
using StrOffsets = DB::ColumnString::Offsets;
using FixedChars = DB::ColumnFixedString::Chars;

std::string digestToHex(const uint8_t * digest)
{
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (int i = 0; i < 16; ++i)
        oss << std::setw(2) << static_cast<unsigned>(digest[i]);
    return oss.str();
}

std::string referenceMD5Hex(const uint8_t * data, size_t len)
{
    unsigned char digest[16];
    EVP_MD_CTX * ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_md5(), nullptr);
    EVP_DigestUpdate(ctx, data, len);
    unsigned int md_len = 0;
    EVP_DigestFinal_ex(ctx, digest, &md_len);
    EVP_MD_CTX_free(ctx);
    return digestToHex(digest);
}

std::string referenceMD5Hex(const std::string & s)
{
    return referenceMD5Hex(reinterpret_cast<const uint8_t *>(s.data()), s.size());
}


/// RFC 1321 test vectors.
struct MD5TestVector
{
    std::string input;
    std::string expected_hex;
};

const std::vector<MD5TestVector> rfc1321_vectors = {
    {"", "d41d8cd98f00b204e9800998ecf8427e"},
    {"a", "0cc175b9c0f1b6a831c399e269772661"},
    {"abc", "900150983cd24fb0d6963f7d28e17f72"},
    {"message digest", "f96b697d7cb7938d525a2f31aaf161d0"},
    {"abcdefghijklmnopqrstuvwxyz", "c3fcd3d76192e4007dfb496cca67e13b"},
    {"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", "d174ab98d277d9f5a5611c2c9f419d9f"},
    {"12345678901234567890123456789012345678901234567890123456789012345678901234567890", "57edf4a22be3c955ac49da2e2107b67a"},
};


// ============================================================
// Helper function tests
// ============================================================

TEST(MD5Helpers, NumMD5Blocks)
{
    EXPECT_EQ(numMD5Blocks(0), 1u);
    EXPECT_EQ(numMD5Blocks(1), 1u);
    EXPECT_EQ(numMD5Blocks(55), 1u);
    EXPECT_EQ(numMD5Blocks(56), 2u);
    EXPECT_EQ(numMD5Blocks(63), 2u);
    EXPECT_EQ(numMD5Blocks(64), 2u);
    EXPECT_EQ(numMD5Blocks(119), 2u);
    EXPECT_EQ(numMD5Blocks(120), 3u);
    EXPECT_EQ(numMD5Blocks(128), 3u);
}

TEST(MD5Helpers, PadFinalBlocks)
{
    {
        /// Empty input: 1 final block, 0x80 at offset 0, bit-length = 0 in last 8 bytes.
        alignas(64) uint8_t buf[128] = {};
        size_t count = md5PadFinalBlocks(reinterpret_cast<const uint8_t *>(""), 0, buf);
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(buf[0], 0x80);
        for (int i = 1; i < 56; ++i)
            EXPECT_EQ(buf[i], 0) << "byte " << i;
        uint64_t stored_len = 0;
        std::memcpy(&stored_len, buf + 56, 8);
        EXPECT_EQ(stored_len, 0u);
    }
    {
        /// 55 bytes: fits in 1 final block (55 data + 0x80 + 0-padding + 8 length = 64).
        std::string input(55, 'x');
        alignas(64) uint8_t buf[128] = {};
        size_t count = md5PadFinalBlocks(reinterpret_cast<const uint8_t *>(input.data()), 55, buf);
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(buf[55], 0x80);
    }
    {
        /// 56 bytes: needs 2 final blocks.
        std::string input(56, 'y');
        alignas(64) uint8_t buf[128] = {};
        size_t count = md5PadFinalBlocks(reinterpret_cast<const uint8_t *>(input.data()), 56, buf);
        EXPECT_EQ(count, 2u);
        EXPECT_EQ(buf[56], 0x80);
    }
}


// ============================================================
// Trait structs: pair Ops with the correct namespace
// ============================================================

struct ScalarMD5Trait
{
    using Ops = DB::TargetSpecific::Default::ScalarMD5Ops;
    static constexpr size_t lanes = Ops::lanes;

    static void skipIfUnsupported() { }

    static void compute(const uint8_t * const inputs[], const size_t lengths[], uint8_t * output, size_t actual_count)
    {
        DB::TargetSpecific::Default::md5MultiBufCompute<Ops>(inputs, lengths, output, actual_count);
    }

    static void computeColumn(const StrChars & data, const StrOffsets & offsets, FixedChars & chars_to, size_t rows)
    {
        DB::TargetSpecific::Default::md5BatchColumnString<Ops>(data, offsets, chars_to, rows);
    }
};

#if defined(__AVX2__)

struct AVX2MD5Trait
{
    using Ops = DB::TargetSpecific::Default::AVX2MD5Ops;
    static constexpr size_t lanes = Ops::lanes;

    static void skipIfUnsupported() { }

    static void compute(const uint8_t * const inputs[], const size_t lengths[], uint8_t * output, size_t actual_count)
    {
        DB::TargetSpecific::Default::md5MultiBufCompute<Ops>(inputs, lengths, output, actual_count);
    }

    static void computeColumn(const StrChars & data, const StrOffsets & offsets, FixedChars & chars_to, size_t rows)
    {
        DB::TargetSpecific::Default::md5BatchColumnString<Ops>(data, offsets, chars_to, rows);
    }
};

#endif

#if USE_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))

struct AVX512MD5Trait
{
    using Ops = DB::TargetSpecific::x86_64_v4::AVX512MD5Ops;
    static constexpr size_t lanes = Ops::lanes;

    static void skipIfUnsupported()
    {
        if (!DB::isArchSupported(DB::TargetArch::x86_64_v4))
            GTEST_SKIP() << "x86_64_v4 (AVX-512) not supported on this host";
    }

    static void compute(const uint8_t * const inputs[], const size_t lengths[], uint8_t * output, size_t actual_count)
    {
        DB::TargetSpecific::x86_64_v4::md5MultiBufCompute<Ops>(inputs, lengths, output, actual_count);
    }

    static void computeColumn(const StrChars & data, const StrOffsets & offsets, FixedChars & chars_to, size_t rows)
    {
        DB::TargetSpecific::x86_64_v4::md5BatchColumnString<Ops>(data, offsets, chars_to, rows);
    }
};

#endif

#if USE_MD5_AARCH64_ASIMD

struct ASIMDMD5Trait
{
    using Ops = DB::TargetSpecific::Default::ASIMDMD5Ops;
    static constexpr size_t lanes = Ops::lanes;

    static void skipIfUnsupported() { }

    static void compute(const uint8_t * const inputs[], const size_t lengths[], uint8_t * output, size_t actual_count)
    {
        DB::TargetSpecific::Default::md5MultiBufCompute<Ops>(inputs, lengths, output, actual_count);
    }

    static void computeColumn(const StrChars & data, const StrOffsets & offsets, FixedChars & chars_to, size_t rows)
    {
        DB::TargetSpecific::Default::md5BatchColumnString<Ops>(data, offsets, chars_to, rows);
    }
};

#endif


// ============================================================
// Typed test suite
// ============================================================

template <typename T>
class MD5MultiBufTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        T::skipIfUnsupported();
    }
};

using MD5Implementations = ::testing::Types<
    ScalarMD5Trait
#if defined(__AVX2__)
    ,
    AVX2MD5Trait
#endif
#if USE_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))
    ,
    AVX512MD5Trait
#endif
#if USE_MD5_AARCH64_ASIMD
    ,
    ASIMDMD5Trait
#endif
    >;

TYPED_TEST_SUITE(MD5MultiBufTest, MD5Implementations);


// ============================================================
// Helpers: compute one or a batch of MD5 digests
// ============================================================

template <typename Trait>
std::string computeOneMD5(const std::string & input)
{
    constexpr size_t N2 = 2 * Trait::lanes;

    const uint8_t * inputs[N2];
    size_t lengths[N2];

    inputs[0] = reinterpret_cast<const uint8_t *>(input.data());
    lengths[0] = input.size();

    for (size_t j = 1; j < N2; ++j)
    {
        inputs[j] = &md5_dummy_lane_byte;
        lengths[j] = 0;
    }

    alignas(64) uint8_t output[N2 * 16];
    std::memset(output, 0xCC, sizeof(output));

    Trait::compute(inputs, lengths, output, 1);
    return digestToHex(output);
}

template <typename Trait>
std::vector<std::string> computeBatchMD5(const std::vector<std::string> & batch_inputs)
{
    constexpr size_t N2 = 2 * Trait::lanes;
    size_t actual_count = batch_inputs.size();
    EXPECT_LE(actual_count, N2);

    const uint8_t * inputs[N2];
    size_t lengths[N2];

    for (size_t j = 0; j < actual_count; ++j)
    {
        inputs[j] = reinterpret_cast<const uint8_t *>(batch_inputs[j].data());
        lengths[j] = batch_inputs[j].size();
    }
    for (size_t j = actual_count; j < N2; ++j)
    {
        inputs[j] = &md5_dummy_lane_byte;
        lengths[j] = 0;
    }

    alignas(64) uint8_t output[N2 * 16];
    std::memset(output, 0xCC, sizeof(output));

    Trait::compute(inputs, lengths, output, actual_count);

    std::vector<std::string> results;
    results.reserve(actual_count);
    for (size_t j = 0; j < actual_count; ++j)
        results.push_back(digestToHex(output + j * 16));
    return results;
}


// ============================================================
// Test cases
// ============================================================

TYPED_TEST(MD5MultiBufTest, RFC1321Vectors)
{
    for (const auto & tv : rfc1321_vectors)
    {
        SCOPED_TRACE(tv.input);
        EXPECT_EQ(tv.expected_hex, computeOneMD5<TypeParam>(tv.input));
    }
}

TYPED_TEST(MD5MultiBufTest, SingleInput)
{
    std::string input = "The quick brown fox jumps over the lazy dog";
    std::string expected = referenceMD5Hex(input);
    EXPECT_EQ(expected, computeOneMD5<TypeParam>(input));
}

TYPED_TEST(MD5MultiBufTest, FullBatch)
{
    constexpr size_t N2 = 2 * TypeParam::lanes;
    std::vector<std::string> inputs;
    inputs.reserve(N2);
    for (size_t i = 0; i < N2; ++i)
        inputs.push_back("input_" + std::to_string(i) + "_" + std::string(i * 3, 'A'));

    auto results = computeBatchMD5<TypeParam>(inputs);
    ASSERT_EQ(results.size(), N2);

    for (size_t i = 0; i < N2; ++i)
    {
        SCOPED_TRACE("batch index " + std::to_string(i) + ": \"" + inputs[i] + "\"");
        EXPECT_EQ(referenceMD5Hex(inputs[i]), results[i]);
    }
}

TYPED_TEST(MD5MultiBufTest, VaryingLengthBatch)
{
    /// Create inputs of specific lengths that exercise different code paths.
    std::vector<size_t> target_lengths = {0, 1, 14, 55, 56, 63, 64, 80};
    constexpr size_t N2 = 2 * TypeParam::lanes;

    /// Use as many as fit in one batch.
    size_t count = std::min(target_lengths.size(), N2);
    std::vector<std::string> inputs;
    inputs.reserve(count);
    for (size_t i = 0; i < count; ++i)
        inputs.push_back(std::string(target_lengths[i], static_cast<char>('a' + (i % 26))));

    auto results = computeBatchMD5<TypeParam>(inputs);
    ASSERT_EQ(results.size(), count);

    for (size_t i = 0; i < count; ++i)
    {
        SCOPED_TRACE("length " + std::to_string(inputs[i].size()));
        EXPECT_EQ(referenceMD5Hex(inputs[i]), results[i]);
    }
}

TYPED_TEST(MD5MultiBufTest, BlockBoundaries)
{
    /// Test inputs at MD5 padding boundary lengths.
    std::vector<size_t> boundary_lengths = {55, 56, 63, 64, 119, 120, 127, 128};

    for (size_t len : boundary_lengths)
    {
        SCOPED_TRACE("length " + std::to_string(len));
        std::string input(len, 'Z');
        EXPECT_EQ(referenceMD5Hex(input), computeOneMD5<TypeParam>(input));
    }
}

TYPED_TEST(MD5MultiBufTest, MultiBlockLongStrings)
{
    for (size_t len : {128, 500, 1000, 4096})
    {
        SCOPED_TRACE("length " + std::to_string(len));
        std::string input(len, 'Q');
        EXPECT_EQ(referenceMD5Hex(input), computeOneMD5<TypeParam>(input));
    }
}

TYPED_TEST(MD5MultiBufTest, StressRandom)
{
    constexpr size_t N2 = 2 * TypeParam::lanes;
    std::random_device rd;
    auto seed = rd();
    std::cerr << "StressRandom seed: " << seed << std::endl;
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<size_t> len_dist(0, 1024);
    std::uniform_int_distribution<int> byte_dist(0, 255);

    constexpr size_t iterations = 200;

    for (size_t iter = 0; iter < iterations; ++iter)
    {
        size_t batch_size = (rng() % N2) + 1;
        std::vector<std::string> inputs;
        inputs.reserve(batch_size);
        for (size_t j = 0; j < batch_size; ++j)
        {
            size_t len = len_dist(rng);
            std::string s(len, '\0');
            for (size_t k = 0; k < len; ++k)
                s[k] = static_cast<char>(byte_dist(rng));
            inputs.push_back(std::move(s));
        }

        auto results = computeBatchMD5<TypeParam>(inputs);
        ASSERT_EQ(results.size(), batch_size);

        for (size_t j = 0; j < batch_size; ++j)
        {
            std::string expected = referenceMD5Hex(inputs[j]);
            if (expected != results[j])
            {
                std::ostringstream oss;
                oss << "\nStress test failed"
                    << "\n  iter=" << iter << " j=" << j << " batch_size=" << batch_size << " len=" << inputs[j].size()
                    << "\n  input(hex): ";
                oss << std::hex << std::setfill('0');
                for (unsigned char c : inputs[j])
                    oss << std::setw(2) << static_cast<unsigned>(c);
                oss << "\n  expected: " << expected << "\n  got:      " << results[j];
                FAIL() << oss.str();
            }
        }
    }
}


// ============================================================
// Driver-level test. The driver groups rows by block count inside a window, and a server run reaches
// only the one Ops width runtime dispatch picks; a gtest reaches every width this build compiles.
// ============================================================

enum class ColumnShape
{
    Spread, /// wide block-count spread: profitable in every window
    Flat, /// constant length: the column-level gate declines
    Mixed, /// spread first half, constant second half: windows decide differently
    Periodic, /// one long row per window, all at the same offset: no window can gain
    AboveCapHalf, /// one-block first half, above-the-cap second half: the second half reaches stage 2
    LeadingSpread, /// one spread window, then constant length: only the first window can gain
    MidSpread, /// the only window that can gain sits at the unclamped last probe index, behind declining ones
};

std::vector<std::string> makeTestColumn(ColumnShape shape, size_t rows)
{
    std::mt19937_64 rng(0x5eed5eed5eed5eedULL); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp)
    std::vector<std::string> out;
    out.reserve(rows);

    /// `p * whole_windows / MD5_GROUP_PROBE_WINDOWS` at p = P - 1, deliberately WITHOUT the clamp
    /// `md5GroupingPays` applies: a row count that leaves this at or below MD5_GROUP_DECLINE_BUDGET has
    /// the screen probe the gainable window, and one that leaves it past the bound does not.
    constexpr size_t window = DB::TargetSpecific::Default::MD5_GROUP_WINDOW;
    const size_t gain_window = (DB::TargetSpecific::Default::MD5_GROUP_PROBE_WINDOWS - 1) * (rows / window)
        / DB::TargetSpecific::Default::MD5_GROUP_PROBE_WINDOWS;

    for (size_t i = 0; i < rows; ++i)
    {
        bool spread = shape == ColumnShape::Spread || (shape == ColumnShape::Mixed && i < rows / 2)
            || (shape == ColumnShape::LeadingSpread && i < DB::TargetSpecific::Default::MD5_GROUP_WINDOW)
            || (shape == ColumnShape::MidSpread && i / window == gain_window);

        size_t len = 64;
        if (spread)
        {
            /// 0 / 1..40 / 200..500 / 4000..4200 bytes is 1 / 1 / 4..8 / 63..66 blocks, so rows on
            /// both sides of the 64-block histogram cap are present.
            switch (rng() % 10)
            {
                case 0: len = 0; break;
                case 1: case 2: case 3: case 4: case 5: case 6: len = 1 + rng() % 40; break;
                case 7: case 8: len = 200 + rng() % 301; break;
                default: len = 4000 + rng() % 201; break;
            }
        }
        else if (shape == ColumnShape::Periodic)
        {
            /// 4100 B is 65 blocks and 1..40 B is one, so every window holds one long row and 1023
            /// short ones, and its own rows can save at most one iteration per batch boundary.
            len = i % DB::TargetSpecific::Default::MD5_GROUP_WINDOW == 0 ? 4100 : 1 + rng() % 40;
        }
        else if (shape == ColumnShape::AboveCapHalf)
        {
            /// 4550 B is 72 blocks, the fewest for which the capped bound passes while the true counts
            /// tie, so the second half's windows are placed and then declined on their exact score.
            len = i < rows / 2 ? 20 : 4550;
        }

        std::string s(len, '\0');
        for (size_t k = 0; k < len; ++k)
            s[k] = static_cast<char>('a' + (rng() % 26));
        out.push_back(std::move(s));
    }

    return out;
}

/// Digests are the same whichever order the rows are hashed in, so which order ran is only visible
/// through these counters. Asserting them is what makes a test notice grouping silently stopping.
template <typename Trait>
void checkColumnMD5(ColumnShape shape, size_t rows, size_t expect_grouped, size_t expect_declined)
{
    const auto inputs = makeTestColumn(shape, rows);

    auto col = DB::ColumnString::create();
    for (const auto & s : inputs)
        col->insertData(s.data(), s.size());

    const ProfileEvents::Count grouped_before = ProfileEvents::global_counters[ProfileEvents::MD5GroupedRows];
    const ProfileEvents::Count declined_before = ProfileEvents::global_counters[ProfileEvents::MD5GroupingDeclinedRows];

    FixedChars digests;
    digests.resize(rows * 16);
    Trait::computeColumn(col->getChars(), col->getOffsets(), digests, rows);

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::MD5GroupedRows] - grouped_before, expect_grouped)
        << "rows hashed in grouped order";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::MD5GroupingDeclinedRows] - declined_before, expect_declined)
        << "rows admitted by the column screen and then hashed in column order";

    for (size_t i = 0; i < rows; ++i)
    {
        const std::string expected = referenceMD5Hex(inputs[i]);
        const std::string got = digestToHex(reinterpret_cast<const uint8_t *>(digests.data()) + i * 16);
        if (expected != got)
            FAIL() << "\nrow " << i << " of " << rows << ", length " << inputs[i].size() << "\n  expected: " << expected
                   << "\n  got:      " << got;
    }
}

TYPED_TEST(MD5MultiBufTest, ColumnBlockCountGrouping)
{
    /// 8192 rows clears the column screen's row minimum at every Ops width, and Mixed's halves are
    /// window-aligned, so its windows split into grouped and declining ones.
    checkColumnMD5<TypeParam>(ColumnShape::Spread, 8192, 8192, 0);
    checkColumnMD5<TypeParam>(ColumnShape::Flat, 8192, 0, 0);
    checkColumnMD5<TypeParam>(ColumnShape::Mixed, 8192, 4096, 4096);
}

TYPED_TEST(MD5MultiBufTest, ColumnBlockCountGroupingPartialWindow)
{
    /// 8709 is neither a multiple of the window nor of any batch width, so the last window is short and
    /// its last grouped batch holds fewer rows than there are lanes.
    checkColumnMD5<TypeParam>(ColumnShape::Spread, 8709, 8709, 0);
}

TYPED_TEST(MD5MultiBufTest, ColumnScreenPeriodicLengths)
{
    /// One long row per window, all at the same offset: no window's own rows can save enough to group,
    /// so the column screen must decline and neither counter may fire.
    checkColumnMD5<TypeParam>(ColumnShape::Periodic, 8192, /*grouped*/ 0, /*declined*/ 0);
}

TYPED_TEST(MD5MultiBufTest, ColumnGroupingDeclinedAfterPlacement)
{
    /// Rows above the histogram cap all tie, so the bound passes and the placement is the identity:
    /// these windows are declined on their exact score, which is the only path that reaches it.
    checkColumnMD5<TypeParam>(ColumnShape::AboveCapHalf, 8192, /*grouped*/ 0, /*declined*/ 8192);
}

TYPED_TEST(MD5MultiBufTest, ColumnGroupingDeclineBudget)
{
    /// Only the first window gains: the screen admits the column on it, and the constant-length ones
    /// each decline. The counters must sum to less than the 14300 rows, because scoring stops once
    /// declining windows outrun grouped ones and the rest is hashed unscored. 14300 leaves that rest a
    /// partial batch wide on the wider kernels, so its trailing short batch is covered too.
    checkColumnMD5<TypeParam>(ColumnShape::LeadingSpread, 14300, /*grouped*/ 1024, /*declined*/ 10240);
}

TYPED_TEST(MD5MultiBufTest, ColumnScreenProbesOnlyReachableWindows)
{
    /// Window w is entered with at most w declines behind it, so the decline budget cannot skip a
    /// window at or below it. 16385 rows put MidSpread's gainable window at that bound, where the
    /// screen probes it and the driver groups it; 18433 rows put it one past, where the screen does
    /// not probe it and the column runs in order having scored nothing.
    checkColumnMD5<TypeParam>(ColumnShape::MidSpread, 16385, /*grouped*/ 1024, /*declined*/ 10240);
    checkColumnMD5<TypeParam>(ColumnShape::MidSpread, 18433, /*grouped*/ 0, /*declined*/ 0);
}

} // anonymous namespace

#endif // USE_SSL
