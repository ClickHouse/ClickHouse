#include <Common/TargetSpecific.h>

#if USE_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))
#    include "config.h"
#    if USE_SSL

#        include <gtest/gtest.h>

#        include <cstring>
#        include <iomanip>
#        include <random>
#        include <sstream>
#        include <string>
#        include <vector>

#        include <openssl/evp.h>

#        define SHA1_GTEST_UNIT_TEST
#        include "Functions/FunctionSHA1.cpp" // NOLINT(bugprone-suspicious-include)

namespace
{

std::string digestToHex(const uint8_t * digest)
{
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (int i = 0; i < 20; ++i)
        oss << std::setw(2) << static_cast<unsigned>(digest[i]);
    return oss.str();
}

std::string referenceSHA1Hex(const uint8_t * data, size_t len)
{
    unsigned char digest[20];
    EVP_MD_CTX * ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha1(), nullptr);
    EVP_DigestUpdate(ctx, data, len);
    unsigned int md_len = 0;
    EVP_DigestFinal_ex(ctx, digest, &md_len);
    EVP_MD_CTX_free(ctx);
    return digestToHex(digest);
}

std::string referenceSHA1Hex(const std::string & s)
{
    return referenceSHA1Hex(reinterpret_cast<const uint8_t *>(s.data()), s.size());
}


/// FIPS 180-4 test vectors.
struct SHA1TestVector
{
    std::string input;
    std::string expected_hex;
};

const std::vector<SHA1TestVector> fips180_vectors = {
    {"", "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
    {"abc", "a9993e364706816aba3e25717850c26c9cd0d89d"},
    {"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", "84983e441c3bd26ebaae4aa1f95129e5e54670f1"},
};


// ============================================================
// Helper function tests
// ============================================================

TEST(SHA1Helpers, NumSHA1Blocks)
{
    EXPECT_EQ(numSHA1Blocks(0), 1u);
    EXPECT_EQ(numSHA1Blocks(1), 1u);
    EXPECT_EQ(numSHA1Blocks(55), 1u);
    EXPECT_EQ(numSHA1Blocks(56), 2u);
    EXPECT_EQ(numSHA1Blocks(63), 2u);
    EXPECT_EQ(numSHA1Blocks(64), 2u);
    EXPECT_EQ(numSHA1Blocks(119), 2u);
    EXPECT_EQ(numSHA1Blocks(120), 3u);
    EXPECT_EQ(numSHA1Blocks(128), 3u);
}

TEST(SHA1Helpers, PadFinalBlocks)
{
    {
        /// Empty input: 1 final block, 0x80 at offset 0, bit-length = 0 in last 8 bytes (big-endian).
        alignas(64) uint8_t buf[128] = {};
        size_t count = sha1PadFinalBlocks(reinterpret_cast<const uint8_t *>(""), 0, buf);
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(buf[0], 0x80);
        for (int i = 1; i < 56; ++i)
            EXPECT_EQ(buf[i], 0) << "byte " << i;
        /// Big-endian length at end of block.
        uint64_t stored_len = 0;
        for (int i = 0; i < 8; ++i)
            stored_len = (stored_len << 8) | buf[56 + i];
        EXPECT_EQ(stored_len, 0u);
    }
    {
        /// 55 bytes: fits in 1 final block.
        std::string input(55, 'x');
        alignas(64) uint8_t buf[128] = {};
        size_t count = sha1PadFinalBlocks(reinterpret_cast<const uint8_t *>(input.data()), 55, buf);
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(buf[55], 0x80);
    }
    {
        /// 56 bytes: needs 2 final blocks.
        std::string input(56, 'y');
        alignas(64) uint8_t buf[128] = {};
        size_t count = sha1PadFinalBlocks(reinterpret_cast<const uint8_t *>(input.data()), 56, buf);
        EXPECT_EQ(count, 2u);
        EXPECT_EQ(buf[56], 0x80);
    }
}


// ============================================================
// Trait structs: pair Ops with the correct namespace
// ============================================================

struct AVX512SHA1Trait
{
    using Ops = DB::TargetSpecific::x86_64_v4::AVX512SHA1Ops;
    static constexpr size_t lanes = Ops::lanes;

    static void skipIfUnsupported()
    {
        if (!DB::isArchSupported(DB::TargetArch::x86_64_v4))
            GTEST_SKIP() << "x86_64_v4 (AVX-512) not supported on this host";
    }

    static void compute(const uint8_t * const inputs[], const size_t lengths[], uint8_t * output, size_t actual_count)
    {
        DB::TargetSpecific::x86_64_v4::sha1MultiBufCompute<Ops>(inputs, lengths, output, actual_count);
    }
};


// ============================================================
// Typed test suite
// ============================================================

template <typename T>
class SHA1MultiBufTest : public ::testing::Test
{
protected:
    void SetUp() override { T::skipIfUnsupported(); }
};

using SHA1Implementations = ::testing::Types<AVX512SHA1Trait>;

TYPED_TEST_SUITE(SHA1MultiBufTest, SHA1Implementations);


// ============================================================
// Helpers: compute one or a batch of SHA-1 digests
// ============================================================

template <typename Trait>
std::string computeOneSHA1(const std::string & input)
{
    constexpr size_t N = Trait::lanes;

    const uint8_t * inputs[N];
    size_t lengths[N];

    inputs[0] = reinterpret_cast<const uint8_t *>(input.data());
    lengths[0] = input.size();

    for (size_t j = 1; j < N; ++j)
    {
        inputs[j] = &sha1_dummy_lane_byte;
        lengths[j] = 0;
    }

    alignas(64) uint8_t output[N * 20];
    std::memset(output, 0xCC, sizeof(output));

    Trait::compute(inputs, lengths, output, 1);
    return digestToHex(output);
}

template <typename Trait>
std::vector<std::string> computeBatchSHA1(const std::vector<std::string> & batch_inputs)
{
    constexpr size_t N = Trait::lanes;
    size_t actual_count = batch_inputs.size();
    EXPECT_LE(actual_count, N);

    const uint8_t * inputs[N];
    size_t lengths[N];

    for (size_t j = 0; j < actual_count; ++j)
    {
        inputs[j] = reinterpret_cast<const uint8_t *>(batch_inputs[j].data());
        lengths[j] = batch_inputs[j].size();
    }
    for (size_t j = actual_count; j < N; ++j)
    {
        inputs[j] = &sha1_dummy_lane_byte;
        lengths[j] = 0;
    }

    alignas(64) uint8_t output[N * 20];
    std::memset(output, 0xCC, sizeof(output));

    Trait::compute(inputs, lengths, output, actual_count);

    std::vector<std::string> results;
    results.reserve(actual_count);
    for (size_t j = 0; j < actual_count; ++j)
        results.push_back(digestToHex(output + j * 20));
    return results;
}


// ============================================================
// Test cases
// ============================================================

TYPED_TEST(SHA1MultiBufTest, FIPS180Vectors)
{
    for (const auto & tv : fips180_vectors)
    {
        SCOPED_TRACE(tv.input);
        EXPECT_EQ(tv.expected_hex, computeOneSHA1<TypeParam>(tv.input));
    }
}

TYPED_TEST(SHA1MultiBufTest, SingleInput)
{
    std::string input = "The quick brown fox jumps over the lazy dog";
    std::string expected = referenceSHA1Hex(input);
    EXPECT_EQ(expected, computeOneSHA1<TypeParam>(input));
}

TYPED_TEST(SHA1MultiBufTest, FullBatch)
{
    constexpr size_t N = TypeParam::lanes;
    std::vector<std::string> inputs;
    inputs.reserve(N);
    for (size_t i = 0; i < N; ++i)
        inputs.push_back("input_" + std::to_string(i) + "_" + std::string(i * 3, 'A'));

    auto results = computeBatchSHA1<TypeParam>(inputs);
    ASSERT_EQ(results.size(), N);

    for (size_t i = 0; i < N; ++i)
    {
        SCOPED_TRACE("batch index " + std::to_string(i) + ": \"" + inputs[i] + "\"");
        EXPECT_EQ(referenceSHA1Hex(inputs[i]), results[i]);
    }
}

TYPED_TEST(SHA1MultiBufTest, VaryingLengthBatch)
{
    /// Create inputs of specific lengths that exercise different code paths.
    std::vector<size_t> target_lengths = {0, 1, 14, 55, 56, 63, 64, 80};
    constexpr size_t N = TypeParam::lanes;

    /// Use as many as fit in one batch.
    size_t count = std::min(target_lengths.size(), N);
    std::vector<std::string> inputs;
    inputs.reserve(count);
    for (size_t i = 0; i < count; ++i)
        inputs.push_back(std::string(target_lengths[i], static_cast<char>('a' + (i % 26))));

    auto results = computeBatchSHA1<TypeParam>(inputs);
    ASSERT_EQ(results.size(), count);

    for (size_t i = 0; i < count; ++i)
    {
        SCOPED_TRACE("length " + std::to_string(inputs[i].size()));
        EXPECT_EQ(referenceSHA1Hex(inputs[i]), results[i]);
    }
}

TYPED_TEST(SHA1MultiBufTest, BlockBoundaries)
{
    /// Test inputs at SHA-1 padding boundary lengths.
    std::vector<size_t> boundary_lengths = {55, 56, 63, 64, 119, 120, 127, 128};

    for (size_t len : boundary_lengths)
    {
        SCOPED_TRACE("length " + std::to_string(len));
        std::string input(len, 'Z');
        EXPECT_EQ(referenceSHA1Hex(input), computeOneSHA1<TypeParam>(input));
    }
}

TYPED_TEST(SHA1MultiBufTest, MultiBlockLongStrings)
{
    for (size_t len : {128, 500, 1000, 4096})
    {
        SCOPED_TRACE("length " + std::to_string(len));
        std::string input(len, 'Q');
        EXPECT_EQ(referenceSHA1Hex(input), computeOneSHA1<TypeParam>(input));
    }
}

TYPED_TEST(SHA1MultiBufTest, StressRandom)
{
    constexpr size_t N = TypeParam::lanes;
    std::random_device rd;
    auto seed = rd();
    std::cerr << "StressRandom seed: " << seed << std::endl;
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<size_t> len_dist(0, 1024);
    std::uniform_int_distribution<int> byte_dist(0, 255);

    constexpr size_t iterations = 200;

    for (size_t iter = 0; iter < iterations; ++iter)
    {
        size_t batch_size = (rng() % N) + 1;
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

        auto results = computeBatchSHA1<TypeParam>(inputs);
        ASSERT_EQ(results.size(), batch_size);

        for (size_t j = 0; j < batch_size; ++j)
        {
            std::string expected = referenceSHA1Hex(inputs[j]);
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

} // anonymous namespace

#    endif // USE_SSL

#endif // USE_MULTITARGET_CODE && x86_64
