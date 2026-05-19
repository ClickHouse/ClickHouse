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
        uint64_t stored_len;
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

    static void compute(const uint8_t * const inputs[], const size_t lengths[], uint8_t * output, size_t actual_count)
    {
        DB::TargetSpecific::Default::md5MultiBufCompute<Ops>(inputs, lengths, output, actual_count);
    }
};

#if USE_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))

struct AVX2MD5Trait
{
    using Ops = DB::TargetSpecific::x86_64_v3::AVX2MD5Ops;
    static constexpr size_t lanes = Ops::lanes;

    static void compute(const uint8_t * const inputs[], const size_t lengths[], uint8_t * output, size_t actual_count)
    {
        DB::TargetSpecific::x86_64_v3::md5MultiBufCompute<Ops>(inputs, lengths, output, actual_count);
    }
};

struct AVX512MD5Trait
{
    using Ops = DB::TargetSpecific::x86_64_v4::AVX512MD5Ops;
    static constexpr size_t lanes = Ops::lanes;

    static void compute(const uint8_t * const inputs[], const size_t lengths[], uint8_t * output, size_t actual_count)
    {
        DB::TargetSpecific::x86_64_v4::md5MultiBufCompute<Ops>(inputs, lengths, output, actual_count);
    }
};

#endif


// ============================================================
// Typed test suite
// ============================================================

template <typename T>
class MD5MultiBufTest : public ::testing::Test
{
};

using MD5Implementations = ::testing::Types<
    ScalarMD5Trait
#if USE_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))
    ,
    AVX2MD5Trait,
    AVX512MD5Trait
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

} // anonymous namespace

#endif // USE_SSL
