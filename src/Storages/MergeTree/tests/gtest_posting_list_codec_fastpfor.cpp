#include <gtest/gtest.h>
#include <config.h>

#if USE_FASTPFOR

#include <Storages/MergeTree/FastPFORBlockCodec.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <cstddef>
#include <cstring>
#include <random>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{

/// Encode `values` as a single FastPFOR block, decode it back and assert the round trip is exact.
void verifyBlockRoundTrip(const std::vector<uint32_t> & values)
{
    ASSERT_FALSE(values.empty());
    ASSERT_LE(values.size(), 128u);

    FastPFORBlockCodec codec;

    std::vector<char> buffer(FastPFORBlockCodec::maxBlockBytes(), char(0xCC));
    std::span<char> out_span(buffer.data(), buffer.size());
    std::span<const uint32_t> in_span(values.data(), values.size());
    const size_t written = codec.encode(in_span, out_span);

    ASSERT_GT(written, 0u);
    ASSERT_LE(written, FastPFORBlockCodec::maxBlockBytes());
    ASSERT_EQ(written % sizeof(uint32_t), 0u) << "FastPFOR payload must be word-aligned";
    /// encode must advance the output span by exactly `written` bytes.
    ASSERT_EQ(out_span.size(), buffer.size() - written);

    std::vector<uint32_t> decoded(values.size(), 0xDEADBEEFu);
    std::span<const std::byte> dec_in(reinterpret_cast<const std::byte *>(buffer.data()), written);
    std::span<uint32_t> dec_out(decoded.data(), decoded.size());
    const size_t consumed = codec.decode(dec_in, values.size(), dec_out);

    EXPECT_EQ(consumed, written) << "decode must consume exactly the encoded bytes";
    EXPECT_EQ(dec_in.size(), 0u) << "decode must advance the input span past the block";
    EXPECT_EQ(decoded, values) << "round trip mismatch for " << values.size() << " values";
}

/// Seed is taken as an argument so the generator is reproducible without tripping the constant-seed lint.
std::mt19937 makeRng(std::mt19937::result_type seed)
{
    return std::mt19937(seed);
}

std::vector<uint32_t> makeMonotoneDeltas(size_t count, uint32_t step, uint32_t seed = 1)
{
    std::mt19937 rng = makeRng(seed);
    std::uniform_int_distribution<uint32_t> dist(1, step);
    std::vector<uint32_t> v(count);
    for (auto & x : v)
        x = dist(rng);
    return v;
}

}

TEST(FastPFORBlockCodec, RoundTripCounts)
{
    /// Cover tail sizes, the block boundary and a few representative shapes.
    for (size_t count : {1u, 2u, 3u, 4u, 5u, 31u, 32u, 63u, 64u, 127u, 128u})
    {
        verifyBlockRoundTrip(std::vector<uint32_t>(count, 0u));                 // all zero
        verifyBlockRoundTrip(std::vector<uint32_t>(count, 0xFFFFFFFFu));        // all max
        verifyBlockRoundTrip(std::vector<uint32_t>(count, 7u));                 // small constant
        verifyBlockRoundTrip(makeMonotoneDeltas(count, 64u));                   // small random deltas
    }
}

TEST(FastPFORBlockCodec, RoundTripExceptionPath)
{
    /// Mostly-small deltas with a few large outliers — this is what FastPFOR's patched exception coding
    /// is designed for, so make sure those blocks survive the round trip.
    for (size_t count : {8u, 32u, 64u, 128u})
    {
        std::vector<uint32_t> v(count, 3u);
        v[0] = 0xFFFFFFFFu;
        if (count > 4)
            v[count / 2] = 1u << 20;
        v[count - 1] = 1u << 28;
        verifyBlockRoundTrip(v);
    }
}

TEST(FastPFORBlockCodec, RoundTripRandomBitWidths)
{
    std::mt19937 rng = makeRng(12345);
    for (uint32_t bits = 1; bits <= 32; ++bits)
    {
        const uint32_t max_val = (bits >= 32) ? 0xFFFFFFFFu : ((1u << bits) - 1);
        std::uniform_int_distribution<uint32_t> dist(0, max_val);
        for (size_t count : {1u, 17u, 128u})
        {
            std::vector<uint32_t> v(count);
            for (auto & x : v)
                x = dist(rng);
            verifyBlockRoundTrip(v);
        }
    }
}

TEST(FastPFORBlockCodec, CorruptByteLengthNotWordAligned)
{
    FastPFORBlockCodec codec;
    std::vector<uint32_t> values(50, 9u);
    std::vector<char> buffer(FastPFORBlockCodec::maxBlockBytes());
    std::span<char> out_span(buffer.data(), buffer.size());
    std::span<const uint32_t> in_span(values.data(), values.size());
    const size_t written = codec.encode(in_span, out_span);

    /// A tail block whose byte length is not a multiple of 4 is structurally impossible.
    std::vector<uint32_t> decoded(values.size());
    std::span<const std::byte> dec_in(reinterpret_cast<const std::byte *>(buffer.data()), written - 1);
    std::span<uint32_t> dec_out(decoded.data(), decoded.size());
    EXPECT_THROW(codec.decode(dec_in, values.size(), dec_out), DB::Exception);
}

TEST(FastPFORBlockCodec, CorruptTooSmall)
{
    FastPFORBlockCodec codec;
    std::vector<uint32_t> decoded(4);
    const std::byte tiny[2] = {};
    std::span<const std::byte> dec_in(tiny, 2);
    std::span<uint32_t> dec_out(decoded.data(), decoded.size());
    EXPECT_THROW(codec.decode(dec_in, 4u, dec_out), DB::Exception);
}

TEST(FastPFORBlockCodec, CorruptOversizedEmbeddedCount)
{
    FastPFORBlockCodec codec;
    /// A full block starts with the SIMDFastPFor length word; forcing it huge must be rejected, not crash.
    std::vector<uint32_t> values(128, 5u);
    std::vector<char> buffer(FastPFORBlockCodec::maxBlockBytes());
    std::span<char> out_span(buffer.data(), buffer.size());
    std::span<const uint32_t> in_span(values.data(), values.size());
    const size_t written = codec.encode(in_span, out_span);

    uint32_t huge = 0x7FFFFFFFu;
    std::memcpy(buffer.data(), &huge, sizeof(huge));

    std::vector<uint32_t> decoded(values.size());
    std::span<const std::byte> dec_in(reinterpret_cast<const std::byte *>(buffer.data()), written);
    std::span<uint32_t> dec_out(decoded.data(), decoded.size());
    EXPECT_THROW(codec.decode(dec_in, values.size(), dec_out), DB::Exception);
}

namespace
{

/// Encode `values` as one FastPFOR block and return the exact encoded bytes, so corruption tests can
/// tamper with individual words and confirm decode rejects them with CORRUPTED_DATA instead of reading
/// or writing out of bounds.
std::vector<char> encodeBlockBytes(const std::vector<uint32_t> & values)
{
    FastPFORBlockCodec codec;
    std::vector<char> buffer(FastPFORBlockCodec::maxBlockBytes());
    std::span<char> out_span(buffer.data(), buffer.size());
    std::span<const uint32_t> in_span(values.data(), values.size());
    const size_t written = codec.encode(in_span, out_span);
    buffer.resize(written);
    return buffer;
}

/// Decode `bytes` as a block of `count` values and assert it raises CORRUPTED_DATA.
void expectDecodeCorrupted(const std::vector<char> & bytes, size_t count)
{
    FastPFORBlockCodec codec;
    std::vector<uint32_t> decoded(count, 0u);
    std::span<const std::byte> dec_in(reinterpret_cast<const std::byte *>(bytes.data()), bytes.size());
    std::span<uint32_t> dec_out(decoded.data(), decoded.size());
    try
    {
        codec.decode(dec_in, count, dec_out);
        FAIL() << "expected decode to throw CORRUPTED_DATA for " << bytes.size() << " bytes";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CORRUPTED_DATA) << e.message();
    }
}

uint32_t getBlockWord(const std::vector<char> & bytes, size_t i)
{
    uint32_t w = 0;
    std::memcpy(&w, bytes.data() + i * sizeof(uint32_t), sizeof(uint32_t));
    return w;
}

void setBlockWord(std::vector<char> & bytes, size_t i, uint32_t w)
{
    std::memcpy(bytes.data() + i * sizeof(uint32_t), &w, sizeof(uint32_t));
}

}

TEST(FastPFORBlockCodec, RejectsCorruptTailOverflow)
{
    /// #2: a corrupted tail whose span holds far more terminated VariableByte values than `count` must not
    /// write past the 128-slot output block. Build a tail block (zero SIMD header) followed by 64 terminated
    /// single-byte values and decode it with a small count: the bounded tail decoder must stop and throw.
    std::vector<char> bytes(sizeof(uint32_t), '\0');                  // SIMD zero-length header word
    bytes.insert(bytes.end(), 64, static_cast<char>(0x80));          // 64 terminated single-byte values
    expectDecodeCorrupted(bytes, 5u);
}

TEST(FastPFORBlockCodec, RejectsCorruptWheremeta)
{
    /// #3: a corrupted `wheremeta` offset (word 1) used to jump the metadata pointer arbitrarily far must be
    /// rejected before the SIMD decoder dereferences it.
    std::vector<char> bytes = encodeBlockBytes(std::vector<uint32_t>(128, 5u));
    setBlockWord(bytes, 1, 0x7FFFFFF5u);                             // (wheremeta - 1) % 4 == 0 but far OOB
    expectDecodeCorrupted(bytes, 128u);
}

TEST(FastPFORBlockCodec, RejectsCorruptByteContainerSize)
{
    /// #3: a corrupted byte-container size must not push the bitmap / exception pointers past the page.
    std::vector<char> bytes = encodeBlockBytes(std::vector<uint32_t>(128, 5u));
    const uint32_t wheremeta = getBlockWord(bytes, 1);
    setBlockWord(bytes, 1u + wheremeta, 0x7FFFFFFFu);                // bytesize word
    expectDecodeCorrupted(bytes, 128u);
}

TEST(FastPFORBlockCodec, RejectsCorruptExceptionPosition)
{
    /// #3: an exception position byte indexes the output block directly; a value >= 128 would write past
    /// decode_out and must be rejected.
    std::vector<uint32_t> values(128, 3u);
    values[10] = 1u << 20;                                           // force at least two exceptions
    values[20] = 1u << 25;
    std::vector<char> bytes = encodeBlockBytes(values);
    const uint32_t wheremeta = getBlockWord(bytes, 1);
    /// Byte container starts right after the bytesize word: [b][cexcept][maxbits][positions...].
    const size_t bc_byte = (static_cast<size_t>(1) + wheremeta + 1) * sizeof(uint32_t);
    bytes[bc_byte + 3] = static_cast<char>(200);                     // first position byte -> outside [0,128)
    expectDecodeCorrupted(bytes, 128u);
}

TEST(FastPFORBlockCodec, RejectsCorruptFullBlockHeaderCount)
{
    /// #3: the SIMD page count word must equal 128 for a full block; any other value is rejected.
    std::vector<char> bytes = encodeBlockBytes(std::vector<uint32_t>(128, 5u));
    setBlockWord(bytes, 0, 64u);
    expectDecodeCorrupted(bytes, 128u);
}

namespace
{

/// Round trip a whole posting list through PostingListCodecFastPFOR (single segment) and compare.
void verifyPostingsRoundTrip(const std::vector<uint32_t> & row_ids)
{
    PostingList src;
    for (auto id : row_ids)
        src.add(id);

    PostingListCodecFastPFOR codec;
    TokenPostingsInfo info;
    WriteBufferFromOwnString out;
    /// A large segment size keeps everything in one segment so a single decode() reconstructs all postings.
    codec.encode(src, 1u << 20, info, out);

    const std::string serialized = out.str();
    ReadBufferFromString in(serialized);
    PostingList dst;
    codec.decode(in, dst);

    EXPECT_TRUE(src == dst) << "posting list round trip mismatch for cardinality " << row_ids.size();
}

}

TEST(PostingListCodecFastPFOR, PostingsRoundTrip)
{
    /// Multi-block single-segment posting lists with different gap shapes.
    std::mt19937 rng = makeRng(777);

    for (size_t cardinality : {13u, 128u, 129u, 1000u, 5000u})
    {
        std::uniform_int_distribution<uint32_t> gap(1, 50);
        std::vector<uint32_t> ids;
        uint32_t cur = 0;
        for (size_t i = 0; i < cardinality; ++i)
        {
            cur += gap(rng);
            ids.push_back(cur);
        }
        verifyPostingsRoundTrip(ids);
    }
}

TEST(PostingListCodecFastPFOR, PostingsRoundTripWithLargeGaps)
{
    /// Sparse posting list with occasional very large gaps (exercises the exception path end to end).
    std::vector<uint32_t> ids;
    uint32_t cur = 0;
    for (size_t i = 0; i < 2000; ++i)
    {
        cur += (i % 100 == 0) ? 1'000'000u : 2u;
        ids.push_back(cur);
    }
    verifyPostingsRoundTrip(ids);
}

#endif
