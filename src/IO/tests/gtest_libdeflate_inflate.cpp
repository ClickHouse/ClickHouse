#include "config.h"

#if USE_LIBDEFLATE

#include <IO/LibdeflateInflatingReadBuffer.h>
#include <IO/ZlibDeflatingWriteBuffer.h>
#include <IO/Libdeflate.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <gtest/gtest.h>

#include <libdeflate.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace DB;

namespace
{

std::string makeData(size_t size, unsigned seed)
{
    std::string out;
    out.reserve(size);
    std::mt19937 rng(seed);
    while (out.size() < size)
    {
        switch (rng() % 4)
        {
            case 0: out.push_back(static_cast<char>(rng())); break;                 /* random */
            case 1: out += "the quick brown fox 0123456789 "; break;               /* text */
            case 2: out.append(rng() % 40, 'x'); break;                            /* runs */
            default: out.push_back(static_cast<char>('a' + rng() % 6)); break;     /* low entropy */
        }
    }
    out.resize(size);
    return out;
}

/// A ReadBuffer that serves the data in fixed-size chunks, to exercise streaming input.
class ChunkedReadBuffer : public ReadBuffer
{
public:
    ChunkedReadBuffer(std::string data_, size_t chunk_) : ReadBuffer(nullptr, 0), data(std::move(data_)), chunk(chunk_) {}
private:
    bool nextImpl() override
    {
        if (pos_ >= data.size())
            return false;
        size_t n = std::min(chunk, data.size() - pos_);
        working_buffer = Buffer(data.data() + pos_, data.data() + pos_ + n);
        pos_ += n;
        return true;
    }
    std::string data;
    size_t pos_ = 0;
    size_t chunk;
};

std::string zlibCompress(const std::string & data, CompressionMethod method, int level)
{
    std::string out;
    {
        ZlibDeflatingWriteBuffer wb(std::make_unique<WriteBufferFromString>(out), method, level);
        wb.write(data.data(), data.size());
        wb.finalize();
    }
    return out;
}

std::string decompressViaBuffer(const std::string & compressed, CompressionMethod method, size_t nested_chunk, size_t buf_size)
{
    LibdeflateInflatingReadBuffer rb(std::make_unique<ChunkedReadBuffer>(compressed, nested_chunk), method, buf_size);
    std::string out;
    readStringUntilEOF(out, rb);
    return out;
}

}

class LibdeflateInflateTest : public ::testing::TestWithParam<CompressionMethod> {};

TEST_P(LibdeflateInflateTest, RoundTripFromZlibNg)
{
    const CompressionMethod method = GetParam();
    for (size_t size : {size_t(0), size_t(1), size_t(100), size_t(50000), size_t(1000000)})
    {
        const std::string data = makeData(size, static_cast<unsigned>(size + 1));
        const std::string compressed = zlibCompress(data, method, 6);
        for (size_t chunk : {size_t(1), size_t(13), size_t(4096), !compressed.empty() ? compressed.size() : size_t(1)})
            for (size_t buf : {size_t(64), size_t(1 << 16)}) /* small buf forces NEED_OUTPUT/grow */
                EXPECT_EQ(decompressViaBuffer(compressed, method, chunk, buf), data)
                    << "size=" << size << " chunk=" << chunk << " buf=" << buf;
    }
}

/// zlib-ng's deflate_quick path (compression level 1) opens a single static-Huffman block and
/// never closes it until the end of the stream, so a multi-megabyte level-1 stream is the
/// real-encoder version of the single-block shape of issue #114045; keep it large and the input
/// chunks small so a decoder that re-decodes from the block start on every refill cannot hide.
TEST_P(LibdeflateInflateTest, RoundTripFromZlibNgLevelOneLarge)
{
    const CompressionMethod method = GetParam();
    const std::string data = makeData(8 << 20, 3);
    const std::string compressed = zlibCompress(data, method, 1);
    for (size_t chunk : {size_t(16 * 1024), size_t(256 * 1024)})
        EXPECT_EQ(decompressViaBuffer(compressed, method, chunk, 1 << 20), data) << "chunk=" << chunk;
}

/// Two concatenated members must decode to the concatenation of their contents.
TEST_P(LibdeflateInflateTest, MultiMember)
{
    const CompressionMethod method = GetParam();
    const std::string a = makeData(20000, 1);
    const std::string b = makeData(35000, 2);
    const std::string compressed = zlibCompress(a, method, 9) + zlibCompress(b, method, 1);
    for (size_t chunk : {size_t(1), size_t(7), size_t(9999)})
        EXPECT_EQ(decompressViaBuffer(compressed, method, chunk, 4096), a + b) << "chunk=" << chunk;
}

/// Cross-check: decode a stream that libdeflate's own one-shot compressor produced.
TEST_P(LibdeflateInflateTest, RoundTripFromLibdeflateOneShot)
{
    const CompressionMethod method = GetParam();
    const std::string data = makeData(123456, 7);
    std::vector<char> buf(Libdeflate::compressBound(method, 9, data.size()));
    const size_t csize = Libdeflate::compress(method, 9, data.data(), data.size(), buf.data(), buf.size());
    EXPECT_EQ(decompressViaBuffer(std::string(buf.data(), csize), method, 257, 8192), data);
}

TEST_P(LibdeflateInflateTest, MalformedThrows)
{
    const CompressionMethod method = GetParam();
    const std::string garbage = "this is not a valid compressed stream at all, no really";
    EXPECT_ANY_THROW(decompressViaBuffer(garbage, method, 8, 4096));
}

/// A corrupted gzip/zlib trailer (wrong CRC32/Adler32/ISIZE) must be rejected even when the caller
/// reads exactly the uncompressed byte count with readStrict and never reaches EOF. The trailer is
/// verified as the final DEFLATE block's output is produced, so the check cannot be skipped.
TEST_P(LibdeflateInflateTest, CorruptTrailerRejectedOnExactRead)
{
    const CompressionMethod method = GetParam();
    const std::string data = makeData(10000, 5);
    for (size_t buf : {size_t(4096), size_t(1 << 16) /* whole stream in the final SUCCESS chunk */})
    {
        std::string compressed = zlibCompress(data, method, 6);
        /// Flip a bit in the last trailer byte: gzip ISIZE high byte, or zlib Adler32 low byte.
        compressed.back() ^= 0x01;

        LibdeflateInflatingReadBuffer rb(std::make_unique<ChunkedReadBuffer>(compressed, 4096), method, buf);
        std::vector<char> dest(data.size());
        EXPECT_ANY_THROW(rb.readStrict(dest.data(), dest.size())) << "buf=" << buf;
    }
}

INSTANTIATE_TEST_SUITE_P(
    GzipAndZlib,
    LibdeflateInflateTest,
    ::testing::Values(CompressionMethod::Gzip, CompressionMethod::Zlib));

/// Malformed headers that the zlib decoder rejects must also be rejected here (validation parity with
/// ZlibInflatingReadBuffer): such streams must not silently decompress. These are format-specific, so
/// they are plain tests rather than parameterized over both methods.

TEST(LibdeflateInflateHeaderValidation, RejectsReservedGzipFlags)
{
    /// 10-byte gzip header with reserved FLG bits (0xE0) set; rejected before the body is read.
    const std::string bad = std::string("\x1f\x8b\x08\xe0\x00\x00\x00\x00\x00\xff", 10) + "garbage";
    EXPECT_ANY_THROW(decompressViaBuffer(bad, CompressionMethod::Gzip, 4, 4096));
}

TEST(LibdeflateInflateHeaderValidation, RejectsZlibWindowTooLarge)
{
    /// zlib header with CINFO = 8 (window > 32 KiB, invalid). CMF=0x88, FLG=0x1c makes the 16-bit
    /// header a multiple of 31, so the check bits pass and only the CINFO check can reject it.
    const std::string bad = std::string("\x88\x1c", 2) + "garbage";
    EXPECT_ANY_THROW(decompressViaBuffer(bad, CompressionMethod::Zlib, 4, 4096));
}

TEST(LibdeflateInflateHeaderValidation, RejectsBadGzipHeaderCrc)
{
    /// gzip header with the FHCRC flag set and a deliberately wrong header CRC16. The correct CRC16 of
    /// these 10 header bytes is 0xC990, so 0xC991 (little-endian 0x91 0xC9) must be rejected.
    std::string bad = std::string("\x1f\x8b\x08\x02\x00\x00\x00\x00\x00\xff", 10);
    bad += std::string("\x91\xc9", 2);
    bad += "garbage";
    EXPECT_ANY_THROW(decompressViaBuffer(bad, CompressionMethod::Gzip, 4, 4096));
}

/// Regression tests for issue #114045: a single DEFLATE block spanning the whole stream - the shape
/// zlib-ng's deflate_quick path (compression level 1, the default of the official .NET SDK) emits -
/// must decompress in linear time with bounded memory. No real encoder is available here, so the
/// stream is crafted directly: a static-Huffman DEFLATE writer producing one block of arbitrary size.

namespace
{

/// Emits a raw DEFLATE stream as a single static-Huffman block (RFC 1951 section 3.2.6).
class SingleBlockDeflateWriter
{
public:
    void putLiteral(uint8_t b)
    {
        if (b < 144)
            putCode(0x30 + b, 8);
        else
            putCode(0x190 + (b - 144), 9);
        reference.push_back(static_cast<char>(b));
    }

    void putMatch(unsigned len /* 3..258 */, unsigned dist /* 1..32768, <= bytes emitted */)
    {
        static constexpr struct { unsigned base; int extra; } lengths[29] = {
            {3,0},{4,0},{5,0},{6,0},{7,0},{8,0},{9,0},{10,0},
            {11,1},{13,1},{15,1},{17,1},{19,2},{23,2},{27,2},{31,2},
            {35,3},{43,3},{51,3},{59,3},{67,4},{83,4},{99,4},{115,4},
            {131,5},{163,5},{195,5},{227,5},{258,0}};
        static constexpr struct { unsigned base; int extra; } distances[30] = {
            {1,0},{2,0},{3,0},{4,0},{5,1},{7,1},{9,2},{13,2},{17,3},{25,3},
            {33,4},{49,4},{65,5},{97,5},{129,6},{193,6},{257,7},{385,7},
            {513,8},{769,8},{1025,9},{1537,9},{2049,10},{3073,10},
            {4097,11},{6145,11},{8193,12},{12289,12},{16385,13},{24577,13}};

        int li = len == 258 ? 28 : 27;
        while (lengths[li].base > len)
            --li;
        unsigned length_sym = 257 + li;
        if (length_sym < 280)
            putCode(length_sym - 256, 7);
        else
            putCode(0xC0 + (length_sym - 280), 8);
        if (lengths[li].extra)
            putBits(len - lengths[li].base, lengths[li].extra);

        int di = 29;
        while (distances[di].base > dist)
            --di;
        putCode(di, 5);
        if (distances[di].extra)
            putBits(dist - distances[di].base, distances[di].extra);

        for (unsigned i = 0; i < len; ++i)
            reference.push_back(reference[reference.size() - dist]);
    }

    /// Returns the raw DEFLATE stream; the object must not be used afterwards.
    std::string finish()
    {
        putCode(0, 7); /* end-of-block */
        if (nbits)
            deflate.push_back(static_cast<char>(bitbuf));
        return std::move(deflate);
    }

    std::string reference; /* the uncompressed bytes described so far */

    SingleBlockDeflateWriter()
    {
        putBits(1, 1); /* BFINAL */
        putBits(1, 2); /* BTYPE: static Huffman */
    }

private:
    /// Raw bits, LSB-first (DEFLATE bit order for headers and extra bits).
    void putBits(uint32_t v, int n)
    {
        bitbuf |= static_cast<uint64_t>(v) << nbits;
        nbits += n;
        while (nbits >= 8)
        {
            deflate.push_back(static_cast<char>(bitbuf));
            bitbuf >>= 8;
            nbits -= 8;
        }
    }

    /// A Huffman codeword is written starting from its most significant bit.
    void putCode(uint32_t c, int n)
    {
        uint32_t reversed = 0;
        for (int i = 0; i < n; ++i)
            reversed |= ((c >> i) & 1) << (n - 1 - i);
        putBits(reversed, n);
    }

    std::string deflate;
    uint64_t bitbuf = 0;
    int nbits = 0;
};

/// One gzip/zlib member whose DEFLATE payload is a single static-Huffman block: 'literal_bytes' of
/// pseudo-random literals followed by matches (with distances reaching the full 32 KiB window) up to
/// 'total_bytes' of output. Returns {member, reference}.
std::pair<std::string, std::string> craftSingleBlockMember(CompressionMethod method, size_t literal_bytes, size_t total_bytes)
{
    SingleBlockDeflateWriter writer;
    std::mt19937 rng(42); /// NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp) deterministic test data on purpose
    for (size_t i = 0; i < literal_bytes; ++i)
        writer.putLiteral(static_cast<uint8_t>(rng()));
    while (writer.reference.size() < total_bytes)
        writer.putMatch(
            3 + rng() % 256,
            static_cast<unsigned>(1 + rng() % std::min<size_t>(writer.reference.size(), 32768)));
    std::string reference = writer.reference;
    const std::string deflate = writer.finish();

    std::string member;
    if (method == CompressionMethod::Gzip)
    {
        member = std::string("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff", 10);
        member += deflate;
        const uint32_t crc = libdeflate_crc32(0, reference.data(), reference.size());
        const auto size32 = static_cast<uint32_t>(reference.size());
        for (int i = 0; i < 4; ++i)
            member.push_back(static_cast<char>(crc >> (8 * i)));
        for (int i = 0; i < 4; ++i)
            member.push_back(static_cast<char>(size32 >> (8 * i)));
    }
    else
    {
        member = std::string("\x78\x9c", 2);
        member += deflate;
        const uint32_t adler = libdeflate_adler32(1, reference.data(), reference.size());
        for (int i = 3; i >= 0; --i)
            member.push_back(static_cast<char>(adler >> (8 * i)));
    }
    return {member, reference};
}

}

/// The whole member is one DEFLATE block: decompression must be correct through the read buffer,
/// including matches that cross the suspension points where the nested input chunks end.
TEST_P(LibdeflateInflateTest, SingleDeflateBlockSpanningWholeMember)
{
    const CompressionMethod method = GetParam();
    const auto [member, reference] = craftSingleBlockMember(method, 4 << 20, 8 << 20);
    for (size_t chunk : {size_t(16 * 1024), size_t(256 * 1024)})
        EXPECT_EQ(decompressViaBuffer(member, method, chunk, 1 << 20), reference) << "chunk=" << chunk;
}

/// The linearity contract underlying #114045: when the input runs out inside a Huffman block, the
/// streaming decoder must have consumed everything up to the last decoded symbol, so that the caller
/// never re-feeds (and the decoder never re-decodes) more than a constant amount. Before the fix, a
/// suspension rolled back to the block start: every call consumed nothing and re-decoded the whole
/// fed prefix, making ingestion quadratic in the block's compressed size.
TEST(LibdeflateSingleBlock, StreamingConsumesInputWithinBlock)
{
    SingleBlockDeflateWriter writer;
    std::mt19937 rng(7); /// NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp) deterministic test data on purpose
    for (size_t i = 0; i < (4 << 20); ++i)
        writer.putLiteral(static_cast<uint8_t>(rng()));
    const std::string reference = writer.reference;
    const std::string deflate = writer.finish();

    libdeflate_decompressor * decompressor = libdeflate_alloc_decompressor();
    ASSERT_NE(decompressor, nullptr);
    libdeflate_deflate_decompress_stream_reset(decompressor);

    constexpr size_t window_max = 32768;
    constexpr size_t chunk = 64 * 1024;
    std::vector<char> out(window_max + (1 << 20));
    std::string result;
    size_t window = 0;
    size_t consumed = 0;
    size_t fed = std::min(deflate.size(), chunk);

    while (true)
    {
        const bool end_of_input = fed == deflate.size();
        size_t in_used = 0;
        size_t out_used = 0;
        const libdeflate_result r = libdeflate_deflate_decompress_stream(
            decompressor, end_of_input ? 1 : 0,
            deflate.data() + consumed, fed - consumed,
            out.data() + window, out.size() - window, window, &in_used, &out_used);
        result.append(out.data() + window, out_used);
        consumed += in_used;

        const size_t total = window + out_used;
        const size_t new_window = std::min(total, window_max);
        memmove(out.data(), out.data() + (total - new_window), new_window);
        window = new_window;

        if (r == LIBDEFLATE_SUCCESS)
            break;
        if (r == LIBDEFLATE_STREAM_NEED_INPUT)
        {
            ASSERT_FALSE(end_of_input);
            /// The heart of the regression test: all but at most a suspended symbol plus the
            /// refill lookahead must have been consumed.
            ASSERT_LE(fed - consumed, 64u);
            fed = std::min(deflate.size(), fed + chunk);
            continue;
        }
        ASSERT_EQ(r, LIBDEFLATE_STREAM_NEED_OUTPUT);
    }
    libdeflate_free_decompressor(decompressor);
    EXPECT_EQ(result, reference);
}

#endif
