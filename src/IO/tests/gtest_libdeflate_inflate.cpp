#include "config.h"

#if USE_LIBDEFLATE

#include <IO/LibdeflateInflatingReadBuffer.h>
#include <IO/ZlibDeflatingWriteBuffer.h>
#include <IO/Libdeflate.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <gtest/gtest.h>

#include <memory>
#include <random>
#include <string>
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

#endif
