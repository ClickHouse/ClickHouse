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
        for (size_t chunk : {size_t(1), size_t(13), size_t(4096), compressed.size() ? compressed.size() : size_t(1)})
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

INSTANTIATE_TEST_SUITE_P(
    GzipAndZlib,
    LibdeflateInflateTest,
    ::testing::Values(CompressionMethod::Gzip, CompressionMethod::Zlib));

#endif
