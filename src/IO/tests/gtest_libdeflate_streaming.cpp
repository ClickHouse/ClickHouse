#include "config.h"

#if USE_LIBDEFLATE

#include <IO/LibdeflateDeflatingWriteBuffer.h>
#include <IO/Libdeflate.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <IO/ReadHelpers.h>

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

using namespace DB;

namespace
{

std::string makeData(size_t size)
{
    std::string out;
    out.reserve(size);
    std::mt19937 rng(777); /// NOLINT(cert-msc32-c,cert-msc51-cpp) deterministic test data on purpose
    while (out.size() < size)
    {
        if (rng() % 2)
            out += "ClickHouse is a column-oriented database management system. ";
        else
            out.push_back(static_cast<char>(rng()));
    }
    out.resize(size);
    return out;
}

/// Compress with the streaming libdeflate writer, writing the input in small pieces so the
/// internal buffer fills up several times (i.e. multiple chunks within one member).
std::string compress(const std::string & data, CompressionMethod method, int level, size_t buf_size, size_t chunk)
{
    std::string compressed;
    {
        LibdeflateDeflatingWriteBuffer wb(std::make_unique<WriteBufferFromString>(compressed), method, level, buf_size);
        for (size_t pos = 0; pos < data.size(); pos += chunk)
            wb.write(data.data() + pos, std::min(chunk, data.size() - pos));
        wb.finalize();
    }
    return compressed;
}

}

class LibdeflateStreamingTest : public ::testing::TestWithParam<CompressionMethod> {};

TEST_P(LibdeflateStreamingTest, RoundTripViaZlibNg)
{
    const CompressionMethod method = GetParam();
    for (size_t size : {size_t(0), size_t(10), size_t(5000), size_t(200000)})
    {
        const std::string data = makeData(size);
        for (int level : {1, 6, 12})
        {
            const std::string c1 = compress(data, method, level, 1024, 100);     // tiny buffer -> many chunks
            const std::string c2 = compress(data, method, level, 1 << 16, 4096); // bigger buffer
            for (const auto & c : {c1, c2})
            {
                /// Decode with the independent zlib-ng decoder directly (not wrapReadBufferWithCompressionMethod,
                /// which dispatches back to libdeflate when USE_LIBDEFLATE is on), so this verifies the bytes the
                /// streaming writer produced against a different implementation.
                ZlibInflatingReadBuffer in(std::make_unique<ReadBufferFromString>(c), method);
                std::string restored;
                readStringUntilEOF(restored, in);
                EXPECT_EQ(restored, data) << "size=" << size << " level=" << level;
            }
        }
    }
}

/// libdeflate's one-shot decompressor decodes exactly ONE member, so a successful decode of the
/// whole output to the exact original size proves the writer produced a single member.
TEST_P(LibdeflateStreamingTest, OutputIsSingleMember)
{
    const CompressionMethod method = GetParam();
    for (size_t size : {size_t(1), size_t(5000), size_t(200000)})
    {
        const std::string data = makeData(size);
        const std::string c = compress(data, method, 6, 1024, 100); // force many chunks
        std::vector<char> restored(size);
        Libdeflate::decompress(method, c.data(), c.size(), restored.data(), size);
        EXPECT_EQ(std::string_view(restored.data(), size), std::string_view(data)) << "size=" << size;
    }
}

INSTANTIATE_TEST_SUITE_P(
    GzipAndZlib,
    LibdeflateStreamingTest,
    ::testing::Values(CompressionMethod::Gzip, CompressionMethod::Zlib));

#endif
