#include "config.h"

#if USE_LIBDEFLATE

#include <IO/Libdeflate.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ZlibDeflatingWriteBuffer.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// A mix of compressible (repetitive) and incompressible (random) data.
std::string makeData(size_t size)
{
    std::string out;
    out.reserve(size);
    std::mt19937 rng(12345); /// NOLINT(cert-msc32-c,cert-msc51-cpp) deterministic test data on purpose
    while (out.size() < size)
    {
        if (rng() % 3 == 0)
            out += "the quick brown fox jumps over the lazy dog 0123456789 ";
        else
            out.push_back(static_cast<char>(rng()));
    }
    out.resize(size);
    return out;
}

}

class LibdeflateTest : public ::testing::TestWithParam<CompressionMethod> {};

TEST_P(LibdeflateTest, RoundTrip)
{
    const CompressionMethod method = GetParam();
    for (size_t size : {size_t(0), size_t(1), size_t(100), size_t(65536), size_t(1'000'003)})
    {
        const std::string data = makeData(size);
        for (int level : {1, 3, 6, 9, 12})
        {
            std::vector<char> compressed(Libdeflate::compressBound(method, level, size));
            const size_t csize = Libdeflate::compress(method, level, data.data(), size, compressed.data(), compressed.size());
            ASSERT_GT(csize, 0u) << "size=" << size << " level=" << level;

            std::vector<char> restored(size);
            Libdeflate::decompress(method, compressed.data(), csize, restored.data(), size);
            ASSERT_EQ(std::string_view(restored.data(), size), std::string_view(data.data(), size))
                << "size=" << size << " level=" << level;
        }
    }
}

/// libdeflate must decode what zlib-ng produced and vice versa (format interchange).
/// Uses ClickHouse's zlib-ng-backed buffers directly (NOT wrap*WithCompressionMethod, which dispatches
/// back to libdeflate when USE_LIBDEFLATE is on) so the check is against an independent encoder/decoder.
TEST_P(LibdeflateTest, CrossCompatibleWithZlibNg)
{
    const CompressionMethod method = GetParam();
    const std::string data = makeData(500'000);

    /// zlib-ng compress -> libdeflate decompress
    {
        std::string compressed;
        {
            ZlibDeflatingWriteBuffer wb(std::make_unique<WriteBufferFromString>(compressed), method, 6);
            wb.write(data.data(), data.size());
            wb.finalize();
        }
        std::vector<char> restored(data.size());
        Libdeflate::decompress(method, compressed.data(), compressed.size(), restored.data(), data.size());
        ASSERT_EQ(std::string_view(restored.data(), data.size()), std::string_view(data));
    }

    /// libdeflate compress -> zlib-ng decompress
    {
        std::vector<char> buf(Libdeflate::compressBound(method, 6, data.size()));
        const size_t csize = Libdeflate::compress(method, 6, data.data(), data.size(), buf.data(), buf.size());
        const std::string compressed(buf.data(), csize);

        ZlibInflatingReadBuffer rb(std::make_unique<ReadBufferFromString>(compressed), method);
        std::string restored;
        readStringUntilEOF(restored, rb);
        ASSERT_EQ(restored, data);
    }
}

TEST_P(LibdeflateTest, MalformedInputThrows)
{
    const CompressionMethod method = GetParam();
    const std::string garbage = "this is definitely not a valid compressed stream";
    std::vector<char> out(1024);
    EXPECT_THROW(
        Libdeflate::decompress(method, garbage.data(), garbage.size(), out.data(), out.size()),
        Exception);
}

TEST_P(LibdeflateTest, WrongUncompressedSizeThrows)
{
    const CompressionMethod method = GetParam();
    const std::string data = makeData(10'000);
    std::vector<char> compressed(Libdeflate::compressBound(method, 6, data.size()));
    const size_t csize = Libdeflate::compress(method, 6, data.data(), data.size(), compressed.data(), compressed.size());

    std::vector<char> too_small(data.size() / 2);
    EXPECT_THROW(
        Libdeflate::decompress(method, compressed.data(), csize, too_small.data(), too_small.size()),
        Exception);
}

INSTANTIATE_TEST_SUITE_P(
    GzipAndZlib,
    LibdeflateTest,
    ::testing::Values(CompressionMethod::Gzip, CompressionMethod::Zlib));

#endif
