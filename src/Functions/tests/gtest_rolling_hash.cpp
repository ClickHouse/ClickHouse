#include <gtest/gtest.h>

#include <Functions/FunctionsRollingHash.h>

#include <limits>
#include <string>
#include <vector>

using namespace DB;

TEST(Buzhash, Deterministic)
{
    const char data[] = "hello world";
    UInt64 h1 = BuzhashImpl::computeInitialHash(reinterpret_cast<const UInt8 *>(data), 4);
    UInt64 h2 = BuzhashImpl::computeInitialHash(reinterpret_cast<const UInt8 *>(data), 4);
    EXPECT_EQ(h1, h2);
}

TEST(Buzhash, RollingConsistency)
{
    const char data[] = "abcdefgh";
    size_t w = 4;

    UInt64 h_initial = BuzhashImpl::computeInitialHash(reinterpret_cast<const UInt8 *>(data), w);
    UInt64 h_rolled = h_initial;

    for (size_t pos = w; pos < 8; ++pos)
    {
        h_rolled = BuzhashImpl::roll(
            h_rolled,
            static_cast<UInt8>(data[pos - w]),
            static_cast<UInt8>(data[pos]),
            w);
    }

    UInt64 h_direct = BuzhashImpl::computeInitialHash(reinterpret_cast<const UInt8 *>(data + 4), w);
    EXPECT_EQ(h_rolled, h_direct);
}

TEST(Buzhash, SingleByteWindow)
{
    const char data[] = "x";
    UInt64 h = BuzhashImpl::computeInitialHash(reinterpret_cast<const UInt8 *>(data), 1);
    EXPECT_NE(0u, h);
}

namespace
{

std::string concatChunks(const std::vector<std::string> & chunks)
{
    std::string s;
    s.reserve(256);
    for (const auto & c : chunks)
        s.append(c);
    return s;
}

void runCdc(
    const std::string & input,
    size_t window_size,
    UInt64 reverse_probability,
    bool utf8_boundaries,
    bool collect_offsets,
    std::vector<std::string> & chunks,
    std::vector<UInt64> & offsets)
{
    chunks.clear();
    offsets.clear();
    RollingHashCDC::forEachContentDefinedChunk(
        reinterpret_cast<const UInt8 *>(input.data()),
        input.size(),
        window_size,
        reverse_probability,
        utf8_boundaries,
        collect_offsets,
        [&](const char * ptr, size_t len) { chunks.emplace_back(ptr, len); },
        [&](UInt64 o) { offsets.push_back(o); });
}

}

TEST(RollingHashCDC, EmptyInput)
{
    std::vector<std::string> chunks;
    std::vector<UInt64> offsets;
    runCdc("", 4, 1000, false, true, chunks, offsets);
    EXPECT_TRUE(chunks.empty());
    EXPECT_TRUE(offsets.empty());

    runCdc("", 4, 1000, false, false, chunks, offsets);
    EXPECT_TRUE(chunks.empty());
}

TEST(RollingHashCDC, ShorterThanWindowSingleChunk)
{
    std::vector<std::string> chunks;
    std::vector<UInt64> offsets;

    runCdc("ab", 4, 1000, false, true, chunks, offsets);
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(chunks[0], "ab");
    ASSERT_EQ(offsets.size(), 1u);
    EXPECT_EQ(offsets[0], 0u);
}

TEST(RollingHashCDC, ConcatEqualsOriginalByteMode)
{
    std::string input;
    input.resize(5000);
    for (size_t i = 0; i < input.size(); ++i)
        input[i] = static_cast<char>('a' + static_cast<int>(i % 26));

    std::vector<std::string> chunks;
    std::vector<UInt64> offsets;
    runCdc(input, 8, 997, false, false, chunks, offsets);
    ASSERT_FALSE(chunks.empty());
    EXPECT_EQ(concatChunks(chunks), input);
}

TEST(RollingHashCDC, ConcatEqualsOriginalUtf8Mode)
{
    const std::string input = "привет мир строка для cdc теста ";
    std::vector<std::string> chunks;
    std::vector<UInt64> offsets;
    runCdc(input, 4, 1000, true, false, chunks, offsets);
    ASSERT_FALSE(chunks.empty());
    EXPECT_EQ(concatChunks(chunks), input);
}

TEST(RollingHashCDC, OffsetsAlignWithChunks)
{
    const std::string input = "abcdefghijklmnop";
    std::vector<std::string> chunks;
    std::vector<UInt64> offsets;
    runCdc(input, 4, 1000, false, true, chunks, offsets);

    ASSERT_EQ(offsets.size(), chunks.size());
    for (size_t i = 0; i < chunks.size(); ++i)
    {
        EXPECT_EQ(offsets[i] + chunks[i].size(), (i + 1 < chunks.size() ? offsets[i + 1] : input.size()));
        EXPECT_EQ(input.substr(offsets[i], chunks[i].size()), chunks[i]);
    }
}

TEST(RollingHashCDC, Utf8ChunksStartOnCodePointBoundaries)
{
    const std::string input = "привет";
    std::vector<std::string> chunks;
    std::vector<UInt64> offsets;
    runCdc(input, 2, 1000, true, true, chunks, offsets);

    ASSERT_FALSE(chunks.empty());
    for (UInt64 start : offsets)
        EXPECT_TRUE(RollingHashCDC::isUtf8ChunkBoundary(
            reinterpret_cast<const UInt8 *>(input.data()), static_cast<size_t>(start), input.size()));
}

TEST(RollingHashCDC, MaxChunkSizeFloor)
{
    EXPECT_GE(RollingHashCDC::maxChunkSizeForCdc(2), 262144u);
}

TEST(RollingHashCDC, MaxChunkSizeSaturatesBeforeMultiplyOverflow)
{
    constexpr size_t max_cap = 256 * 1024 * 1024;
    EXPECT_EQ(RollingHashCDC::maxChunkSizeForCdc(std::numeric_limits<UInt64>::max()), max_cap);
    EXPECT_EQ(RollingHashCDC::maxChunkSizeForCdc(max_cap / 64 + 1), max_cap);
}

TEST(RollingHashCDC, ForceCutUtf8WhenTentativeEqualsDataSize)
{
    const std::string input = "hello";
    const UInt8 * data = reinterpret_cast<const UInt8 *>(input.data());
    size_t cut = RollingHashCDC::forceCutPositionUtf8(data, input.size(), 0, 100);
    EXPECT_EQ(cut, input.size());
}
