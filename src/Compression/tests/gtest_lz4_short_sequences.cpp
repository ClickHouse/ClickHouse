#include <Compression/LZ4_decompress_faster.h>

#include <lz4.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

namespace
{

void appendLength(std::vector<char> & block, size_t length)
{
    while (length >= 255)
    {
        block.push_back(static_cast<char>(255));
        length -= 255;
    }
    block.push_back(static_cast<char>(length));
}

void appendSequence(std::vector<char> & block, size_t literals, size_t match_length, unsigned offset)
{
    block.push_back(static_cast<char>((std::min(literals, size_t(15)) << 4) | std::min(match_length - 4, size_t(15))));
    if (literals >= 15)
        appendLength(block, literals - 15);
    for (size_t i = 0; i < literals; ++i)
        block.push_back(static_cast<char>('A' + i % 23));
    block.push_back(static_cast<char>(offset));
    block.push_back(static_cast<char>(offset >> 8));
    if (match_length >= 19)
        appendLength(block, match_length - 19);
}

std::vector<char> makeBlock(size_t literals, size_t match_length, unsigned offset)
{
    std::vector<char> block;
    appendSequence(block, 1024, 8, 8);
    appendSequence(block, literals, match_length, offset);
    block.push_back(static_cast<char>(15 << 4));
    appendLength(block, 64 - 15);
    block.insert(block.end(), 64, 'z');
    return block;
}

bool decode(const std::vector<char> & block, size_t source_size, size_t output_size, std::vector<char> & output)
{
    std::vector<char> source(block);
    source.resize(source.size() + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER, '\xA5');
    output.assign(output_size + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER + 32, '\xCD');
    LZ4::PerformanceStatistics statistics;
    const bool success = LZ4::decompress(source.data(), output.data(), source_size, output_size, statistics);
    EXPECT_TRUE(std::all_of(
        output.begin() + output_size + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER,
        output.end(),
        [](char value)
        {
            return static_cast<unsigned char>(value) == 0xCD;
        }));
    output.resize(output_size);
    return success;
}

}

TEST(LZ4ShortSequences, LiteralMatchPairsAndOverlapBoundaries)
{
    for (size_t literals = 0; literals <= 16; ++literals)
    {
        for (size_t match_length = 4; match_length <= 20; ++match_length)
        {
            for (unsigned offset : {1U, 7U, 8U, 9U, 15U, 16U, 17U, 32U})
            {
                const auto block = makeBlock(literals, match_length, offset);
                const size_t output_size = 1024 + 8 + literals + match_length + 64;
                ASSERT_GE(output_size, size_t(1024));
                ASSERT_LT(output_size, LZ4::MIN_BLOCK_SIZE_TO_CHOOSE_VARIANT);
                ASSERT_GE(block.size(), output_size / 2);
                std::vector<char> expected(output_size);
                ASSERT_EQ(
                    LZ4_decompress_safe(
                        block.data(), expected.data(), static_cast<int>(block.size()), static_cast<int>(output_size)),
                    static_cast<int>(output_size));
                std::vector<char> output;
                ASSERT_TRUE(decode(block, block.size(), output_size, output));
                EXPECT_EQ(output, expected);
            }
        }
    }
}

TEST(LZ4ShortSequences, TruncatedPayloadAndOutputMargins)
{
    const auto block = makeBlock(14, 18, 8);
    const size_t output_size = 1024 + 8 + 14 + 18 + 64;
    std::vector<char> output;
    for (size_t source_size = 0; source_size < block.size(); ++source_size)
        EXPECT_FALSE(decode(block, source_size, output_size, output)) << source_size;
    for (size_t remaining : {31, 32})
        EXPECT_FALSE(decode(block, block.size(), 1024 + 8 + remaining, output));
    EXPECT_FALSE(decode(block, block.size(), output_size - 1, output));
    for (unsigned offset : {0U, 65535U})
    {
        const auto invalid = makeBlock(14, 18, offset);
        EXPECT_FALSE(decode(invalid, invalid.size(), output_size, output));
    }
}
