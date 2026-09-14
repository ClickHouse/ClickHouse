#include <Common/StringUtils.h>

#include <array>
#include <vector>

#include <gtest/gtest.h>


namespace
{
const std::array<size_t, 31> boundary_sizes = {
    0, 1, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129,
    255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 4095, 4096, 4097,
    16383, 16384, 16385};

const std::array<size_t, 11> avx512_sizes = {
    16384, 16385, 16447, 16448, 16449, 32767, 32768, 32769, 65535, 65536, 65537};
const std::array<size_t, 6> alignment_offsets = {1, 7, 15, 31, 32, 63};

std::vector<UInt8> makeASCIIData(size_t size)
{
    std::vector<UInt8> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<UInt8>(i & 0x7F);
    return data;
}
}

TEST(StringUtils, IsAllASCIIAcceptsEmptyInput)
{
    EXPECT_TRUE(isAllASCII(nullptr, 0));

    const std::vector<UInt8> empty;
    EXPECT_TRUE(isAllASCII(empty.data(), empty.size()));
}

TEST(StringUtils, IsAllASCIIAcceptsAllASCIIBytes)
{
    for (const size_t size : boundary_sizes)
    {
        const auto data = makeASCIIData(size);
        EXPECT_TRUE(isAllASCII(data.data(), data.size())) << "size: " << size;
    }
}

TEST(StringUtils, IsAllASCIIHandlesUnalignedData)
{
    for (const size_t size : boundary_sizes)
    {
        std::vector<UInt8> storage(size + 1);
        UInt8 * data = storage.data() + 1;
        for (size_t i = 0; i < size; ++i)
            data[i] = static_cast<UInt8>(i & 0x7F);

        EXPECT_TRUE(isAllASCII(data, size)) << "size: " << size;

        if (size == 0)
            continue;

        data[size / 2] = 0x80;
        EXPECT_FALSE(isAllASCII(data, size)) << "size: " << size;
    }
}

TEST(StringUtils, IsAllASCIIHandlesAVX512TailAndAlignmentBoundaries)
{
    const std::array<UInt8, 2> non_ascii_bytes = {0x80, 0xFF};

    for (const size_t size : avx512_sizes)
    {
        for (const size_t offset : alignment_offsets)
        {
            std::vector<UInt8> storage(size + offset);
            UInt8 * data = storage.data() + offset;
            for (size_t i = 0; i < size; ++i)
                data[i] = static_cast<UInt8>(i & 0x7F);

            EXPECT_TRUE(isAllASCII(data, size)) << "size: " << size << ", offset: " << offset;

            const std::array<size_t, 3> positions = {0, size / 2, size - 1};
            for (const size_t position : positions)
            {
                for (const UInt8 byte : non_ascii_bytes)
                {
                    data[position] = byte;
                    EXPECT_FALSE(isAllASCII(data, size))
                        << "size: " << size << ", offset: " << offset << ", position: " << position
                        << ", byte: " << static_cast<unsigned>(byte);
                    data[position] = static_cast<UInt8>(position & 0x7F);
                }
            }
        }
    }
}

TEST(StringUtils, IsAllASCIIRejectsHighBitBytesAtEveryPosition)
{
    const std::array<UInt8, 6> non_ascii_bytes = {0x80, 0x81, 0xC2, 0xE2, 0xF0, 0xFF};

    for (const size_t size : boundary_sizes)
    {
        auto data = makeASCIIData(size);
        for (size_t position = 0; position < size; ++position)
        {
            for (const UInt8 byte : non_ascii_bytes)
            {
                data[position] = byte;
                EXPECT_FALSE(isAllASCII(data.data(), data.size()))
                    << "size: " << size << ", position: " << position << ", byte: " << static_cast<unsigned>(byte);
                data[position] = static_cast<UInt8>(position & 0x7F);
            }
        }
    }
}
