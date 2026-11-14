#include <base/StringViewHash.h>
#include <gtest/gtest.h>
#include <array>
#include <string>
#include <utility>

#if defined(CRC_INT)
TEST(CRC32Hash, PreservesLongStringHashValues)
{
    constexpr std::array<std::pair<size_t, UInt64>, 13> cases{{
        {8, 0xf4c5e18af4c5e18aULL},
        {9, 0xbd8022efbd8022efULL},
        {16, 0x39e16a7839e16a78ULL},
        {31, 0x9ed0098b9ed0098bULL},
        {1000, 0x1265d25d1265d25dULL},
        {1023, 0xc5bca5f6c5bca5f6ULL},
        {1024, 0x395eb258395eb258ULL},
        {1025, 0xc56eeee7c56eeee7ULL},
        {1031, 0xc7669f24c7669f24ULL},
        {1032, 0x9dfb364c9dfb364cULL},
        {1033, 0x702d965b702d965bULL},
        {4096, 0x75bc6d7375bc6d73ULL},
        {65536, 0x45245eac45245eacULL},
    }};
    for (const auto & [size, expected] : cases)
    {
        std::string input(size, '\0');
        for (size_t i = 0; i < size; ++i)
            input[i] = static_cast<char>((i * 29 + 17) & 255);
        EXPECT_EQ(CRC32Hash{}(input), expected) << "size=" << size;
    }
}

TEST(CRC32Hash, PreservesPrefixAndBulkBoundaryValues)
{
    constexpr std::array<std::pair<size_t, UInt64>, 15> cases{{
        {32, 0xb285e1bbb285e1bbULL},
        {33, 0xf20290f4f20290f4ULL},
        {63, 0x3b20322b3b20322bULL},
        {64, 0xa7cd04d4a7cd04d4ULL},
        {65, 0xd53f6bffd53f6bffULL},
        {4095, 0x895e7add895e7addULL},
        {4097, 0x4f5c33c34f5c33c3ULL},
        {8191, 0xb793f71bb793f71bULL},
        {8192, 0x4b71e0b54b71e0b5ULL},
        {8193, 0x4add1ab94add1ab9ULL},
        {16383, 0x864dc553864dc553ULL},
        {16384, 0x7aafd2fd7aafd2fdULL},
        {16385, 0x14041d5a14041d5aULL},
        {65535, 0xb9c64902b9c64902ULL},
        {65537, 0x6cc421cf6cc421cfULL},
    }};
    for (const auto & [size, expected] : cases)
    {
        std::string input(size, '\0');
        for (size_t i = 0; i < size; ++i)
            input[i] = static_cast<char>((i * 29 + 17) & 255);
        EXPECT_EQ(CRC32Hash{}(input), expected) << "size=" << size;
    }
}

TEST(CRC32Hash, PreservesIndependentFoldingLanes)
{
    constexpr std::array<std::pair<size_t, UInt64>, 5> cases{{
        {16384, 0x71ba239371ba2393ULL},
        {16385, 0xcf9f1d17cf9f1d17ULL},
        {32769, 0x066d7b7a066d7b7aULL},
        {65537, 0x6ca541a86ca541a8ULL},
        {131079, 0x3fc5499f3fc5499fULL},
    }};
    for (const auto & [size, expected] : cases)
    {
        std::string input(size, '\0');
        UInt64 state = 0x243f6a8885a308d3ULL;
        for (char & byte : input)
        {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            byte = static_cast<char>(state >> 56);
        }
        EXPECT_EQ(CRC32Hash{}(input), expected) << "size=" << size;
    }
}
#endif
