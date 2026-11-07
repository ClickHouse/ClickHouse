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
#endif
