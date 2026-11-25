#include <Common/SymbolNamePreference.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

namespace DB
{
namespace
{

TEST(SymbolNamePreference, Precedence)
{
    /// Fewer leading underscores first, whatever the size.
    EXPECT_TRUE(isPreferredSymbolName("memcpy", "_ZN11__llvm_libc6memcpyEPvPKvm"));
    EXPECT_TRUE(isPreferredSymbolName("newlocale", "__newlocale"));
    EXPECT_TRUE(isPreferredSymbolName("a_very_long_name_without_underscores_in_front", "_x"));
    EXPECT_TRUE(isPreferredSymbolName("_a", "__"));

    /// Then the shorter one, whatever the bytes.
    EXPECT_TRUE(isPreferredSymbolName("zz", "aaa"));
    EXPECT_TRUE(isPreferredSymbolName("_Zzz", "_Zaaa"));

    /// Then the smaller in unsigned byte order: a byte of 0x80 or more is greater than any ASCII byte.
    EXPECT_TRUE(isPreferredSymbolName("ab", "ba"));
    EXPECT_TRUE(isPreferredSymbolName("a\x7f", "a\x80"));
    EXPECT_TRUE(isPreferredSymbolName("az", "a\xff"));
}

TEST(SymbolNamePreference, UnderscoreChain)
{
    const std::array<std::string_view, 4> expected{"environ", "_environ", "__environ", "___environ"};
    for (size_t preferred = 0; preferred < expected.size(); ++preferred)
    {
        for (size_t other = preferred + 1; other < expected.size(); ++other)
        {
            EXPECT_TRUE(isPreferredSymbolName(expected[preferred], expected[other])) << expected[preferred] << " " << expected[other];
            EXPECT_FALSE(isPreferredSymbolName(expected[other], expected[preferred])) << expected[other] << " " << expected[preferred];
        }
    }
}

TEST(SymbolNamePreference, StrictTotalOrder)
{
    const std::vector<std::string_view> names{
        "", "_", "__", "a", "b", "_a", "__a", "ab", "ba", "a\x80", "a\x7f", "\x80", "memcpy", "_memcpy", "memcpz", "x_"};

    for (auto lhs : names)
    {
        EXPECT_FALSE(isPreferredSymbolName(lhs, lhs)) << lhs;
        for (auto rhs : names)
        {
            if (lhs != rhs)
                EXPECT_NE(isPreferredSymbolName(lhs, rhs), isPreferredSymbolName(rhs, lhs)) << lhs << " " << rhs;
            for (auto third : names)
            {
                if (isPreferredSymbolName(lhs, rhs) && isPreferredSymbolName(rhs, third))
                    EXPECT_TRUE(isPreferredSymbolName(lhs, third)) << lhs << " " << rhs << " " << third;
            }
        }
    }
}

struct Entry
{
    uint64_t begin;
    uint64_t end;
    std::string name;

    bool operator==(const Entry &) const = default;
};

std::vector<Entry> keepOnePerRange(std::vector<Entry> entries)
{
    std::sort(entries.begin(), entries.end(), [](const Entry & lhs, const Entry & rhs)
    {
        return lhs.begin != rhs.begin ? lhs.begin < rhs.begin : lhs.end < rhs.end;
    });
    entries.erase(
        keepPreferredPerRange(
            entries.begin(),
            entries.end(),
            [](const Entry & lhs, const Entry & rhs) { return lhs.begin == rhs.begin && lhs.end == rhs.end; },
            [](const Entry & lhs, const Entry & rhs) { return isPreferredSymbolName(lhs.name, rhs.name); }),
        entries.end());
    return entries;
}

TEST(SymbolNamePreference, KeepsThePreferredNameOfEachRangeInAnyOrder)
{
    std::vector<Entry> entries{
        {0x10, 0x20, "__environ"},
        {0x10, 0x20, "environ"},
        {0x10, 0x20, "_environ"},
        {0x10, 0x20, "___environ"},
        {0x10, 0x30, "wider"},
        {0x40, 0x50, "single"},
        {0x60, 0x70, "_ZN11__llvm_libc6memcpyEPvPKvm"},
        {0x60, 0x70, "memcpy"},
    };
    const std::vector<Entry> expected{
        {0x10, 0x20, "environ"},
        {0x10, 0x30, "wider"},
        {0x40, 0x50, "single"},
        {0x60, 0x70, "memcpy"},
    };

    std::sort(entries.begin(), entries.end(), [](const Entry & lhs, const Entry & rhs) { return lhs.name < rhs.name; });
    size_t permutations = 0;
    do
    {
        ASSERT_EQ(keepOnePerRange(entries), expected) << "permutation " << permutations;
        ++permutations;
    } while (std::next_permutation(entries.begin(), entries.end(), [](const Entry & lhs, const Entry & rhs) { return lhs.name < rhs.name; }));
    EXPECT_EQ(permutations, 40320);
}

}
}
