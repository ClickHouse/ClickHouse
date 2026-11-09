#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include <Interpreters/AdaptiveAggregation/GrowingCountTopK.h>

using namespace DB;

namespace
{

/// The tops under test take limits up to 16; only the split of a word into count and index bits depends on it.
template <typename Key>
using TopK = GrowingCountTopK<Key, 16>;

/// The keys and counts of the top's groups, ordered by key.
template <typename Key>
std::vector<std::pair<Key, UInt64>> sortedGroups(const TopK<Key> & top)
{
    std::vector<std::pair<Key, UInt64>> groups;
    for (const auto & group : top.getGroups())
        groups.emplace_back(group.key, group.count);
    std::ranges::sort(groups);
    return groups;
}

}

/// The caller keeps every group's count in the low bits of its word and passes the word after each growth.
TEST(GrowingCountTopK, KeepsTheLargestCounts)
{
    TopK<UInt64> top(2);
    UInt64 a = 0;
    UInt64 b = 0;
    UInt64 c = 0;
    const auto grow = [&](UInt64 & word, UInt64 key, UInt64 rows)
    {
        word += rows;
        top.update<false>(word, key, key);
    };
    grow(a, 1, 1);
    grow(b, 2, 1);
    /// Equal to the smallest count of the full top, so it does not enter.
    grow(c, 3, 1);
    grow(c, 3, 2);
    grow(a, 1, 1);

    EXPECT_EQ(sortedGroups(top), (std::vector<std::pair<UInt64, UInt64>>{{1, 2}, {3, 3}}));
}

/// A group evicted from the top keeps the index of its entry in its word, and the entry goes to another group: the
/// top confirms an entry by the key, so the evicted group enters anew instead of growing the other group's count.
TEST(GrowingCountTopK, ConfirmsAStaleEntryByItsKey)
{
    TopK<UInt64> top(1);
    UInt64 a = 1;
    top.update<false>(a, UInt64{1}, 1);
    UInt64 b = 2;
    top.update<false>(b, UInt64{2}, 2);
    a += 2;
    top.update<false>(a, UInt64{1}, 1);

    EXPECT_EQ(sortedGroups(top), (std::vector<std::pair<UInt64, UInt64>>{{1, 3}}));
}

/// A key that views a block read back from a spill comes with `copy_key`: the top keeps a copy of it for a new entry,
/// so the entry is still found by its key after the next block overwrote the first.
TEST(GrowingCountTopK, KeepsACopyOfAStringKey)
{
    TopK<std::string_view> top(2);
    std::string block = "alpha";
    UInt64 word = 1;
    top.update<true>(word, block, 7);
    block = "omega";
    const std::string next_block = "alpha";
    ++word;
    top.update<true>(word, next_block, 7);

    EXPECT_EQ(sortedGroups(top), (std::vector<std::pair<std::string_view, UInt64>>{{"alpha", 2}}));
}

/// The same for a packed string key long enough to keep its bytes out of line.
TEST(GrowingCountTopK, KeepsACopyOfAPackedStringKey)
{
    /// `PackedStringRef::build` reads whole words, so the strings keep readable bytes past their ends.
    const auto pack = [](const std::string & padded, size_t length)
    { return PackedStringRef::build(padded.data(), length, PackedStringRefHash{}); };
    const std::string key(40, 'k');
    const std::string padding(8, '\0');

    TopK<PackedStringRef> top(2);
    std::string block = key + padding;
    UInt64 word = 1;
    top.update<true>(word, pack(block, key.size()), 7);
    std::ranges::fill(block, 'x');
    const std::string next_block = key + padding;
    ++word;
    top.update<true>(word, pack(next_block, key.size()), 7);

    ASSERT_EQ(top.getGroups().size(), 1);
    EXPECT_EQ(static_cast<std::string_view>(top.getGroups()[0].key), key);
    EXPECT_EQ(top.getGroups()[0].count, 2);
}

/// Groups that overtake each other leave the top and enter it again, A, B, B, A, A with one entry: each change copies
/// the new leader's key over the copy of the group it evicts, so the copies never outnumber the entries.
TEST(GrowingCountTopK, ReusesTheCopyOfAnEntry)
{
    TopK<std::string_view> top(1);
    std::string block;
    UInt64 a = 0;
    UInt64 b = 0;
    const auto grow = [&](UInt64 & word, const std::string & key)
    {
        block = key;
        ++word;
        top.update<true>(word, block, 0);
        /// The next block overwrites the bytes that the key viewed.
        std::ranges::fill(block, 'x');
    };
    grow(a, "alpha");
    const char * copy = top.getGroups()[0].key.data();
    grow(b, "omega");
    grow(b, "omega");
    grow(a, "alpha");
    grow(a, "alpha");

    EXPECT_EQ(sortedGroups(top), (std::vector<std::pair<std::string_view, UInt64>>{{"alpha", 3}}));
    EXPECT_EQ(top.getGroups()[0].key.data(), copy);
}
