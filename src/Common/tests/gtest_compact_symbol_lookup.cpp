#include <Common/CompactSymbolLookup.h>
#include <base/defines.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

namespace DB
{
namespace
{

/// A step of a linear congruential generator: the multiplication and the addition are modulo 2^64.
NO_SANITIZE_UNSIGNED_OVERFLOW uint64_t nextRandom(uint64_t state)
{
    return state * 6364136223846793005ULL + 1442695040888963407ULL;
}

/// A section produced by the encoder and the names in name index order, which is the sorted order.
struct TestTable
{
    TestTable(const std::string & prefix, size_t count)
    {
        for (size_t index = 0; index < count; ++index)
            names.push_back(prefix + std::to_string(index * 7919 % 100003) + std::string(index % 13, 'x'));

        std::vector<CompactSymbols::Symbol> symbols;
        for (size_t index = 0; index < names.size(); ++index)
            symbols.push_back({0x1000 + index * 16, 16, names[index]});
        encoded = CompactSymbols::encode(symbols);
        std::sort(names.begin(), names.end());
        table = std::make_unique<CompactSymbolTable>(std::string_view(encoded.data(), encoded.size()));
    }

    std::vector<std::string> names;
    std::vector<char> encoded;
    std::unique_ptr<CompactSymbolTable> table;
};

class CompactSymbolLookupTest : public ::testing::Test
{
protected:
    /// Each table spans several granules.
    TestTable first{"_ZN2DB5first", 1000};
    TestTable second{"_ZN2DB6second", 700};
    CompactNameWorkspace workspace;
    UInt64 now = 1000;

    [[nodiscard]] size_t maximumNameSize() const
    {
        return std::max(first.table->reader.maximumNameSize(), second.table->reader.maximumNameSize());
    }

    [[nodiscard]] size_t maximumGranuleSize() const
    {
        return std::max(first.table->reader.maximumNameGranuleSize(), second.table->reader.maximumNameGranuleSize());
    }

    [[nodiscard]] uint32_t sourceId(const TestTable & table) const { return &table == &first ? 0 : 1; }

    /// Looks the name up as the server does and checks it against the expected one.
    void expectName(const TestTable & table, uint32_t name_index)
    {
        std::string_view name = workspace.getName(*table.table, sourceId(table), name_index, now, maximumNameSize(), maximumGranuleSize());
        ASSERT_EQ(name, table.names.at(name_index)) << "name index " << name_index;
        /// The terminator is right after the view.
        ASSERT_EQ(*(name.data() + name.size()), '\0');
    }

    static bool isCached(const TestTable & table, uint32_t name_index, UInt64 now_seconds)
    {
        std::string buffer(table.table->reader.maximumNameSize() + 1, '\0');
        return table.table->name_cache.tryGet(name_index, now_seconds, buffer.data(), buffer.size()).has_value();
    }

    /// Repeats misses of the name, as lookups of a recurring frame do, until the cache admits it.
    void makeCached(const TestTable & table, uint32_t name_index, uint32_t other_index)
    {
        for (size_t attempt = 0; attempt < 100 && !isCached(table, name_index, now); ++attempt)
        {
            expectName(table, name_index);
            expectName(table, other_index);
        }
        ASSERT_TRUE(isCached(table, name_index, now));
    }
};

TEST_F(CompactSymbolLookupTest, HitThenMissInTheSameGranule)
{
    makeCached(first, 5, 900);
    ASSERT_FALSE(isCached(first, 7, now));

    expectName(first, 3);
    /// A hit overwrites the name the decoder would continue from.
    expectName(first, 5);
    expectName(first, 7);
    expectName(first, 6);
    expectName(first, 8);
    expectName(first, 5);
    expectName(first, 4);
}

TEST_F(CompactSymbolLookupTest, MissThenHitThenTheSameName)
{
    makeCached(first, 5, 900);

    expectName(first, 200);
    expectName(first, 5);
    expectName(first, 5);
    expectName(first, 201);
    expectName(first, 201);
    expectName(first, 5);
    expectName(first, 202);
}

TEST_F(CompactSymbolLookupTest, DescendingEntriesInAGranule)
{
    for (uint32_t entry : {127u, 100u, 64u, 63u, 1u, 0u})
        expectName(first, 2 * CompactSymbols::names_per_granule + entry);
}

TEST_F(CompactSymbolLookupTest, TwoTablesAlternately)
{
    makeCached(second, 10, 600);

    for (size_t round = 0; round < 3; ++round)
    {
        expectName(first, 10);
        expectName(second, 10);
        expectName(first, 11);
        expectName(second, 11);
        expectName(first, 10);
        expectName(second, 300);
    }

    uint64_t state = 1;
    for (size_t step = 0; step < 20000; ++step)
    {
        state = nextRandom(state);
        const TestTable & table = (state >> 63) ? first : second;
        /// A small working set, so that names get cached and hits mix with misses.
        auto name_index = static_cast<uint32_t>((state >> 20) % 64 * 11 % table.names.size());
        if (step % 1000 == 0)
            ++now;
        expectName(table, name_index);
    }
    EXPECT_GT(first.table->name_cache.size() + second.table->name_cache.size(), 0);
}

TEST_F(CompactSymbolLookupTest, ReadOnlyLooksUpButNeverInserts)
{
    workspace.prepare(maximumNameSize(), maximumGranuleSize());
    workspace.setNameCacheReadOnly();

    for (size_t round = 0; round < 5; ++round)
        for (uint32_t name_index : {1u, 500u, 2u, 999u, 1u})
            expectName(first, name_index);

    EXPECT_EQ(first.table->name_cache.size(), 0);
    EXPECT_EQ(first.table->name_cache.capacity(), 0);
}

}
}
