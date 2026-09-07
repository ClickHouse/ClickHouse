#include <gtest/gtest.h>

#include <Parsers/LiteralTokenInfo.h>

#include <random>
#include <unordered_map>
#include <vector>

using namespace DB;

namespace
{

/// The map is keyed by `ASTLiteral` addresses but never dereferences them, so the tests can use
/// made-up aligned addresses rather than build real literals.
const ASTLiteral * fakeLiteral(uintptr_t n)
{
    return reinterpret_cast<const ASTLiteral *>((n + 1) * alignof(void *));
}

LiteralTokenInfo someTokenInfo(uintptr_t n)
{
    return LiteralTokenInfo{reinterpret_cast<const char *>(n * 2 + 1), reinterpret_cast<const char *>(n * 2 + 2)};
}

}

TEST(LiteralTokenMap, EmptyMapFindsNothing)
{
    LiteralTokenMap map;
    EXPECT_EQ(map.find(fakeLiteral(0)), nullptr);
}

TEST(LiteralTokenMap, FindsWhatWasInserted)
{
    LiteralTokenMap map;
    map.insert_or_assign(fakeLiteral(1), someTokenInfo(1));

    const auto * found = map.find(fakeLiteral(1));
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->begin, someTokenInfo(1).begin);
    EXPECT_EQ(found->end, someTokenInfo(1).end);

    EXPECT_EQ(map.find(fakeLiteral(2)), nullptr);
}

TEST(LiteralTokenMap, InsertOverwrites)
{
    /// Nested literals can reuse the address of a discarded node, so the last write must win.
    LiteralTokenMap map;
    map.insert_or_assign(fakeLiteral(7), someTokenInfo(1));
    map.insert_or_assign(fakeLiteral(7), someTokenInfo(2));

    const auto * found = map.find(fakeLiteral(7));
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->begin, someTokenInfo(2).begin);
    EXPECT_EQ(found->end, someTokenInfo(2).end);
}

TEST(LiteralTokenMap, ForgetHidesAnEntry)
{
    /// A parser that discards a subtree forgets the literals in it, so that a literal created at a
    /// reused address does not inherit their token positions.
    LiteralTokenMap map;
    map.insert_or_assign(fakeLiteral(3), someTokenInfo(1));
    map.forget(fakeLiteral(3));
    EXPECT_EQ(map.find(fakeLiteral(3)), nullptr);

    /// Forgetting a literal that was never recorded is allowed, and neighbours are unaffected.
    map.insert_or_assign(fakeLiteral(4), someTokenInfo(2));
    map.forget(fakeLiteral(5));
    EXPECT_EQ(map.find(fakeLiteral(5)), nullptr);
    ASSERT_NE(map.find(fakeLiteral(4)), nullptr);
    EXPECT_EQ(map.find(fakeLiteral(4))->begin, someTokenInfo(2).begin);

    /// And a forgotten address can be recorded again.
    map.insert_or_assign(fakeLiteral(3), someTokenInfo(9));
    ASSERT_NE(map.find(fakeLiteral(3)), nullptr);
    EXPECT_EQ(map.find(fakeLiteral(3))->begin, someTokenInfo(9).begin);
}

TEST(LiteralTokenMap, GrowsBeyondInlineCapacity)
{
    /// Well past the inline storage, so the table rehashes onto the heap more than once.
    constexpr size_t count = 1000;
    LiteralTokenMap map;
    for (size_t i = 0; i < count; ++i)
        map.insert_or_assign(fakeLiteral(i), someTokenInfo(i));

    for (size_t i = 0; i < count; ++i)
    {
        const auto * found = map.find(fakeLiteral(i));
        ASSERT_NE(found, nullptr) << "missing entry " << i;
        EXPECT_EQ(found->begin, someTokenInfo(i).begin);
    }
    EXPECT_EQ(map.find(fakeLiteral(count)), nullptr);
}

TEST(LiteralTokenMap, AgreesWithUnorderedMap)
{
    std::mt19937_64 rng(12345); /// NOLINT(cert-msc32-c,cert-msc51-cpp) deterministic seed, so a failure is reproducible

    for (int round = 0; round < 500; ++round)
    {
        LiteralTokenMap map;
        std::unordered_map<const ASTLiteral *, LiteralTokenInfo> reference;
        std::vector<const ASTLiteral *> keys;

        size_t inserts = rng() % 400;
        for (size_t i = 0; i < inserts; ++i)
        {
            const ASTLiteral * key = nullptr;
            if (!keys.empty() && rng() % 4 == 0)
                key = keys[rng() % keys.size()];    /// exercise overwriting
            else
            {
                key = fakeLiteral(rng() % 100000);
                keys.push_back(key);
            }

            auto value = someTokenInfo(rng());
            map.insert_or_assign(key, value);
            reference.insert_or_assign(key, value);
        }

        for (const auto * key : keys)
        {
            const auto * found = map.find(key);
            auto it = reference.find(key);
            ASSERT_NE(found, nullptr);
            ASSERT_NE(it, reference.end());
            EXPECT_EQ(found->begin, it->second.begin);
            EXPECT_EQ(found->end, it->second.end);
        }

        for (int i = 0; i < 50; ++i)
        {
            const auto * absent = fakeLiteral(100000 + rng() % 100000);
            if (!reference.contains(absent))
                EXPECT_EQ(map.find(absent), nullptr);
        }
    }
}
