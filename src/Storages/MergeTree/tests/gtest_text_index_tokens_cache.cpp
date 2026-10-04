#include <gtest/gtest.h>

#include <Storages/MergeTree/TextIndexCache.h>

using namespace DB;

TEST(TextIndexTokensCache, PatternBypassDoesNotAliasToken)
{
    const String index_id = "index";
    const String token = "pattern_bypass" + String(sizeof(UInt128) + sizeof(UInt64), '\0');

    EXPECT_NE(TextIndexTokensCache::hash(index_id, token), TextIndexTokensCache::hashPatternBypass(index_id, 0, 0));
}

TEST(TextIndexTokensCache, IndexAndTokenBoundaryIsUnambiguous)
{
    EXPECT_NE(TextIndexTokensCache::hash("part/skp_idx_a", "bc"), TextIndexTokensCache::hash("part/skp_idx_ab", "c"));
}
