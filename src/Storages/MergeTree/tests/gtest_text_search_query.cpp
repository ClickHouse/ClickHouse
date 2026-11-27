#include <gtest/gtest.h>

#include <Common/OptimizedRegularExpression.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>

using namespace DB;

namespace
{

UInt128 hashOfPattern(const String & pattern)
{
    std::vector<OptimizedRegularExpression> patterns;
    patterns.emplace_back(pattern);

    const TextSearchQuery query("like", TextSearchMode::Any, TextIndexDirectReadMode::None, {}, std::move(patterns));
    return query.getHash();
}

}

/// The hash is the key of the text index query cache, so two patterns which select different tokens must not
/// share it. An anchored literal compiles no re2 pattern, which is what the hash uses for every other regexp.
TEST(TextSearchQuery, hashDistinguishesAnchoredLiterals)
{
    const UInt128 prefix = hashOfPattern("^lit");
    const UInt128 suffix = hashOfPattern("lit$");
    const UInt128 exact = hashOfPattern("^lit$");
    const UInt128 substring = hashOfPattern("lit");

    EXPECT_NE(prefix, suffix);
    EXPECT_NE(prefix, exact);
    EXPECT_NE(prefix, substring);
    EXPECT_NE(suffix, exact);
    EXPECT_NE(suffix, substring);
    EXPECT_NE(exact, substring);

    /// The same pattern still hashes the same, otherwise the cache would never hit.
    EXPECT_EQ(prefix, hashOfPattern("^lit"));
}
