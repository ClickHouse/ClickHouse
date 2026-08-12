#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeArray.h>

#include <Common/assert_cast.h>
#include <Common/FailPoint.h>

#include <gtest/gtest.h>

#include <base/scope_guard.h>

namespace DB
{
namespace FailPoints
{
extern const char json_shared_regexp_force_combined_compile_failure[];
}

namespace
{

using MatchMode = JSONPathRegexpMatchMode;

DataTypePtr makeJSONType(std::vector<JSONPathRegexpRule> rules, String prefix = {})
{
    return std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{},
        std::unordered_set<String>{},
        std::vector<String>{},
        DataTypeObject::DEFAULT_MAX_DYNAMIC_PATHS,
        DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES,
        std::move(rules),
        std::move(prefix));
}

DataTypePtr makeJSONTypeWithTypedPath(std::vector<JSONPathRegexpRule> rules, DataTypePtr nested_type)
{
    return std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{{"typed", std::move(nested_type)}},
        std::unordered_set<String>{},
        std::vector<String>{},
        DataTypeObject::DEFAULT_MAX_DYNAMIC_PATHS,
        DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES,
        std::move(rules));
}

std::vector<JSONPathRegexpRule> makeRules(std::string_view prefix, size_t first, size_t count)
{
    std::vector<JSONPathRegexpRule> rules;
    rules.reserve(count);
    for (size_t i = first; i != first + count; ++i)
        rules.push_back({String{prefix} + std::to_string(i), MatchMode::Full});
    return rules;
}

const DataTypeObject & asJSON(const DataTypePtr & type)
{
    return assert_cast<const DataTypeObject &>(*type);
}

void expectProvenanceTop(const DataTypePtr & type)
{
    const auto & json = asJSON(type);
    ASSERT_EQ(json.getSharedDataPathRules().size(), 1);
    EXPECT_EQ(json.getSharedDataPathRules().front(), (JSONPathRegexpRule{"(?s:.*)", MatchMode::Full}));
    EXPECT_TRUE(json.getSharedDataPathPrefix().empty());

    const auto & matcher = json.getSharedDataPathMatcher();
    ASSERT_TRUE(matcher);
    EXPECT_TRUE(matcher->matches("ordinary.path"));
    EXPECT_TRUE(matcher->matches("path\nwith-newline"));
}

TEST(DataTypeObjectSharedRegexp, ProvenanceJoinIsCanonicalAndAlgebraicWithinLimits)
{
    const auto first = makeJSONType({{"first", MatchMode::Partial}, {"common", MatchMode::Full}}, "root.");
    const auto second = makeJSONType({{"second", MatchMode::Full}, {"common", MatchMode::Full}}, "root.");
    const auto third = makeJSONType({{"third", MatchMode::Partial}}, "root.");

    const auto first_second = mergeJSONSharedDataPathRules(first, second);
    EXPECT_EQ(asJSON(first_second).getSharedDataPathRules().size(), 3);
    EXPECT_EQ(asJSON(first_second).getSharedDataPathPrefix(), "root.");
    EXPECT_TRUE(first_second->equals(*mergeJSONSharedDataPathRules(second, first)));
    EXPECT_EQ(mergeJSONSharedDataPathRules(first, first).get(), first.get());
    EXPECT_EQ(mergeJSONSharedDataPathRules(first_second, first).get(), first_second.get());

    const auto left_associative = mergeJSONSharedDataPathRules(first_second, third);
    const auto right_associative = mergeJSONSharedDataPathRules(first, mergeJSONSharedDataPathRules(second, third));
    EXPECT_TRUE(left_associative->equals(*right_associative));
}

TEST(DataTypeObjectSharedRegexp, IdempotentJoinPreservesPointerIdentityThroughTypedPathContainers)
{
    const auto first = makeJSONTypeWithTypedPath(
        {{"root_first", MatchMode::Partial}},
        std::make_shared<DataTypeArray>(makeJSONType({{"nested_first", MatchMode::Partial}})));
    const auto second = makeJSONTypeWithTypedPath(
        {{"root_second", MatchMode::Partial}},
        std::make_shared<DataTypeArray>(makeJSONType({{"nested_second", MatchMode::Partial}})));

    const auto joined = mergeJSONSharedDataPathRules(first, second);
    EXPECT_EQ(mergeJSONSharedDataPathRules(joined, first).get(), joined.get());
}

TEST(DataTypeObjectSharedRegexp, ProvenanceJoinSaturatesAtRuleLimitAfterDeduplication)
{
    const auto first = makeJSONType(makeRules("^path_", 0, JSONPathRegexpMatcher::MAX_RULES / 2));
    const auto exact_limit = makeJSONType(makeRules(
        "^path_", JSONPathRegexpMatcher::MAX_RULES / 2, JSONPathRegexpMatcher::MAX_RULES / 2));

    const auto exact = mergeJSONSharedDataPathRules(first, exact_limit);
    EXPECT_EQ(asJSON(exact).getSharedDataPathRules().size(), JSONPathRegexpMatcher::MAX_RULES);

    auto overlapping_rules = makeRules("^path_", 0, JSONPathRegexpMatcher::MAX_RULES / 2);
    overlapping_rules.push_back({"^one_more$", MatchMode::Full});
    expectProvenanceTop(mergeJSONSharedDataPathRules(exact, makeJSONType(std::move(overlapping_rules))));
}

TEST(DataTypeObjectSharedRegexp, ProvenanceJoinSaturatesAtTotalByteLimit)
{
    constexpr size_t rules_per_side = 17;
    constexpr size_t repeated_characters = 32 * 1024;

    auto make_large_rules = [](std::string_view suffix)
    {
        std::vector<JSONPathRegexpRule> rules;
        rules.reserve(rules_per_side);
        for (size_t i = 0; i != rules_per_side; ++i)
        {
            /// Repeated characters in a character class keep the compiled regexp small while
            /// exercising the persisted-pattern byte bound.
            rules.push_back({
                "[" + String(repeated_characters, 'a') + "]" + String{suffix} + std::to_string(i),
                MatchMode::Full});
        }
        return rules;
    };

    const auto first = makeJSONType(make_large_rules("left"));
    const auto second = makeJSONType(make_large_rules("right"));
    expectProvenanceTop(mergeJSONSharedDataPathRules(first, second));
}

TEST(DataTypeObjectSharedRegexp, ProvenanceJoinSaturatesWhenCombinedMatcherCannotCompile)
{
    const auto first = makeJSONType({{"first", MatchMode::Full}});
    const auto second = makeJSONType({{"second", MatchMode::Full}});

    /// `RE2`'s exact compiled-set size depends on its version and target architecture. Exercise the
    /// internal provenance fallback deterministically instead of relying on patterns that happen to
    /// exceed its private memory accounting on one build.
    FailPointInjection::enableFailPoint(FailPoints::json_shared_regexp_force_combined_compile_failure);
    SCOPE_EXIT({ FailPointInjection::disableFailPoint(FailPoints::json_shared_regexp_force_combined_compile_failure); });
    expectProvenanceTop(mergeJSONSharedDataPathRules(first, second));
}

TEST(DataTypeObjectSharedRegexp, ProvenanceJoinSaturatesForDifferentRootPrefixes)
{
    const auto first = makeJSONType({{"^left[.]path$", MatchMode::Full}}, "left.");
    const auto second = makeJSONType({{"^right[.]path$", MatchMode::Full}}, "right.");

    const auto saturated = mergeJSONSharedDataPathRules(first, second);
    expectProvenanceTop(saturated);
    EXPECT_TRUE(saturated->equals(*mergeJSONSharedDataPathRules(second, first)));
    EXPECT_EQ(mergeJSONSharedDataPathRules(saturated, first).get(), saturated.get());
    EXPECT_TRUE(saturated->equals(*mergeJSONSharedDataPathRules(makeJSONType({}), saturated)));
}

}
}
