#include <DataTypes/JSONPathRegexpMatcher.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

namespace DB
{
namespace
{

using MatchMode = JSONPathRegexpMatchMode;

TEST(JSONPathRegexpMatcher, EmptyMatcherMatchesNothing)
{
    const auto matcher = JSONPathRegexpMatcher::create({});

    ASSERT_NE(matcher, nullptr);
    EXPECT_TRUE(matcher->empty());
    EXPECT_TRUE(matcher->getRules().empty());
    EXPECT_FALSE(matcher->matches(""));
    EXPECT_FALSE(matcher->matches("some.path"));
}

TEST(JSONPathRegexpMatcher, PartialMatchIsUnanchored)
{
    const auto matcher = JSONPathRegexpMatcher::create({{R"(metrics\.[^.]+)", MatchMode::Partial}});

    EXPECT_TRUE(matcher->matches("metrics.cpu"));
    EXPECT_TRUE(matcher->matches("prefix.metrics.cpu.suffix"));
    EXPECT_FALSE(matcher->matches("metrics."));
    EXPECT_FALSE(matcher->matches("metric.cpu"));
}

TEST(JSONPathRegexpMatcher, FullMatchIsAnchoredAtBothEnds)
{
    const auto matcher = JSONPathRegexpMatcher::create({{R"(metrics\.[^.]+)", MatchMode::Full}});

    EXPECT_TRUE(matcher->matches("metrics.cpu"));
    EXPECT_FALSE(matcher->matches("prefix.metrics.cpu"));
    EXPECT_FALSE(matcher->matches("metrics.cpu.suffix"));
}

TEST(JSONPathRegexpMatcher, MixedMatchModesUseEitherRuleSet)
{
    const auto matcher = JSONPathRegexpMatcher::create({
        {R"((^|\.)secret(\.|$))", MatchMode::Partial},
        {R"(root\.fixed)", MatchMode::Full},
    });

    EXPECT_TRUE(matcher->matches("app.secret.token"));
    EXPECT_TRUE(matcher->matches("root.fixed"));
    EXPECT_FALSE(matcher->matches("prefix.root.fixed"));
    EXPECT_FALSE(matcher->matches("app.not_secret.token"));
}

TEST(JSONPathRegexpMatcher, RulesAreSortedAndDeduplicatedByPatternAndMode)
{
    const auto matcher = JSONPathRegexpMatcher::create({
        {"z", MatchMode::Full},
        {"a", MatchMode::Partial},
        {"z", MatchMode::Full},
        {"a", MatchMode::Full},
        {"a", MatchMode::Partial},
    });

    const auto & rules = matcher->getRules();
    ASSERT_EQ(rules.size(), 3);
    EXPECT_EQ(rules[0], (JSONPathRegexpRule{"a", MatchMode::Partial}));
    EXPECT_EQ(rules[1], (JSONPathRegexpRule{"a", MatchMode::Full}));
    EXPECT_EQ(rules[2], (JSONPathRegexpRule{"z", MatchMode::Full}));
}

TEST(JSONPathRegexpMatcher, InvalidRulesAreRejected)
{
    EXPECT_THROW(JSONPathRegexpMatcher::create(std::vector<JSONPathRegexpRule>{{"", MatchMode::Partial}}), Exception);
    EXPECT_THROW(JSONPathRegexpMatcher::create(std::vector<JSONPathRegexpRule>{{"(", MatchMode::Partial}}), Exception);
    EXPECT_THROW(JSONPathRegexpMatcher::create(std::vector<JSONPathRegexpRule>{{"valid", static_cast<MatchMode>(255)}}), Exception);
}

TEST(JSONPathRegexpMatcher, SizeLimitsAreEnforcedBeforeDeduplication)
{
    std::vector<JSONPathRegexpRule> too_many_rules(JSONPathRegexpMatcher::MAX_RULES + 1, JSONPathRegexpRule{"a", MatchMode::Partial});
    EXPECT_THROW(JSONPathRegexpMatcher::create(std::move(too_many_rules)), Exception);

    String oversized_pattern(JSONPathRegexpMatcher::MAX_PATTERN_BYTES + 1, 'a');
    EXPECT_THROW(
        JSONPathRegexpMatcher::create(std::vector<JSONPathRegexpRule>{{std::move(oversized_pattern), MatchMode::Partial}}), Exception);

    const String maximum_pattern = "[" + String(JSONPathRegexpMatcher::MAX_PATTERN_BYTES - 2, 'a') + "]";
    std::vector<JSONPathRegexpRule> too_many_pattern_bytes(
        JSONPathRegexpMatcher::MAX_TOTAL_PATTERN_BYTES / JSONPathRegexpMatcher::MAX_PATTERN_BYTES + 1,
        JSONPathRegexpRule{maximum_pattern, MatchMode::Partial});
    EXPECT_THROW(JSONPathRegexpMatcher::create(std::move(too_many_pattern_bytes)), Exception);
}

TEST(JSONPathRegexpMatcher, BoundaryLimitsAreAccepted)
{
    const String maximum_pattern = "[" + String(JSONPathRegexpMatcher::MAX_PATTERN_BYTES - 2, 'a') + "]";
    const auto maximum_pattern_matcher = JSONPathRegexpMatcher::create({{maximum_pattern, MatchMode::Partial}});
    EXPECT_EQ(maximum_pattern_matcher->getRules().front().pattern.size(), JSONPathRegexpMatcher::MAX_PATTERN_BYTES);
    EXPECT_TRUE(maximum_pattern_matcher->matches("a"));

    const String pattern = "[" + String(JSONPathRegexpMatcher::MAX_TOTAL_PATTERN_BYTES / JSONPathRegexpMatcher::MAX_RULES - 2, 'a') + "]";
    std::vector<JSONPathRegexpRule> rules(JSONPathRegexpMatcher::MAX_RULES, JSONPathRegexpRule{pattern, MatchMode::Partial});

    const auto matcher = JSONPathRegexpMatcher::create(std::move(rules));

    ASSERT_EQ(matcher->getRules().size(), 1);
    EXPECT_EQ(
        matcher->getRules().front().pattern.size(), JSONPathRegexpMatcher::MAX_TOTAL_PATTERN_BYTES / JSONPathRegexpMatcher::MAX_RULES);
    EXPECT_TRUE(matcher->matches("a"));
}

}
}
