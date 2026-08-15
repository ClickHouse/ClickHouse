#include <Storages/MergeTree/JSONBloomPathMatcher.h>

#include <gtest/gtest.h>

namespace DB
{
namespace
{

TEST(JSONBloomPathMatcher, ExactPathsAndRegularExpressions)
{
    const JSONBloomPathMatcher matcher(
        {},
        {},
        {"request_id", "payload.raw"},
        {"^metadata\\.", "trace_id$"});

    EXPECT_FALSE(matcher.shouldIndex("request_id"));
    EXPECT_FALSE(matcher.shouldIndex("payload.raw.secret"));
    EXPECT_FALSE(matcher.shouldIndex("payload.raw[0]"));
    EXPECT_FALSE(matcher.shouldIndex(std::string_view("payload.raw\0map-key", 19)));
    EXPECT_FALSE(matcher.shouldIndex("metadata.trace"));
    EXPECT_FALSE(matcher.shouldIndex("other.trace_id"));

    EXPECT_TRUE(matcher.shouldIndex("request"));
    EXPECT_TRUE(matcher.shouldIndex("payload.rawness"));
    EXPECT_TRUE(matcher.shouldIndex("payload.kept"));
    EXPECT_TRUE(matcher.shouldIndex("meta"));
}

TEST(JSONBloomPathMatcher, ExactIncludesVisitAncestors)
{
    const JSONBloomPathMatcher matcher(
        {"items.id", "payload-other", "payload.ids", "request_id", "request_id"},
        {},
        {},
        {});

    EXPECT_TRUE(matcher.shouldIndex("request_id"));
    EXPECT_TRUE(matcher.shouldIndex("payload.ids.primary"));
    EXPECT_TRUE(matcher.shouldIndex("items.id"));
    EXPECT_FALSE(matcher.shouldIndex("request"));
    EXPECT_FALSE(matcher.shouldIndex("payload.identity"));
    EXPECT_FALSE(matcher.shouldIndex("items.name"));
    EXPECT_TRUE(matcher.shouldIndex("payload-other"));

    EXPECT_TRUE(matcher.shouldVisit("payload"));
    EXPECT_TRUE(matcher.shouldVisit("payload.ids"));
    EXPECT_TRUE(matcher.shouldVisit("items"));
    EXPECT_TRUE(matcher.shouldVisit("items.id"));
    EXPECT_FALSE(matcher.shouldVisit("message"));
}

TEST(JSONBloomPathMatcher, RegexpIncludesAndSkipsWin)
{
    const JSONBloomPathMatcher matcher(
        {"payload", "payload!"},
        {"(?:^|\\.)(?:.*_id|.*_at)$"},
        {"payload.secret"},
        {"private_"});

    EXPECT_TRUE(matcher.shouldIndex("request_id"));
    EXPECT_TRUE(matcher.shouldIndex("nested.created_at"));
    EXPECT_TRUE(matcher.shouldIndex("payload.message"));
    EXPECT_FALSE(matcher.shouldIndex("message"));
    EXPECT_FALSE(matcher.shouldIndex("payload.secret"));
    EXPECT_FALSE(matcher.shouldIndex("payload.secret.value"));
    EXPECT_FALSE(matcher.shouldIndex("private_id"));

    EXPECT_TRUE(matcher.shouldVisit("unmatched_parent"));
    EXPECT_FALSE(matcher.shouldVisit("payload.secret"));

    const JSONBloomPathMatcher ancestor_regexp_matcher({}, {}, {}, {"^container$"});
    EXPECT_FALSE(ancestor_regexp_matcher.shouldIndex("container"));
    EXPECT_TRUE(ancestor_regexp_matcher.shouldVisit("container"));
    EXPECT_TRUE(ancestor_regexp_matcher.shouldIndex("container.value"));
}

}
}
