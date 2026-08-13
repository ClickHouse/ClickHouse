#include <Storages/MergeTree/JSONBloomPathMatcher.h>

#include <gtest/gtest.h>

namespace DB
{
namespace
{

TEST(JSONBloomPathMatcher, ExactPathsAndRegularExpressions)
{
    const JSONBloomPathMatcher matcher(
        {"request_id", "payload.raw"},
        {"^metadata\\.", "trace_id$"});

    EXPECT_TRUE(matcher.shouldSkip("request_id"));
    EXPECT_TRUE(matcher.shouldSkip("payload.raw.secret"));
    EXPECT_TRUE(matcher.shouldSkip("payload.raw[0]"));
    EXPECT_TRUE(matcher.shouldSkip(std::string_view("payload.raw\0map-key", 19)));
    EXPECT_TRUE(matcher.shouldSkip("metadata.trace"));
    EXPECT_TRUE(matcher.shouldSkip("other.trace_id"));

    EXPECT_FALSE(matcher.shouldSkip("request"));
    EXPECT_FALSE(matcher.shouldSkip("payload.rawness"));
    EXPECT_FALSE(matcher.shouldSkip("payload.kept"));
    EXPECT_FALSE(matcher.shouldSkip("meta"));
}

}
}
