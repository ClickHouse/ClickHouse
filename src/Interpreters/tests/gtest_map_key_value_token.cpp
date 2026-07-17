#include <gtest/gtest.h>
#include <Interpreters/MapKeyValueToken.h>

#include <string>
#include <utility>
#include <vector>

using namespace DB;

TEST(MapKeyValueToken, RoundTrip)
{
    for (const auto & [k, v] : std::vector<std::pair<std::string, std::string>>{
             {"foo", "bar"}, {"", ""}, {"k", ""}, {"", "v"},
             {"a=b", "c"}, {std::string(200, 'x'), "y"}})
    {
        auto token = encodeMapKeyValueToken(k, v);
        auto [dk, dv] = decodeMapKeyValueToken(token);
        EXPECT_EQ(std::string(dk), k);
        EXPECT_EQ(std::string(dv), v);
    }
}

TEST(MapKeyValueToken, DecodeDegenerateTokensAreSafe)
{
    /// Empty, single-byte and short/malformed tokens must not read past the buffer, throw, or
    /// return views outside the token.
    for (const std::string & token : {std::string(""), std::string(1, '\0'), std::string("x"),
                                      std::string("\x80", 1), std::string("\xff\xff\xff", 3)})
    {
        auto [key, value] = decodeMapKeyValueToken(token);
        EXPECT_LE(key.size() + value.size(), token.size());
    }
}

TEST(MapKeyValueToken, DistinctPairsNeverCollide)
{
    /// The ambiguous case the separator approach fails: (fo, obar) vs (foo, bar).
    EXPECT_NE(encodeMapKeyValueToken("fo", "obar"), encodeMapKeyValueToken("foo", "bar"));
    /// Values containing the classic '=' separator must not collide either.
    EXPECT_NE(encodeMapKeyValueToken("a", "b=c"), encodeMapKeyValueToken("a=b", "c"));
}
