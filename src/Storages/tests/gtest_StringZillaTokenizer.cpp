#include <Interpreters/ITokenizer.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace DB;

/// `stringLikeToTokens` tokenizes each literal of the pattern once, `nextInStringLike` returns one token per call.
/// Both must extract the same tokens.
TEST(StringZillaTokenizer, NextInStringLikeMatchesStringLikeToTokens)
{
    const std::vector<std::string> patterns = {
        "",
        "%%__",
        "abc",
        "%abc def ghi%",
        "a_b%c_d",
        "%,000 rows loaded%",
        "%1,000 rows,%",
        "%. bar baz .%",
        "foo bar'%",
        "a\\%b a\\_c",
        "x a\\\\%b y",
        "x y\\",
        "x caf\\\xC3\xA9 y",
        "%the quick brown\\_fox jumps over\\% the lazy dog%",
        "%\xE4\xBD\xA0\xE5\xA5\xBD%\xE4\xB8\x96\xE7\x95\x8C%",
        "%\xED\x95\x9C\xEA\xB5\xAD\xEC\x96\xB4 \xED\x85\x8D\xEC\x8A\xA4\xED\x8A\xB8%",
    };

    const StringZillaTokenizer tokenizer;
    for (const auto & pattern : patterns)
    {
        VectorWithMemoryTracking<String> expected;
        tokenizer.stringLikeToTokens(pattern.data(), pattern.size(), expected);

        std::vector<String> actual;
        size_t pos = 0;
        String token;
        while (pos < pattern.size() && tokenizer.nextInStringLike(pattern.data(), pattern.size(), pos, token))
            actual.push_back(token);

        EXPECT_EQ(std::vector<String>(expected.begin(), expected.end()), actual) << "pattern: " << pattern;
    }
}
