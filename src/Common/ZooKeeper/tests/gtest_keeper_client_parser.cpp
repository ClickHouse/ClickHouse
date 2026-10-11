#include <gtest/gtest.h>

#include <Common/ZooKeeper/KeeperClientCLI/KeeperClient.h>
#include <Common/ZooKeeper/KeeperClientCLI/Parser.h>
#include <Parsers/Lexer.h>

#include <optional>
#include <string>
#include <vector>

using namespace DB;

namespace
{

std::optional<String> parseArg(const String & text)
{
    Tokens tokens(text.data(), text.data() + text.size(), 0, /*skip_insignificant=*/false);
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    Expected expected;
    String result;
    if (!parseKeeperArg(pos, expected, result))
        return std::nullopt;
    return result;
}

}

TEST(KeeperClientParser, ParseKeeperArgBasic)
{
    EXPECT_EQ(parseArg("/foo"), "/foo");
    EXPECT_EQ(parseArg("/foo\\ bar"), "/foo bar");
    EXPECT_EQ(parseArg("\"/foo bar\""), "/foo bar");
    EXPECT_EQ(parseArg("'/foo bar'"), "/foo bar");
    EXPECT_EQ(parseArg("/dir/\"sub dir\""), "/dir/sub dir");
    EXPECT_EQ(parseArg("/space\\ node"), "/space node");
    EXPECT_EQ(parseArg("/root/'dq\"node'"), "/root/dq\"node");
    EXPECT_EQ(parseArg("/a\\\\b"), "/a\\b");
    EXPECT_EQ(parseArg("/foo\\x"), "/foo\\x");
}

TEST(KeeperClientParser, ParseKeeperArgStopsOnNonBare)
{
    /// Bare mode only consumes BareWord / Slash / Dot / Number / Minus.
    EXPECT_EQ(parseArg("/foo;bar"), "/foo");
    EXPECT_EQ(parseArg("/foo(bar)"), "/foo");
    EXPECT_EQ(parseArg("/foo bar"), "/foo");
}

TEST(KeeperClientParser, ParseKeeperArgRejectsUnclosedQuote)
{
    EXPECT_EQ(parseArg("'/unclosed"), std::nullopt);
    EXPECT_EQ(parseArg("\"/unclosed"), std::nullopt);
    /// Content before a failed quote is kept; the failed segment is not appended.
    EXPECT_EQ(parseArg("/ok/'still open"), "/ok/");
}

TEST(KeeperClientParser, FormatKeeperNodeNameRoundTrip)
{
    const std::vector<String> names = {
        "plain",
        "space node",
        "back\\slash",
        "dq\"node",
        "semi;colon",
        "a-b.c_1",
    };

    for (const auto & name : names)
    {
        const String escaped = formatKeeperNodeName(name);
        auto parsed = parseArg(escaped);
        ASSERT_TRUE(parsed.has_value()) << "failed to parse " << escaped;
        EXPECT_EQ(*parsed, name) << "escaped as " << escaped;
    }
}
