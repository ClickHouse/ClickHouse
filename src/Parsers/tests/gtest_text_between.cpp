#include <gtest/gtest.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/TokenIterator.h>

using namespace DB;

namespace
{

/// The text spanned by the whole query, which is what a parser gets when it records where it
/// started and where it ended up.
std::string spanOfWholeQuery(const std::string & query)
{
    Tokens tokens(query.data(), query.data() + query.size());
    TokenIterator begin(tokens);
    TokenIterator end = begin;
    while (end.isValid())
        ++end;
    return std::string(textBetween(begin, end));
}

/// What `CAST` and `defaultValueOfTypeName` store for a type written like this.
std::string typeText(const std::string & type)
{
    Tokens tokens(type.data(), type.data() + type.size());
    IParser::Pos pos(tokens, 1000, 1000);
    Expected expected;
    auto text = parseDataTypeAsText(pos, expected);
    return text ? *text : std::string("<no type>");
}

}

TEST(TextBetween, SkipsInsignificantTokensAtTheEdges)
{
    /// Leading and trailing whitespace and comments are not part of the span: the iterator does not
    /// stop on them, so the first and last positions are already significant tokens.
    EXPECT_EQ(spanOfWholeQuery("  SELECT 1  "), "SELECT 1");
    EXPECT_EQ(spanOfWholeQuery("/* c */ SELECT 1 -- trailing\n"), "SELECT 1");
    EXPECT_EQ(spanOfWholeQuery("\n\tSELECT\t1\n"), "SELECT\t1");
}

TEST(TextBetween, KeepsEverythingInTheMiddleVerbatim)
{
    /// Including whitespace and comments, which is why it is not a substitute for formatting.
    EXPECT_EQ(spanOfWholeQuery("SELECT   1"), "SELECT   1");
    EXPECT_EQ(spanOfWholeQuery("SELECT /* c */ 1"), "SELECT /* c */ 1");
    EXPECT_EQ(spanOfWholeQuery("SELECT\n    1"), "SELECT\n    1");
}

TEST(TextBetween, SingleToken)
{
    EXPECT_EQ(spanOfWholeQuery("SELECT"), "SELECT");
    EXPECT_EQ(spanOfWholeQuery("'a string'"), "'a string'");
}

TEST(ParseDataTypeAsText, IsTheFormattedSpellingAndNotTheSourceText)
{
    /// This text is stored in the query - `CAST(x, 'T')` - and from there in table metadata, so it
    /// has to be the canonical spelling however the type was written.
    EXPECT_EQ(typeText("UInt8"), "UInt8");
    EXPECT_EQ(typeText("Nullable(  UInt8  )"), "Nullable(UInt8)");
    EXPECT_EQ(typeText("Enum8\n  (\n    'hello' = 1,\n    'world' = 2\n  )"), "Enum8('hello' = 1, 'world' = 2)");
    EXPECT_EQ(typeText("Decimal(10,2)"), "Decimal(10, 2)");
    EXPECT_EQ(typeText("Array(/* comment */ String)"), "Array(String)");
    EXPECT_EQ(typeText("Tuple(x UInt8, y String)"), "Tuple(x UInt8, y String)");
    EXPECT_EQ(typeText("INT SIGNED"), "INT SIGNED");
}

TEST(ParseDataTypeAsText, NoTypeAtAll)
{
    EXPECT_EQ(typeText("'a string'"), "<no type>");
    EXPECT_EQ(typeText("123"), "<no type>");
    EXPECT_EQ(typeText(","), "<no type>");
    EXPECT_EQ(typeText(""), "<no type>");

    /// Any bare word is a type name here; whether it names a real type is settled later, by
    /// `DataTypeFactory`.
    EXPECT_EQ(typeText("NotAType"), "NotAType");
}
