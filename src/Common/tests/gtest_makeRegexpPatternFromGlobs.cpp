#include <Common/parseGlobs.h>
#include <Common/re2.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(Common, BetterGlob)
{
    // Smoke tests.
    auto s = BetterGlob::GlobString("123");

    EXPECT_EQ(s.getExpressions().size(), 1);
    EXPECT_EQ(s.getExpressions().front().type(), BetterGlob::ExpressionType::CONSTANT);

    s = BetterGlob::GlobString("123{123,456,789,234,567,890}");

    EXPECT_EQ(s.getExpressions().size(), 2);
    EXPECT_EQ(s.getExpressions().front().type(), BetterGlob::ExpressionType::CONSTANT);
    EXPECT_EQ(s.getExpressions().back().type(), BetterGlob::ExpressionType::ENUM);
    EXPECT_EQ(s.getExpressions().back().cardinality(), 6);

    s = BetterGlob::GlobString("123{123}{12..23}");

    EXPECT_EQ(s.getExpressions().size(), 3);
    EXPECT_EQ(s.getExpressions().front().type(), BetterGlob::ExpressionType::CONSTANT);
    EXPECT_EQ(s.getExpressions().back().type(), BetterGlob::ExpressionType::RANGE);

    s = BetterGlob::GlobString("123{12..23}{1223}");

    EXPECT_EQ(s.getExpressions().size(), 3);
    EXPECT_EQ(s.getExpressions().front().type(), BetterGlob::ExpressionType::CONSTANT);
    EXPECT_EQ(s.getExpressions().back().type(), BetterGlob::ExpressionType::ENUM);

    // Range tests.
    //
    BetterGlob::Range r;
    s = BetterGlob::GlobString("f{1..9}");

    EXPECT_EQ(s.getExpressions().size(), 2);
    EXPECT_EQ(s.getExpressions().back().type(), BetterGlob::ExpressionType::RANGE);

    r = std::get<BetterGlob::Range>(s.getExpressions().back().getData());
    EXPECT_EQ(r.start, 1);
    EXPECT_EQ(r.end, 9);

    s = BetterGlob::GlobString("f{0..10}");
    EXPECT_EQ(s.getExpressions().size(), 2);
    EXPECT_EQ(s.getExpressions().back().type(), BetterGlob::ExpressionType::RANGE);

    r = std::get<BetterGlob::Range>(s.getExpressions().back().getData());
    EXPECT_EQ(r.start, 0);
    EXPECT_EQ(r.end, 10);

    s = BetterGlob::GlobString("f{10..20}");
    EXPECT_EQ(s.getExpressions().size(), 2);
    EXPECT_EQ(s.getExpressions().back().type(), BetterGlob::ExpressionType::RANGE);

    r = std::get<BetterGlob::Range>(s.getExpressions().back().getData());
    EXPECT_EQ(r.start, 10);
    EXPECT_EQ(r.end, 20);

    s = BetterGlob::GlobString("f{00..10}");
    EXPECT_EQ(s.getExpressions().size(), 2);
    EXPECT_EQ(s.getExpressions().back().type(), BetterGlob::ExpressionType::RANGE);

    r = std::get<BetterGlob::Range>(s.getExpressions().back().getData());
    EXPECT_EQ(r.start, 0);
    EXPECT_EQ(r.end, 10);

    EXPECT_EQ(r.start_digit_count, 2);
    EXPECT_EQ(r.end_digit_count, 2);

    s = BetterGlob::GlobString("f{9..000}");
    EXPECT_EQ(s.getExpressions().size(), 2);
    EXPECT_EQ(s.getExpressions().back().type(), BetterGlob::ExpressionType::RANGE);
    EXPECT_EQ(s.getExpressions().back().dump(), "{9..000}");

    r = std::get<BetterGlob::Range>(s.getExpressions().back().getData());
    EXPECT_EQ(r.start, 9);
    EXPECT_EQ(r.end, 0);

    EXPECT_EQ(r.start_digit_count, 1);
    EXPECT_EQ(r.end_digit_count, 3);
}

class BetterGlobEchoTest : public ::testing::TestWithParam<std::string> {};
class BetterGlobRegexTest : public ::testing::TestWithParam<std::pair<std::string, std::string>> {};

TEST_P(BetterGlobEchoTest, EchoTest)
{
    const auto & glob = GetParam();
    EXPECT_EQ(BetterGlob::GlobString(glob).dump(), glob);
}

INSTANTIATE_TEST_SUITE_P(
    Common,
    BetterGlobEchoTest,
    ::testing::Values(
        // Basic constant
        "a",
        "aaa",
        "file.txt",
        "/path/to/file",
        "?",
        "*",
        "/?",

        // Simple enum
        "{test}",
        "{a,b}",
        "f{hello,world,one,two,three}",
        "f{a,b,c,d,e,f,g,h}",
        "f{test.tar.gz}",

        // Single-element range
        "{5..5}",
        "{0..0}",
        "{00..00}",

        // Ascending numeric ranges
        "{1..9}",
        "{0..10}",
        "{10..20}",
        "{95..103}",
        "{99..109}",

        // Descending numeric ranges
        "{9..1}",
        "{20..15}",
        "{200..199}",
        "{103..95}",

        // Zero-padded ranges
        "{00..10}",
        "{000..9}",
        "{01..9}",
        "{001..0009}",
        "{0009..0001}",  // descending with padding
        "{9..000}",      // mixed digit count

        // Mixed content
        "f{1..9}",
        "file{1..5}",
        "log{2023..2025}.txt",
        "data{00..99}.csv",
        "backup_{1..3}{a..c}",

        // Multiple braces
        "{1..2}{a..b}",
        "{1..1}{2..2}",
        "{0..0}{0..0}",
        "f{1..2}{1..2}",
        "{a,b}{1,2}",
        "prefix{a,b}middle{1,2}suffix",

        // Complex patterns
        "*_{{a,b,c,d}}/?.csv",
        "{1,2,3}blabla{a.x,b.x,c.x}smth[]_else{aa,bb}?*"
    )
);

TEST_P(BetterGlobRegexTest, RegexTest)
{
    const auto & [glob, regex] = GetParam();
    EXPECT_EQ(BetterGlob::GlobString(glob).asRegex(), regex);
}

INSTANTIATE_TEST_SUITE_P(
    Common,
    BetterGlobRegexTest,
    ::testing::Values(
        std::make_pair("?", "[^/]"),
        std::make_pair("*", "[^/]*"),
        std::make_pair("/?", "/[^/]"),
        std::make_pair("/*", "/[^/]*"),
        std::make_pair("{123}", "(123)"),
        std::make_pair("{test}", "(test)"),
        std::make_pair("{test.tar.gz}", "(test\\.tar\\.gz)"),
        // std::make_pair("*_{{a,b,c,d}}/?.csv", "[^/]*_\\{(a|b|c|d)\\}/[^/]\\.csv")

        std::make_pair("f{1..9}", "f(1|2|3|4|5|6|7|8|9)"),
        std::make_pair("f{0..10}", "f(0|1|2|3|4|5|6|7|8|9|10)"),
        std::make_pair("f{10..20}", "f(10|11|12|13|14|15|16|17|18|19|20)"),
        std::make_pair("f{00..10}", "f(00|01|02|03|04|05|06|07|08|09|10)"),
        std::make_pair("f{0001..0009}", "f(0001|0002|0003|0004|0005|0006|0007|0008|0009)"),
        std::make_pair("f{01..9}", "f(01|02|03|04|05|06|07|08|09)"),
        std::make_pair("f{000..9}", "f(000|001|002|003|004|005|006|007|008|009)"),
        std::make_pair("f{95..103}", "f(95|96|97|98|99|100|101|102|103)"),
        std::make_pair("f{99..109}", "f(99|100|101|102|103|104|105|106|107|108|109)"),
        std::make_pair("f{001..0009}", "f(0001|0002|0003|0004|0005|0006|0007|0008|0009)"),

        std::make_pair("f{20..15}", "f(15|16|17|18|19|20)"),
        std::make_pair("f{200..199}", "f(199|200)"),
        std::make_pair("f{0009..0001}", "f(0001|0002|0003|0004|0005|0006|0007|0008|0009)"),
        std::make_pair("f{100..90}", "f(90|91|92|93|94|95|96|97|98|99|100)"),
        std::make_pair("f{103..95}", "f(95|96|97|98|99|100|101|102|103)"),
        std::make_pair("f{9..01}", "f(01|02|03|04|05|06|07|08|09)"),
        std::make_pair("f{9..000}", "f(000|001|002|003|004|005|006|007|008|009)"),
        std::make_pair("f{1..2}{1..2}", "f(1|2)(1|2)"),
        std::make_pair("f{1..1}{1..1}", "f(1)(1)"),
        std::make_pair("f{0..0}{0..0}", "f(0)(0)"),
        std::make_pair("file{1..5}", "file(1|2|3|4|5)"),
        std::make_pair("file{1,2,3}", "file(1|2|3)"),
        std::make_pair("{1,2,3}blabla{a.x,b.x,c.x}smth[]_else{aa,bb}?*", "(1|2|3)blabla(a\\.x|b\\.x|c\\.x)smth\\[\\]_else(aa|bb)[^/][^/]*")
    )
);

TEST(Common, makeRegexpPatternFromGlobs)
{
    EXPECT_EQ(makeRegexpPatternFromGlobs("?"), "[^/]");

    EXPECT_EQ(makeRegexpPatternFromGlobs("?"), "[^/]");
    EXPECT_EQ(makeRegexpPatternFromGlobs("*"), "[^/]*");
    EXPECT_EQ(makeRegexpPatternFromGlobs("/?"), "/[^/]");
    EXPECT_EQ(makeRegexpPatternFromGlobs("/*"), "/[^/]*");
    EXPECT_EQ(makeRegexpPatternFromGlobs("{123}"), "(123)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("{test}"), "(test)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("{test.tar.gz}"), "(test\\.tar\\.gz)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("*_{{a,b,c,d}}/?.csv"), "[^/]*_\\{(a|b|c|d)\\}/[^/]\\.csv");
    /* Regex Parsing for {..} can have three possible cases
       1) The left range width == the right range width
       2) The left range width > the right range width
       3) The left range width < the right range width
    */
    // Ascending Sequences
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{1..9}"), "f(1|2|3|4|5|6|7|8|9)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{0..10}"), "f(0|1|2|3|4|5|6|7|8|9|10)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{10..20}"), "f(10|11|12|13|14|15|16|17|18|19|20)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{00..10}"), "f(00|01|02|03|04|05|06|07|08|09|10)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{0001..0009}"), "f(0001|0002|0003|0004|0005|0006|0007|0008|0009)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{01..9}"), "f(01|02|03|04|05|06|07|08|09)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{000..9}"), "f(000|001|002|003|004|005|006|007|008|009)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{95..103}"), "f(95|96|97|98|99|100|101|102|103)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{99..109}"), "f(99|100|101|102|103|104|105|106|107|108|109)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{001..0009}"), "f(0001|0002|0003|0004|0005|0006|0007|0008|0009)");
    // Descending Sequences
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{20..15}"), "f(15|16|17|18|19|20)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{200..199}"), "f(199|200)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{0009..0001}"), "f(0001|0002|0003|0004|0005|0006|0007|0008|0009)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{100..90}"), "f(90|91|92|93|94|95|96|97|98|99|100)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{103..95}"), "f(95|96|97|98|99|100|101|102|103)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{9..01}"), "f(01|02|03|04|05|06|07|08|09)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{9..000}"), "f(000|001|002|003|004|005|006|007|008|009)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{1..2}{1..2}"), "f(1|2)(1|2)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{1..1}{1..1}"), "f(1)(1)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{0..0}{0..0}"), "f(0)(0)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("file{1..5}"), "file(1|2|3|4|5)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("file{1,2,3}"), "file(1|2|3)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("{1,2,3}blabla{a.x,b.x,c.x}smth[]_else{aa,bb}?*"), "(1|2|3)blabla(a\\.x|b\\.x|c\\.x)smth\\[\\]_else(aa|bb)[^/][^/]*");
}
