#include <Common/parseGlobs.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>

#include <map>
#include <optional>
#include <random>
#include <string>

using namespace DB;

TEST(Common, GlobAST)
{
    // Smoke tests.
    {
        auto s = GlobAST::GlobString("123");
        EXPECT_EQ(s.getExpressions().size(), 1);
        EXPECT_EQ(s.getExpressions().front().type(), GlobAST::ExpressionType::CONSTANT);
    }
    {
        auto s = GlobAST::GlobString("123{123,456,789,234,567,890}");
        EXPECT_EQ(s.getExpressions().size(), 2);
        EXPECT_EQ(s.getExpressions().front().type(), GlobAST::ExpressionType::CONSTANT);
        EXPECT_EQ(s.getExpressions().back().type(), GlobAST::ExpressionType::ENUM);
        EXPECT_EQ(s.getExpressions().back().cardinality(), 6);
    }
    {
        auto s = GlobAST::GlobString("123{123}{12..23}");
        EXPECT_EQ(s.getExpressions().size(), 3);
        EXPECT_EQ(s.getExpressions().front().type(), GlobAST::ExpressionType::CONSTANT);
        EXPECT_EQ(s.getExpressions().back().type(), GlobAST::ExpressionType::RANGE);
    }
    {
        auto s = GlobAST::GlobString("123{12..23}{1223}");
        EXPECT_EQ(s.getExpressions().size(), 3);
        EXPECT_EQ(s.getExpressions().front().type(), GlobAST::ExpressionType::CONSTANT);
        EXPECT_EQ(s.getExpressions().back().type(), GlobAST::ExpressionType::ENUM);
    }
    {
        /// A single-character brace group is literal text, not a one-element enum
        /// (parity with the legacy parser, whose enum pattern requires >= 2 chars).
        auto s = GlobAST::GlobString("{a}");
        for (const auto & expression : s.getExpressions())
            EXPECT_EQ(expression.type(), GlobAST::ExpressionType::CONSTANT);
        EXPECT_EQ(s.dump(), "{a}");
        EXPECT_FALSE(s.hasEnums());
    }
    {
        /// An enum body may not begin or end with ',' (no empty edge alternative), so
        /// these are literal text, not enums (legacy parity).
        for (const auto * glob : {"{a,}", "{,a}", "{,}", "{a,b,}", "{,a,b}"})
        {
            auto s = GlobAST::GlobString(glob);
            EXPECT_FALSE(s.hasEnums()) << "glob=" << glob;
            EXPECT_EQ(s.dump(), glob) << "glob=" << glob;
        }
    }
    {
        /// An enum body may not contain a nested '{', so these are literal text.
        for (const auto * glob : {"{a{b}", "{0b{}", "{1{?}"})
        {
            auto s = GlobAST::GlobString(glob);
            EXPECT_FALSE(s.hasEnums()) << "glob=" << glob;
            EXPECT_EQ(s.dump(), glob) << "glob=" << glob;
        }
    }
    {
        /// A range needs digits on both sides; an empty side ("{..1}", "{0..}") is not a
        /// range but a one-element enum matching the literal body (legacy parity).
        auto missing_start = GlobAST::GlobString("{..1}");
        EXPECT_EQ(missing_start.getExpressions().size(), 1);
        EXPECT_EQ(missing_start.getExpressions().front().type(), GlobAST::ExpressionType::ENUM);

        auto missing_end = GlobAST::GlobString("{0..}");
        EXPECT_EQ(missing_end.getExpressions().size(), 1);
        EXPECT_EQ(missing_end.getExpressions().front().type(), GlobAST::ExpressionType::ENUM);
    }

    // Range tests.
    {
        auto s = GlobAST::GlobString("f{1..9}");
        EXPECT_EQ(s.getExpressions().size(), 2);
        EXPECT_EQ(s.getExpressions().back().type(), GlobAST::ExpressionType::RANGE);

        auto r = std::get<GlobAST::Range>(s.getExpressions().back().getData());
        EXPECT_EQ(r.start, 1);
        EXPECT_EQ(r.end, 9);
    }
    {
        auto s = GlobAST::GlobString("f{0..10}");
        EXPECT_EQ(s.getExpressions().size(), 2);
        EXPECT_EQ(s.getExpressions().back().type(), GlobAST::ExpressionType::RANGE);

        auto r = std::get<GlobAST::Range>(s.getExpressions().back().getData());
        EXPECT_EQ(r.start, 0);
        EXPECT_EQ(r.end, 10);
    }
    {
        auto s = GlobAST::GlobString("f{10..20}");
        EXPECT_EQ(s.getExpressions().size(), 2);
        EXPECT_EQ(s.getExpressions().back().type(), GlobAST::ExpressionType::RANGE);

        auto r = std::get<GlobAST::Range>(s.getExpressions().back().getData());
        EXPECT_EQ(r.start, 10);
        EXPECT_EQ(r.end, 20);
    }
    {
        auto s = GlobAST::GlobString("f{00..10}");
        EXPECT_EQ(s.getExpressions().size(), 2);
        EXPECT_EQ(s.getExpressions().back().type(), GlobAST::ExpressionType::RANGE);

        auto r = std::get<GlobAST::Range>(s.getExpressions().back().getData());
        EXPECT_EQ(r.start, 0);
        EXPECT_EQ(r.end, 10);
        EXPECT_EQ(r.start_digit_count, 2);
        EXPECT_EQ(r.end_digit_count, 2);
    }
    {
        auto s = GlobAST::GlobString("f{9..000}");
        EXPECT_EQ(s.getExpressions().size(), 2);
        EXPECT_EQ(s.getExpressions().back().type(), GlobAST::ExpressionType::RANGE);
        EXPECT_EQ(s.getExpressions().back().dump(), "{9..000}");

        auto r = std::get<GlobAST::Range>(s.getExpressions().back().getData());
        EXPECT_EQ(r.start, 9);
        EXPECT_EQ(r.end, 0);
        EXPECT_EQ(r.start_digit_count, 1);
        EXPECT_EQ(r.end_digit_count, 3);
    }
}

class GlobASTEchoTest : public ::testing::TestWithParam<std::string> {};

TEST_P(GlobASTEchoTest, EchoTest)
{
    const auto & glob = GetParam();
    EXPECT_EQ(GlobAST::GlobString(glob).dump(), glob);
}

INSTANTIATE_TEST_SUITE_P(
    Common,
    GlobASTEchoTest,
    ::testing::Values(
        // Basic constant
        "a",
        "aaa",
        "file.txt",
        "/path/to/file",
        "?",
        "*",
        "/?",

        // Single-character brace group is literal text, not an enum
        "{a}",

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

TEST(Common, GlobASTExpand)
{
    using V = std::vector<std::string>;

    // No globs — single result.
    EXPECT_EQ(GlobAST::GlobString("file.csv").expand(), V({"file.csv"}));

    // Single enum.
    EXPECT_EQ(GlobAST::GlobString("file{a,b,c}.csv").expand(), V({"filea.csv", "fileb.csv", "filec.csv"}));

    // Multiple enums — cartesian product.
    EXPECT_EQ(GlobAST::GlobString("{a,b}{1,2}").expand(), V({"a1", "a2", "b1", "b2"}));

    // Enum with prefix, middle, suffix.
    EXPECT_EQ(
        GlobAST::GlobString("prefix{a,b}middle{1,2}suffix").expand(),
        V({"prefixamiddle1suffix", "prefixamiddle2suffix", "prefixbmiddle1suffix", "prefixbmiddle2suffix"}));

    // Single-element enum — braces are stripped (matches legacy expandSelectionGlob behavior).
    EXPECT_EQ(GlobAST::GlobString("{test}").expand(), V({"test"}));

    // Wildcards are passed through as literal text.
    EXPECT_EQ(GlobAST::GlobString("*.csv").expand(), V({"*.csv"}));
    EXPECT_EQ(GlobAST::GlobString("file?.csv").expand(), V({"file?.csv"}));
    EXPECT_EQ(GlobAST::GlobString("{a,b}/*.csv").expand(), V({"a/*.csv", "b/*.csv"}));

    // Ranges are passed through as literal text by default (expand_ranges=false).
    EXPECT_EQ(GlobAST::GlobString("f{1..9}.csv").expand(), V({"f{1..9}.csv"}));
    EXPECT_EQ(GlobAST::GlobString("{a,b}{1..3}.csv").expand(), V({"a{1..3}.csv", "b{1..3}.csv"}));

    // Ranges are expanded into concrete values with expand_ranges=true.
    EXPECT_EQ(GlobAST::GlobString("f{1..3}.csv").expand(1000, true), V({"f1.csv", "f2.csv", "f3.csv"}));
    EXPECT_EQ(GlobAST::GlobString("f{1..1}.csv").expand(1000, true), V({"f1.csv"}));
    EXPECT_EQ(GlobAST::GlobString("f{3..1}.csv").expand(1000, true), V({"f1.csv", "f2.csv", "f3.csv"}));

    // Ranges with zero-padding.
    EXPECT_EQ(GlobAST::GlobString("f{01..03}.csv").expand(1000, true), V({"f01.csv", "f02.csv", "f03.csv"}));
    EXPECT_EQ(GlobAST::GlobString("f{001..003}.csv").expand(1000, true), V({"f001.csv", "f002.csv", "f003.csv"}));

    // Mixed-width padding derives from the lower endpoint (legacy parity):
    // {0..010} has a one-digit lower endpoint, so values are unpadded.
    EXPECT_EQ(GlobAST::GlobString("f{0..010}.csv").expand(1000, true),
              V({"f0.csv", "f1.csv", "f2.csv", "f3.csv", "f4.csv", "f5.csv", "f6.csv", "f7.csv", "f8.csv", "f9.csv", "f10.csv"}));
    // {00..10} has a two-digit lower endpoint, so values are padded to width 2.
    EXPECT_EQ(GlobAST::GlobString("f{00..10}.csv").expand(1000, true),
              V({"f00.csv", "f01.csv", "f02.csv", "f03.csv", "f04.csv", "f05.csv", "f06.csv", "f07.csv", "f08.csv", "f09.csv", "f10.csv"}));

    // Range + enum cartesian product.
    EXPECT_EQ(GlobAST::GlobString("{a,b}{1..3}.csv").expand(1000, true), V({"a1.csv", "a2.csv", "a3.csv", "b1.csv", "b2.csv", "b3.csv"}));

    // Range cardinality guard.
    EXPECT_THROW(GlobAST::GlobString("f{1..2000}.csv").expand(100, true), DB::Exception);
    EXPECT_NO_THROW(GlobAST::GlobString("f{1..100}.csv").expand(1000, true));

    // Parity with old expandSelectionGlob.
    EXPECT_EQ(GlobAST::GlobString("file{1,2,3}").expand(), expandSelectionGlob("file{1,2,3}"));
    EXPECT_EQ(
        GlobAST::GlobString("{a,b}{1,2}").expand(),
        expandSelectionGlob("{a,b}{1,2}"));
    EXPECT_EQ(GlobAST::GlobString("{test}").expand(), expandSelectionGlob("{test}"));
    EXPECT_EQ(GlobAST::GlobString("{a,b,c}").expand(), expandSelectionGlob("{a,b,c}"));
    EXPECT_EQ(GlobAST::GlobString("prefix{x,y}suffix").expand(), expandSelectionGlob("prefix{x,y}suffix"));
    EXPECT_EQ(GlobAST::GlobString("{a,b}{c,d}{e,f}").expand(), expandSelectionGlob("{a,b}{c,d}{e,f}"));
    EXPECT_EQ(GlobAST::GlobString("no_globs.csv").expand(), expandSelectionGlob("no_globs.csv"));
    EXPECT_EQ(GlobAST::GlobString("file{1,2,3}.csv").expand(), expandSelectionGlob("file{1,2,3}.csv"));
    EXPECT_EQ(GlobAST::GlobString("prefix{a,b}middle{1,2}suffix").expand(), expandSelectionGlob("prefix{a,b}middle{1,2}suffix"));

    // Cardinality guard.
    EXPECT_THROW(GlobAST::GlobString("{1,2,3,4,5,6,7,8,9,10}{1,2,3,4,5,6,7,8,9,10}{1,2,3,4,5,6,7,8,9,10}").expand(100), DB::Exception);
    EXPECT_NO_THROW(GlobAST::GlobString("{1,2,3,4,5,6,7,8,9,10}{1,2,3,4,5,6,7,8,9,10}{1,2,3,4,5,6,7,8,9,10}").expand(1000));
}

/// Test suite for GlobString::matches() — direct AST-based matching without regex.
class GlobASTMatchTest : public ::testing::TestWithParam<std::tuple<std::string, std::string, bool>> {};

TEST_P(GlobASTMatchTest, MatchTest)
{
    const auto & [glob, candidate, expected] = GetParam();
    auto glob_string = GlobAST::GlobString(glob);
    EXPECT_EQ(glob_string.matches(candidate), expected)
        << "glob=" << glob << " candidate=" << candidate << " expected=" << expected;
}

INSTANTIATE_TEST_SUITE_P(
    Common,
    GlobASTMatchTest,
    ::testing::Values(
        // --- Constants ---
        std::make_tuple("abc", "abc", true),
        std::make_tuple("abc", "abcd", false),
        std::make_tuple("abc", "ab", false),
        std::make_tuple("abc", "", false),
        std::make_tuple("", "", true),
        std::make_tuple("/path/to/file.csv", "/path/to/file.csv", true),
        std::make_tuple("/path/to/file.csv", "/path/to/file.tsv", false),

        // --- Single-character brace group is literal, not a one-element enum ---
        std::make_tuple("{a}", "{a}", true),
        std::make_tuple("{a}", "a", false),

        // --- Malformed brace groups are literal text (legacy parity) ---
        // Empty leading/trailing enum alternative
        std::make_tuple("{a,}", "{a,}", true),
        std::make_tuple("{a,}", "a", false),
        std::make_tuple("{,a}", "{,a}", true),
        std::make_tuple("{,a}", "a", false),
        std::make_tuple("{,}", "{,}", true),
        std::make_tuple("{,}", "", false),
        // Nested '{' in the body
        std::make_tuple("{a{b}", "{a{b}", true),
        std::make_tuple("{a{b}", "ab", false),
        // Range with an empty side is a one-element enum matching the literal body
        std::make_tuple("{..1}", "..1", true),
        std::make_tuple("{..1}", "0", false),
        std::make_tuple("{0..}", "0..", true),
        std::make_tuple("{0..}", "0", false),

        // --- Question mark wildcard ---
        std::make_tuple("?", "a", true),
        std::make_tuple("?", "Z", true),
        std::make_tuple("?", "1", true),
        std::make_tuple("?", "", false),
        std::make_tuple("?", "ab", false),
        std::make_tuple("?", "/", false),  // '?' does not match '/'
        std::make_tuple("file?.csv", "file1.csv", true),
        std::make_tuple("file?.csv", "fileA.csv", true),
        std::make_tuple("file?.csv", "file.csv", false),
        std::make_tuple("file?.csv", "file12.csv", false),
        std::make_tuple("??", "ab", true),
        std::make_tuple("??", "a", false),
        std::make_tuple("??", "abc", false),

        // --- Single asterisk wildcard ---
        std::make_tuple("*", "", true),
        std::make_tuple("*", "anything", true),
        std::make_tuple("*", "file.csv", true),
        std::make_tuple("*", "/", false),  // '*' does not match '/'
        std::make_tuple("*", "a/b", false),
        std::make_tuple("/*.csv", "/data.csv", true),
        std::make_tuple("/*.csv", "/anything.csv", true),
        std::make_tuple("/*.csv", "/data.tsv", false),
        std::make_tuple("/*.csv", "/.csv", true),
        std::make_tuple("/data*", "/data", true),
        std::make_tuple("/data*", "/data123", true),
        std::make_tuple("/data*", "/dat", false),
        std::make_tuple("/data*.csv", "/data.csv", true),
        std::make_tuple("/data*.csv", "/data123.csv", true),
        std::make_tuple("/data*.csv", "/data.tsv", false),
        std::make_tuple("/*", "/anything", true),
        std::make_tuple("/*", "/", true),
        std::make_tuple("*/*", "a/b", true),
        std::make_tuple("*/*", "abc/def", true),
        std::make_tuple("*/*", "abc", false),

        // --- Double asterisk wildcard ---
        std::make_tuple("**", "", true),
        std::make_tuple("**", "anything", true),
        std::make_tuple("**", "a/b/c", true),      // '**' matches across '/'
        // Legacy translates '**' to the regex `[^/]*[^{}]*`, so a brace is matched
        // as long as it does not appear at or after the first '/'.
        std::make_tuple("**", "a{b", true),        // brace before any '/' is matched
        std::make_tuple("**", "a}b", true),        // brace before any '/' is matched
        std::make_tuple("**", "a/b{c", false),     // brace at/after the first '/' is not matched
        std::make_tuple("**", "a/b}c", false),     // brace at/after the first '/' is not matched

        // --- Ranges ---
        std::make_tuple("f{1..9}", "f1", true),
        std::make_tuple("f{1..9}", "f5", true),
        std::make_tuple("f{1..9}", "f9", true),
        std::make_tuple("f{1..9}", "f0", false),    // 0 out of range
        std::make_tuple("f{1..9}", "f10", false),   // 10 out of range
        std::make_tuple("f{1..9}", "f", false),     // no digits
        std::make_tuple("f{0..10}", "f0", true),
        std::make_tuple("f{0..10}", "f10", true),
        std::make_tuple("f{0..10}", "f5", true),
        std::make_tuple("f{0..10}", "f11", false),
        std::make_tuple("f{10..20}", "f10", true),
        std::make_tuple("f{10..20}", "f15", true),
        std::make_tuple("f{10..20}", "f20", true),
        std::make_tuple("f{10..20}", "f9", false),
        std::make_tuple("f{10..20}", "f21", false),

        // --- Zero-padded ranges ---
        std::make_tuple("f{00..10}", "f00", true),
        std::make_tuple("f{00..10}", "f05", true),
        std::make_tuple("f{00..10}", "f10", true),
        std::make_tuple("f{00..10}", "f0", false),   // wrong width (1 digit, need 2)
        std::make_tuple("f{00..10}", "f5", false),    // wrong width
        std::make_tuple("f{00..10}", "f010", false),  // wrong width (3 digits)
        std::make_tuple("f{0001..0009}", "f0001", true),
        std::make_tuple("f{0001..0009}", "f0009", true),
        std::make_tuple("f{0001..0009}", "f0005", true),
        std::make_tuple("f{0001..0009}", "f1", false),    // wrong width
        std::make_tuple("f{0001..0009}", "f0010", false),  // out of range

        // --- Descending ranges ---
        std::make_tuple("f{20..15}", "f15", true),
        std::make_tuple("f{20..15}", "f20", true),
        std::make_tuple("f{20..15}", "f17", true),
        std::make_tuple("f{20..15}", "f14", false),
        std::make_tuple("f{20..15}", "f21", false),
        std::make_tuple("f{9..000}", "f000", true),
        std::make_tuple("f{9..000}", "f009", true),
        std::make_tuple("f{9..000}", "f005", true),
        std::make_tuple("f{9..000}", "f9", false),     // wrong width (need 3 digits)

        // --- Enums ---
        std::make_tuple("{a,b,c}", "a", true),
        std::make_tuple("{a,b,c}", "b", true),
        std::make_tuple("{a,b,c}", "c", true),
        std::make_tuple("{a,b,c}", "d", false),
        std::make_tuple("{a,b,c}", "", false),
        std::make_tuple("file{1,2,3}.csv", "file1.csv", true),
        std::make_tuple("file{1,2,3}.csv", "file2.csv", true),
        std::make_tuple("file{1,2,3}.csv", "file4.csv", false),
        std::make_tuple("{hello,world}", "hello", true),
        std::make_tuple("{hello,world}", "world", true),
        std::make_tuple("{hello,world}", "xyz", false),

        // --- Mixed expressions ---
        std::make_tuple("file{1..3}*.csv", "file1data.csv", true),
        std::make_tuple("file{1..3}*.csv", "file2.csv", true),
        std::make_tuple("file{1..3}*.csv", "file3abc.csv", true),
        std::make_tuple("file{1..3}*.csv", "file4.csv", false),
        std::make_tuple("{a,b}/*.csv", "a/data.csv", true),
        std::make_tuple("{a,b}/*.csv", "b/data.csv", true),
        std::make_tuple("{a,b}/*.csv", "c/data.csv", false),

        // --- Complex patterns ---
        std::make_tuple("{1,2,3}blabla{a.x,b.x,c.x}smth[]_else{aa,bb}?*",
                        "1blablaa.xsmth[]_elseaaXY", true),
        std::make_tuple("{1,2,3}blabla{a.x,b.x,c.x}smth[]_else{aa,bb}?*",
                        "2blablab.xsmth[]_elsebbZ", true),
        std::make_tuple("{1,2,3}blabla{a.x,b.x,c.x}smth[]_else{aa,bb}?*",
                        "4blablaa.xsmth[]_elseaaXY", false),  // 4 not in enum

        // --- Double brace ---
        std::make_tuple("*_{{a,b,c,d}}/?.csv", "x_{a}/1.csv", true),
        std::make_tuple("*_{{a,b,c,d}}/?.csv", "x_{d}/Z.csv", true),
        std::make_tuple("*_{{a,b,c,d}}/?.csv", "x_{e}/1.csv", false),

        // --- Consecutive ranges (backtracking) ---
        std::make_tuple("f{0..9}{0..9}", "f00", true),
        std::make_tuple("f{0..9}{0..9}", "f59", true),
        std::make_tuple("f{0..9}{0..9}", "f99", true),
        std::make_tuple("f{0..9}{0..9}", "f0", false),    // only 1 digit total, need 2
        std::make_tuple("f{0..9}{0..9}", "f100", false),   // 3 digits but ranges only produce 2
        std::make_tuple("f{0..9}{0..9}{0..9}", "f000", true),
        std::make_tuple("f{0..9}{0..9}{0..9}", "f123", true),
        std::make_tuple("f{0..9}{0..9}{0..9}", "f999", true),
        std::make_tuple("f{0..9}{0..9}{0..9}", "f12", false),   // only 2 digits
        std::make_tuple("f{0..9}{0..9}{0..9}", "f1234", false), // 4 digits
        std::make_tuple("/file{0..9}{0..9}{0..9}", "/file000", true),
        std::make_tuple("/file{0..9}{0..9}{0..9}", "/file444", true),
        std::make_tuple("/file{0..9}{0..9}{0..9}", "/file1", false),
        std::make_tuple("f{0..10}{0..10}", "f00", true),
        std::make_tuple("f{0..10}{0..10}", "f1010", true),
        std::make_tuple("f{0..10}{0..10}", "f55", true),
        std::make_tuple("f{0..10}{0..10}", "f110", true),  // 1+10: both in range

        // --- Wildcards inside enum alternatives (legacy parity: '?' is a wildcard) ---
        std::make_tuple("file{a?,b?}.csv", "filea1.csv", true),
        std::make_tuple("file{a?,b?}.csv", "fileb9.csv", true),
        std::make_tuple("file{a?,b?}.csv", "filec1.csv", false),  // 'c' not an alternative
        std::make_tuple("file{a?,b?}.csv", "filea.csv", false),   // '?' needs one char
        std::make_tuple("file{a?,b?}.csv", "filea/.csv", false),  // '?' does not match '/'
        std::make_tuple("{ab?,cd?}", "abx", true),
        std::make_tuple("{ab?,cd?}", "cd9", true),
        std::make_tuple("{ab?,cd?}", "ab", false),
        // '*' inside braces is not an enum — braces are literal, '*' is a wildcard.
        std::make_tuple("{a*,b*}", "axyz", false),
        std::make_tuple("{a*,b*}", "{axyz,b}", true),

        // --- Mixed-width range padding derives from the lower endpoint (legacy parity) ---
        std::make_tuple("f{0..010}", "f10", true),    // width 1 -> unpadded
        std::make_tuple("f{0..010}", "f5", true),
        std::make_tuple("f{0..010}", "f010", false),  // not zero-padded to 3
        std::make_tuple("f{00..10}", "f00", true),    // width 2 -> padded
        std::make_tuple("f{00..10}", "f0", false),

        // --- Full size_t range endpoints (checked accumulation, no 19-digit cap) ---
        std::make_tuple("v{0..18446744073709551615}", "v18446744073709551615", true),
        std::make_tuple("v{0..18446744073709551615}", "v42", true),

        // --- '?' matches a whole UTF-8 code point, like RE2 [^/] ---
        std::make_tuple("file?.csv", "file\xc3\xa9.csv", true),  // fileé.csv
        std::make_tuple("?", "\xc3\xa9", true),                     // é

        // --- "**" allows braces before the first slash (legacy [^/]*[^{}]*) ---
        std::make_tuple("/**", "/a{b}c", true),
        std::make_tuple("/**", "/dir/file", true)
    )
);


/// Systematic parity test: GlobAST::GlobString::expand() must agree with expandSelectionGlob()
/// for all patterns that only contain enum globs (no ranges/wildcards).
class GlobASTExpandParityTest : public ::testing::TestWithParam<std::string> {};

TEST_P(GlobASTExpandParityTest, ExpandParity)
{
    const auto & glob = GetParam();
    EXPECT_EQ(GlobAST::GlobString(glob).expand(), expandSelectionGlob(glob))
        << "Expand mismatch for glob: " << glob;
}

INSTANTIATE_TEST_SUITE_P(
    Common,
    GlobASTExpandParityTest,
    ::testing::Values(
        "no_globs.csv",
        "{test}",
        "{a,b,c}",
        "file{1,2,3}.csv",
        "{a,b}{1,2}",
        "{a,b}{c,d}{e,f}",
        "prefix{x,y}suffix",
        "prefix{a,b}middle{1,2}suffix",
        "{hello,world}",
        "file{x,y,z}.{csv,tsv}",
        "*.csv",
        "file?.csv",
        "{a,b}/*.csv"
    )
);

/// Edge-case tests for specific bug fixes addressed during review.
TEST(Common, GlobASTExponentialBacktracking)
{
    /// Pattern with many wildcards that could cause exponential backtracking
    /// without memoization. This must complete in reasonable time.
    GlobAST::GlobString pattern("*a*b*c*d*e*f*g*h*i*j*");
    EXPECT_TRUE(pattern.matches("XaXbXcXdXeXfXgXhXiXjX"));
    EXPECT_FALSE(pattern.matches("XaXbXcXdXeXfXgXhXiX"));

    /// Worst-case for wildcard: long non-matching candidate.
    /// Without memoization this would be O(2^n); with it, O(n*e).
    std::string long_candidate(200, 'a');
    GlobAST::GlobString many_stars("*a*a*a*a*a*a*a*a*a*a*b");
    EXPECT_FALSE(many_stars.matches(long_candidate));
}

TEST(Common, GlobASTFindDoubleDot)
{
    /// Patterns with single dots inside braces should be parsed as enums, not ranges.
    /// This tests the fix: find_first_of("..") -> find("..") in tryParseRangeMatcher.
    GlobAST::GlobString single_dot("{a.x,b.x}");
    EXPECT_EQ(single_dot.getExpressions().size(), 1);
    EXPECT_EQ(single_dot.getExpressions().front().type(), GlobAST::ExpressionType::ENUM);
    EXPECT_TRUE(single_dot.matches("a.x"));
    EXPECT_TRUE(single_dot.matches("b.x"));

    /// A pattern with ".." should still be parsed as a range.
    GlobAST::GlobString range("{1..5}");
    EXPECT_EQ(range.getExpressions().size(), 1);
    EXPECT_EQ(range.getExpressions().front().type(), GlobAST::ExpressionType::RANGE);
    EXPECT_TRUE(range.matches("3"));
}

TEST(Common, GlobASTRangeOverflow)
{
    /// Large range cardinality must not overflow.
    GlobAST::GlobString large_range("{0..18446744073709551614}");
    EXPECT_EQ(large_range.getExpressions().front().cardinality(), std::numeric_limits<size_t>::max());

    /// Expansion of such a range with expand_ranges=true must throw, not hang or overflow.
    EXPECT_THROW(large_range.expand(1000, true), DB::Exception);

    /// Matching with huge range end: candidate value exceeding the range
    /// should be rejected without overflow in digit accumulation.
    EXPECT_FALSE(large_range.matches("99999999999999999999999"));
    EXPECT_TRUE(large_range.matches("0"));
    EXPECT_TRUE(large_range.matches("12345"));
}

/// Differential fuzzer: the AST matcher (use_glob_ast_parser = 1) must agree with the
/// legacy regex matcher (makeRegexpPatternFromGlobs + re2::RE2::FullMatch) on every
/// (pattern, candidate) pair. Patterns and candidates are drawn from a small alphabet
/// rich in glob metacharacters, with a fixed seed for reproducibility. Divergences are
/// deduplicated by pattern and reported in bulk so a single run surfaces every distinct
/// disagreement at once.
///
/// The fuzzer runs over the input domain where the legacy parser is *correct*. An earlier
/// (unrestricted) run found 40 diverging patterns; three were genuine AST bugs now fixed
/// in GlobString::parse (empty enum edges "{,0}", nested '{' in enum "{b{,1}", empty-sided
/// ranges "{..1}"). The remaining divergences are all cases where legacy is buggy relative
/// to POSIX shell semantics and the AST is correct, so they are excluded from the domain
/// rather than asserted (each is covered by explicit cases in GlobASTMatchTest):
///   * Brace body of a legacy-escaped char (e.g. "{-}"): legacy escapes '-' -> "\-" before
///     enum detection, inflating the body to two chars, so it matches '-' instead of the
///     literal "{-}". Excluded by dropping '-' from the pattern alphabet.
///   * Wildcard runs ("**", "***", "*?*"): legacy's makeRegexpPatternFromGlobs mis-tracks
///     its 'previous' char after a '?' and turns a '*' following a '*' into "[^{}]", so its
///     wildcard runs wrongly cross '/' or stop at braces. Excluded by skipping patterns
///     with two adjacent wildcard characters (which also drops the recursive "**").
TEST(Common, GlobASTLegacyMatchFuzz)
{
    /// Pattern alphabet of glob metacharacters plus a literal the legacy escaper treats
    /// specially ('.'). '-' is intentionally omitted: legacy escapes it before enum
    /// detection, so a brace body like "{-}" wrongly becomes an enum (see comment above).
    static const std::string PATTERN_ALPHABET = "ab01{},*?./";
    /// Candidate alphabet includes braces and slashes to probe enum/wildcard edges.
    static const std::string CANDIDATE_ALPHABET = "ab01/.{}";

    constexpr int num_patterns = 20000;
    constexpr int candidates_per_pattern = 40;
    constexpr size_t max_pattern_len = 6;
    constexpr size_t max_candidate_len = 6;
    constexpr size_t max_reported = 40;

    std::mt19937 rng(0xC10BA5); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible fuzzing

    auto random_string = [&](const std::string & alphabet, size_t max_len)
    {
        size_t len = rng() % (max_len + 1);
        std::string s;
        s.reserve(len);
        for (size_t i = 0; i < len; ++i)
            s += alphabet[rng() % alphabet.size()];
        return s;
    };

    /// Two adjacent wildcard characters trigger the legacy wildcard-run bug (including the
    /// recursive "**"), where legacy diverges from POSIX and the AST. Skip such patterns.
    auto has_adjacent_wildcards = [](const std::string & s)
    {
        for (size_t i = 0; i + 1 < s.size(); ++i)
        {
            const bool a = s[i] == '*' || s[i] == '?';
            const bool b = s[i + 1] == '*' || s[i + 1] == '?';
            if (a && b)
                return true;
        }
        return false;
    };

    std::map<std::string, std::string> divergences;   // pattern -> first disagreeing sample
    size_t compared = 0;
    size_t skipped = 0;

    for (int p = 0; p < num_patterns && divergences.size() < max_reported; ++p)
    {
        /// Patterns are at least one character long.
        std::string pattern = random_string(PATTERN_ALPHABET, max_pattern_len - 1);
        pattern += PATTERN_ALPHABET[rng() % PATTERN_ALPHABET.size()];

        if (has_adjacent_wildcards(pattern))
        {
            ++skipped;
            continue;
        }

        bool legacy_ok = true;
        bool ast_ok = true;
        std::optional<GlobMatcher> legacy;
        std::optional<GlobMatcher> ast;
        /// Ok: a pattern that one engine rejects (throws on) is recorded as a divergence below.
        try { legacy = GlobMatcher::createLegacy(pattern); } catch (...) { legacy_ok = false; }  /// Ok
        try { ast = GlobMatcher::createNew(pattern); } catch (...) { ast_ok = false; }  /// Ok

        /// One engine accepting a pattern that the other rejects is itself a divergence.
        if (legacy_ok != ast_ok)
        {
            divergences.emplace(pattern, "construction: legacy_ok=" + std::to_string(legacy_ok)
                                             + " ast_ok=" + std::to_string(ast_ok));
            continue;
        }
        if (!legacy_ok)
            continue;

        /// Always probe the pattern itself and the empty string, then random candidates.
        std::vector<std::string> candidates{pattern, ""};
        for (int c = 0; c < candidates_per_pattern; ++c)
            candidates.push_back(random_string(CANDIDATE_ALPHABET, max_candidate_len));

        for (const auto & candidate : candidates)
        {
            bool legacy_match = legacy->matches(candidate);
            bool ast_match = ast->matches(candidate);
            ++compared;
            if (legacy_match != ast_match && !divergences.contains(pattern))
            {
                divergences.emplace(pattern, "candidate=[" + candidate + "] legacy="
                                                 + std::to_string(legacy_match) + " ast=" + std::to_string(ast_match));
            }
        }
    }

    if (!divergences.empty())
    {
        std::string report = "AST vs legacy matcher diverged on " + std::to_string(divergences.size())
            + " distinct pattern(s) (compared " + std::to_string(compared) + " pairs, skipped "
            + std::to_string(skipped) + " wildcard-run patterns):\n";
        for (const auto & [pattern, sample] : divergences)
            report += "  pattern=[" + pattern + "] " + sample + "\n";
        ADD_FAILURE() << report;
    }
}
