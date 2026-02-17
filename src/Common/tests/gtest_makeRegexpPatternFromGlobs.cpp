#include <Common/parseGlobs.h>
#include <Common/re2.h>
#include <Common/Exception.h>
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
        std::make_pair("*_{{a,b,c,d}}/?.csv", "[^/]*_\\{(a|b|c|d)\\}/[^/]\\.csv"),

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

TEST(Common, BetterGlobExpand)
{
    using V = std::vector<std::string>;

    // No globs — single result.
    EXPECT_EQ(BetterGlob::GlobString("file.csv").expand(), V({"file.csv"}));

    // Single enum.
    EXPECT_EQ(BetterGlob::GlobString("file{a,b,c}.csv").expand(), V({"filea.csv", "fileb.csv", "filec.csv"}));

    // Multiple enums — cartesian product.
    EXPECT_EQ(BetterGlob::GlobString("{a,b}{1,2}").expand(), V({"a1", "a2", "b1", "b2"}));

    // Enum with prefix, middle, suffix.
    EXPECT_EQ(
        BetterGlob::GlobString("prefix{a,b}middle{1,2}suffix").expand(),
        V({"prefixamiddle1suffix", "prefixamiddle2suffix", "prefixbmiddle1suffix", "prefixbmiddle2suffix"}));

    // Single-element enum — acts like a constant.
    EXPECT_EQ(BetterGlob::GlobString("{test}").expand(), V({"{test}"}));

    // Wildcards are passed through as literal text.
    EXPECT_EQ(BetterGlob::GlobString("*.csv").expand(), V({"*.csv"}));
    EXPECT_EQ(BetterGlob::GlobString("file?.csv").expand(), V({"file?.csv"}));
    EXPECT_EQ(BetterGlob::GlobString("{a,b}/*.csv").expand(), V({"a/*.csv", "b/*.csv"}));

    // Ranges are passed through as literal text.
    EXPECT_EQ(BetterGlob::GlobString("f{1..9}.csv").expand(), V({"f{1..9}.csv"}));
    EXPECT_EQ(BetterGlob::GlobString("{a,b}{1..3}.csv").expand(), V({"a{1..3}.csv", "b{1..3}.csv"}));

    // Parity with old expandSelectionGlob.
    EXPECT_EQ(BetterGlob::GlobString("file{1,2,3}").expand(), expandSelectionGlob("file{1,2,3}"));
    EXPECT_EQ(
        BetterGlob::GlobString("{a,b}{1,2}").expand(),
        expandSelectionGlob("{a,b}{1,2}"));

    // Cardinality guard.
    EXPECT_THROW(BetterGlob::GlobString("{1,2,3,4,5,6,7,8,9,10}{1,2,3,4,5,6,7,8,9,10}{1,2,3,4,5,6,7,8,9,10}").expand(100), DB::Exception);
    EXPECT_NO_THROW(BetterGlob::GlobString("{1,2,3,4,5,6,7,8,9,10}{1,2,3,4,5,6,7,8,9,10}{1,2,3,4,5,6,7,8,9,10}").expand(1000));
}

/// Test suite for GlobString::matches() — direct AST-based matching without regex.
class BetterGlobMatchTest : public ::testing::TestWithParam<std::tuple<std::string, std::string, bool>> {};

TEST_P(BetterGlobMatchTest, MatchTest)
{
    const auto & [glob, candidate, expected] = GetParam();
    auto glob_string = BetterGlob::GlobString(glob);
    EXPECT_EQ(glob_string.matches(candidate), expected)
        << "glob=" << glob << " candidate=" << candidate << " expected=" << expected;
}

INSTANTIATE_TEST_SUITE_P(
    Common,
    BetterGlobMatchTest,
    ::testing::Values(
        // --- Constants ---
        std::make_tuple("abc", "abc", true),
        std::make_tuple("abc", "abcd", false),
        std::make_tuple("abc", "ab", false),
        std::make_tuple("abc", "", false),
        std::make_tuple("", "", true),
        std::make_tuple("/path/to/file.csv", "/path/to/file.csv", true),
        std::make_tuple("/path/to/file.csv", "/path/to/file.tsv", false),

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
        std::make_tuple("**", "a/b/c", true),     // '**' matches across '/'
        std::make_tuple("**", "a{b", false),       // '**' does not match '{'
        std::make_tuple("**", "a}b", false),       // '**' does not match '}'

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
        std::make_tuple("*_{{a,b,c,d}}/?.csv", "x_{e}/1.csv", false)
    )
);

/// Verify that matches() agrees with asRegex() + RE2::FullMatch on all BetterGlobRegexTest patterns.
class BetterGlobMatchVsRegexTest : public ::testing::TestWithParam<std::tuple<std::string, std::vector<std::string>, std::vector<std::string>>> {};

TEST_P(BetterGlobMatchVsRegexTest, MatchVsRegex)
{
    const auto & [glob, positives, negatives] = GetParam();
    auto glob_string = BetterGlob::GlobString(glob);
    auto regex_str = glob_string.asRegex();
    re2::RE2 matcher(regex_str);
    ASSERT_TRUE(matcher.ok()) << "Regex failed to compile: " << regex_str;

    for (const auto & candidate : positives)
    {
        bool regex_result = re2::RE2::FullMatch(candidate, matcher);
        bool direct_result = glob_string.matches(candidate);
        EXPECT_EQ(regex_result, direct_result)
            << "glob=" << glob << " candidate=" << candidate
            << " regex=" << regex_result << " direct=" << direct_result;
        EXPECT_TRUE(direct_result)
            << "Expected match for glob=" << glob << " candidate=" << candidate;
    }

    for (const auto & candidate : negatives)
    {
        bool regex_result = re2::RE2::FullMatch(candidate, matcher);
        bool direct_result = glob_string.matches(candidate);
        EXPECT_EQ(regex_result, direct_result)
            << "glob=" << glob << " candidate=" << candidate
            << " regex=" << regex_result << " direct=" << direct_result;
        EXPECT_FALSE(direct_result)
            << "Expected no match for glob=" << glob << " candidate=" << candidate;
    }
}

using V = std::vector<std::string>;

INSTANTIATE_TEST_SUITE_P(
    Common,
    BetterGlobMatchVsRegexTest,
    ::testing::Values(
        std::make_tuple("?", V({"a", "1", "Z"}), V({"", "ab", "/"})),
        std::make_tuple("*", V({"", "abc", "file.csv"}), V({"/", "a/b"})),
        std::make_tuple("/*", V({"/", "/abc", "/file.csv"}), V({"", "a/b"})),
        std::make_tuple("file?.csv", V({"file1.csv", "fileA.csv"}), V({"file.csv", "file12.csv"})),
        std::make_tuple("file{1,2,3}.csv", V({"file1.csv", "file2.csv", "file3.csv"}), V({"file4.csv", "file.csv"})),
        std::make_tuple("f{1..9}", V({"f1", "f5", "f9"}), V({"f0", "f10", "f"})),
        std::make_tuple("f{00..10}", V({"f00", "f05", "f10"}), V({"f0", "f5", "f11"})),
        std::make_tuple("f{10..20}", V({"f10", "f15", "f20"}), V({"f9", "f21"})),
        std::make_tuple("f{20..15}", V({"f15", "f17", "f20"}), V({"f14", "f21"})),
        std::make_tuple("f{0001..0009}", V({"f0001", "f0005", "f0009"}), V({"f1", "f0010"})),
        std::make_tuple("f{9..000}", V({"f000", "f005", "f009"}), V({"f9", "f010"})),
        std::make_tuple("{a,b}/*.csv", V({"a/data.csv", "b/x.csv"}), V({"c/data.csv", "a/b/c.csv"})),
        std::make_tuple("*_{{a,b,c,d}}/?.csv",
            V({"x_{a}/1.csv", "test_{d}/Z.csv"}),
            V({"x_{e}/1.csv", "x_{a}/12.csv"}))
    )
);
