#include <Common/parseGlobs.h>
#include <re2/re2.h>
#include <gtest/gtest.h>


using namespace DB;


TEST(Common, makeRegexpPatternFromGlobs)
{
    EXPECT_EQ(makeRegexpPatternFromGlobs("?"), "[^/]");
    EXPECT_EQ(makeRegexpPatternFromGlobs("*"), "[^/]*");
    EXPECT_EQ(makeRegexpPatternFromGlobs("/?"), "/[^/]");
    EXPECT_EQ(makeRegexpPatternFromGlobs("/*"), "/[^/]*");
    EXPECT_EQ(makeRegexpPatternFromGlobs("*_{{a,b,c,d}}/?.csv"), "[^/]*_\\{(a|b|c|d)\\}/[^/]\\.csv");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{01..09}"), "f(1|2|3|4|5|6|7|8|9)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{01..9}"), "f(1|2|3|4|5|6|7|8|9)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{0001..0000009}"), "f(1|2|3|4|5|6|7|8|9)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{1..2}{1..2}"), "f(1|2)(1|2)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{1..1}{1..1}"), "f(1)(1)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("f{0..0}{0..0}"), "f(0)(0)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("file{1..5}"),"file(1|2|3|4|5)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("file{1,2,3}"),"file(1|2|3)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("{1,2,3}blabla{a.x,b.x,c.x}smth[]_else{aa,bb}?*"), "(1|2|3)blabla(a\\.x|b\\.x|c\\.x)smth\\[\\]_else(aa|bb)[^/][^/]*");
}
