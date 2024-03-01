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
    EXPECT_EQ(makeRegexpPatternFromGlobs("file{1..5}"),"file(1|2|3|4|5)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("file{1,2,3}"),"file(1|2|3)");
    EXPECT_EQ(makeRegexpPatternFromGlobs("{1,2,3}blabla{a.x,b.x,c.x}smth[]_else{aa,bb}?*"), "(1|2|3)blabla(a\\.x|b\\.x|c\\.x)smth\\[\\]_else(aa|bb)[^/][^/]*");
}
