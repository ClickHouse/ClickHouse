#include <Common/Exception.h>
#include <Common/parseGlobs.h>
#include <Common/re2.h>
#include <gtest/gtest.h>

using namespace DB;


TEST(Common, makeRegexpPatternFromGlobs)
{
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

    /// `**` (double-star / globstar) tests
    /// `**/` matches zero or more directory components
    EXPECT_EQ(makeRegexpPatternFromGlobs("**/file.txt"), "([^/]*/)*file\\.txt");
    EXPECT_EQ(makeRegexpPatternFromGlobs("data/**/file.txt"), "data/([^/]*/)*file\\.txt");
    EXPECT_EQ(makeRegexpPatternFromGlobs("data/**/sub/**/file.txt"), "data/([^/]*/)*sub/([^/]*/)*file\\.txt");
    /// `**` at end (not followed by `/`) keeps old behavior
    EXPECT_EQ(makeRegexpPatternFromGlobs("data/**"), "data/[^/]*[^{}]*");

    /// Runs of 3+ consecutive stars keep the legacy regex.
    /// The `**/` rewrite must not leak into these patterns, even when followed by `/`.
    EXPECT_EQ(makeRegexpPatternFromGlobs("***"), "[^/]*[^{}]*[^{}]*");
    EXPECT_EQ(makeRegexpPatternFromGlobs("a***b"), "a[^/]*[^{}]*[^{}]*b");
    EXPECT_EQ(makeRegexpPatternFromGlobs("****"), "[^/]*[^{}]*[^{}]*[^{}]*");
    EXPECT_EQ(makeRegexpPatternFromGlobs("***/file.txt"), "[^/]*[^{}]*[^{}]*/file\\.txt");
    EXPECT_EQ(makeRegexpPatternFromGlobs("a***/b"), "a[^/]*[^{}]*[^{}]*/b");

    /// `**` is a globstar only when it forms a whole path segment (bounded by `/` or string
    /// boundaries). A `**` adjacent to other characters in the segment (e.g. `?**`, `*?**`) is
    /// not a globstar: it keeps the legacy `*`-expansion. This matches the local file listing in
    /// `StorageFile`, which gives zero-level semantics only to a segment that is exactly `**`.
    EXPECT_EQ(makeRegexpPatternFromGlobs("*?**/file.txt"), "[^/]*[^/][^{}]*[^{}]*/file\\.txt");
    EXPECT_EQ(makeRegexpPatternFromGlobs("?**/file.txt"), "[^/][^/]*[^{}]*/file\\.txt");

    /// Verify that `**/` patterns actually match expected paths
    {
        re2::RE2 re(makeRegexpPatternFromGlobs("data/**/part1.tsv"));
        EXPECT_TRUE(RE2::FullMatch("data/part1.tsv", re));            /// zero directory levels
        EXPECT_TRUE(RE2::FullMatch("data/sub1/part1.tsv", re));       /// one directory level
        EXPECT_TRUE(RE2::FullMatch("data/a/b/part1.tsv", re));        /// two directory levels
        EXPECT_TRUE(RE2::FullMatch("data/a/b/c/part1.tsv", re));      /// three directory levels
        EXPECT_TRUE(RE2::FullMatch("data/{a}/part1.tsv", re));        /// directory name with braces
        EXPECT_TRUE(RE2::FullMatch("data/{a}/{b}/part1.tsv", re));    /// multiple brace-containing segments
        EXPECT_FALSE(RE2::FullMatch("data/part2.tsv", re));           /// wrong filename
        EXPECT_FALSE(RE2::FullMatch("other/part1.tsv", re));          /// wrong prefix
    }

    /// A non-whole-segment `?**/` is not a globstar, so it does not match at zero directory
    /// levels: the leading `?` requires at least one character in the directory component.
    {
        re2::RE2 re(makeRegexpPatternFromGlobs("data/?**/part1.tsv"));
        EXPECT_FALSE(RE2::FullMatch("data/part1.tsv", re));           /// zero directory levels: not matched
        EXPECT_TRUE(RE2::FullMatch("data/sub1/part1.tsv", re));       /// one directory level (name starts with any char)
    }
}

TEST(Common, expandSelectionGlob)
{
    EXPECT_EQ(expandSelectionGlob("file.csv"), std::vector<std::string>({"file.csv"}));
    EXPECT_EQ(expandSelectionGlob("file{1,2,3}.csv"), std::vector<std::string>({"file1.csv", "file2.csv", "file3.csv"}));
    EXPECT_EQ(expandSelectionGlob("{a}.csv"), std::vector<std::string>({"a.csv"}));
    EXPECT_EQ(expandSelectionGlob("{a,b}/{c,d}"), std::vector<std::string>({"a/c", "a/d", "b/c", "b/d"}));
    EXPECT_EQ(expandSelectionGlob("{a,,b}"), std::vector<std::string>({"a", "", "b"}));
    EXPECT_EQ(expandSelectionGlob("dir/{ab}{cd}/*.csv"), std::vector<std::string>({"dir/abcd/*.csv"}));

    /// A `{N..M}` range glob is not enumerated here: `makeRegexpPatternFromGlobs` turns it into a regexp.
    EXPECT_EQ(expandSelectionGlob("file{1..3}.csv"), std::vector<std::string>({"file{1..3}.csv"}));
    EXPECT_EQ(expandSelectionGlob("{a,b}file{1..3}.csv"), std::vector<std::string>({"{a,b}file{1..3}.csv"}));

    /// `{a,b}{c,d}{e,f}...` is a Cartesian product, so a short pattern must not be allowed to expand
    /// to an astronomical number of paths, or to an astronomical amount of data.
    auto repeat = [](const std::string & what, size_t times)
    {
        std::string result;
        for (size_t i = 0; i < times; ++i)
            result += what;
        return result;
    };

    /// Too many paths: 2^20 of them.
    EXPECT_THROW(expandSelectionGlob(repeat("{a,b}", 20)), DB::Exception);
    /// Too much data: 2^13 paths of 10 KiB each.
    EXPECT_THROW(expandSelectionGlob(std::string(10000, 'x') + repeat("{a,b}", 13)), DB::Exception);
    /// Too many globs: every group is a single element, so this expands to one path, but only after
    /// looking at every one of the 2000 groups.
    EXPECT_THROW(expandSelectionGlob(repeat("{ab}", 2000)), DB::Exception);

    /// A single group with an enormous number of alternatives - what a whole file passed as a path
    /// by `file(file(...))` looks like. The limit has to fire while the group is being scanned: if
    /// it were checked only after the group has been parsed, the parser would keep one offset and
    /// one `string_view` per alternative, so the memory it takes would grow with the size of the
    /// input even though nothing is ever expanded. The scan stops at the comma that makes the group
    /// exceed the limit, which is why the pattern below - a hundred times more alternatives than
    /// the limit allows - is rejected with the same message as a small one.
    std::string one_huge_group = "{" + repeat("a,", 10'000'000) + "a}";
    try
    {
        expandSelectionGlob(one_huge_group);
        FAIL() << "An oversized group was not rejected.";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_NE(std::string(e.message()).find("expand to more than"), std::string::npos) << e.message();
    }
}
