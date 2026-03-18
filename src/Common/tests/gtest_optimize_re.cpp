#include <gtest/gtest.h>

#include <Common/OptimizedRegularExpression.h>

TEST(OptimizeRE, analyze)
{
    auto test_f = [](const std::string & regexp, const std::string & required, std::vector<std::string> expect_alternatives = {}, bool trival_expected = false, bool prefix_expected = false)
    {
        std::string answer;
        bool is_trivial;
        bool is_prefix;
        std::vector<std::string> alternatives;
        OptimizedRegularExpression::analyze(regexp, answer, is_trivial, is_prefix, alternatives);
        std::cerr << regexp << std::endl;
        EXPECT_EQ(required, answer);
        EXPECT_EQ(alternatives, expect_alternatives);
        EXPECT_EQ(is_trivial, trival_expected);
        EXPECT_EQ(is_prefix, prefix_expected);
    };
    test_f("abc", "abc", {}, true, true);
    test_f("c([^k]*)de", "");
    test_f("(?-s)bob", "bob", {}, false, true);
    test_f("(?s)bob", "bob", {}, false, true);
    test_f("(?ssss", "");
    test_f("abc(de)fg", "abcdefg", {}, false, true);
    test_f("abc(de|xyz)fg", "abc", {"abcdefg", "abcxyzfg"}, false, true);
    test_f("abc(de?f|xyz)fg", "abc", {"abcd", "abcxyzfg"}, false, true);
    test_f("abc|fgk|xyz", "", {"abc","fgk", "xyz"});
    test_f("(abc)", "abc", {}, false, true);
    test_f("(abc|fgk)", "", {"abc","fgk"});
    test_f("(abc|fgk)(e|f|zkh|)", "", {"abc","fgk"});
    test_f("abc(abc|fg)xyzz", "xyzz", {"abcabcxyzz","abcfgxyzz"});
    test_f("((abc|fg)kkk*)xyzz", "xyzz", {"abckk", "fgkk"});
    test_f("abc(*(abc|fg)*)xyzz", "xyzz");
    test_f("abc[k]xyzz", "xyzz");
    test_f("(abc[k]xyzz)", "xyzz");
    test_f("abc((de)fg(hi))jk", "abcdefghijk", {}, false, true);
    test_f("abc((?:de)fg(?:hi))jk", "abcdefghijk", {}, false, true);
    test_f("abc((de)fghi+zzz)jk", "abcdefghi", {}, false, true);
    test_f("abc((de)fg(hi))?jk", "abc", {}, false, true);
    test_f("abc((de)fghi?zzz)jk", "abcdefgh", {}, false, true);
    test_f("abc(*cd)jk", "cdjk");
    test_f(R"(abc(de|xyz|(\{xx\}))fg)", "abc", {"abcdefg", "abcxyzfg", "abc{xx}fg"}, false, true);
    test_f("abc(abc|fg)?xyzz", "xyzz");
    test_f("abc(abc|fg){0,1}xyzz", "xyzz");
    test_f("abc(abc|fg)xyzz|bcdd?k|bc(f|g|h?)z", "", {"abcabcxyzz", "abcfgxyzz", "bcd", "bc"});
    test_f("abc(abc|fg)xyzz|bc(dd?x|kk?y|(f))k|bc(f|g|h?)z", "", {"abcabcxyzz", "abcfgxyzz", "bcd", "bck", "bcfk", "bc"});
    test_f("((?:abc|efg|xyz)/[a-zA-Z0-9]{1-50})(/?[^ ]*|)", "", {"abc/", "efg/", "xyz/"});
    test_f(R"([Bb]ai[Dd]u[Ss]pider(?:-[A-Za-z]{1,30})(?:-[A-Za-z]{1,30}|)|bingbot|\bYeti(?:-[a-z]{1,30}|)|Catchpoint(?: bot|)|[Cc]harlotte|Daumoa(?:-feedfetcher|)|(?:[a-zA-Z]{1,30}-|)Googlebot(?:-[a-zA-Z]{1,30}|))", "", {"pider-", "bingbot", "Yeti-", "Yeti", "Catchpoint bot", "Catchpoint", "harlotte", "Daumoa-feedfetcher", "Daumoa", "-Googlebot", "Googlebot"});
    test_f("abc|(:?xx|yy|zz|x?)def", "", {"abc", "def"});
    test_f("abc|(:?xx|yy|zz|x?){1,2}def", "", {"abc", "def"});
    test_f(R"(\\A(?:(?:[-0-9_a-z]+(?:\\.[-0-9_a-z]+)*)/k8s1)\\z)", "/k8s1");
    test_f("[a-zA-Z]+(?P<num>\\d+)", "");
    test_f("[a-zA-Z]+(?<num>\\d+)", "");
    test_f("[a-zA-Z]+(?'num'\\d+)", "");
    test_f("[a-zA-Z]+(?x<num>\\d+)", "x<num>");
}
