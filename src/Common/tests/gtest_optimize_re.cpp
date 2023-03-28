#include <gtest/gtest.h>

#include <Common/OptimizedRegularExpression.h>

TEST(OptimizeRE, analyze)
{
    auto test_f = [](const std::string & regexp, const std::string & answer, std::vector<std::string> expect_alternatives = {}, bool trival_expected = false)
    {
        std::string required;
        bool is_trivial;
        bool is_prefix;
        std::vector<std::string> alternatives;
        OptimizedRegularExpression::analyze(regexp, required, is_trivial, is_prefix, alternatives);
        std::cerr << regexp << std::endl;
        EXPECT_EQ(required, answer);
        EXPECT_EQ(alternatives, expect_alternatives);
        EXPECT_EQ(is_trivial, trival_expected);
    };
    test_f("abc", "abc", {}, true);
    test_f("c([^k]*)de", "");
    test_f("abc(de)fg", "abcdefg");
    test_f("abc(de|xyz)fg", "abc", {"abcdefg", "abcxyzfg"});
    test_f("abc(de?f|xyz)fg", "abc", {"abcd", "abcxyzfg"});
    test_f("abc|fgk|xyz", "", {"abc","fgk", "xyz"});
    test_f("(abc)", "abc");
    test_f("(abc|fgk)", "", {"abc","fgk"});
    test_f("(abc|fgk)(e|f|zkh|)", "", {"abc","fgk"});
    test_f("abc(abc|fg)xyzz", "xyzz", {"abcabcxyzz","abcfgxyzz"});
    test_f("abc[k]xyzz", "xyzz");
    test_f("(abc[k]xyzz)", "xyzz");
    test_f("abc((de)fg(hi))jk", "abcdefghijk");
    test_f("abc((?:de)fg(?:hi))jk", "abcdefghijk");
    test_f("abc((de)fghi+zzz)jk", "abcdefghi");
    test_f("abc((de)fg(hi))?jk", "abc");
    test_f("abc((de)fghi?zzz)jk", "abcdefgh");
    test_f("abc(*cd)jk", "cdjk");
    test_f(R"(abc(de|xyz|(\{xx\}))fg)", "abc", {"abcdefg", "abcxyzfg", "abc{xx}fg"});
    test_f("abc(abc|fg)?xyzz", "xyzz");
    test_f("abc(abc|fg){0,1}xyzz", "xyzz");
    test_f("abc(abc|fg)xyzz|bcdd?k|bc(f|g|h?)z", "", {"abcabcxyzz", "abcfgxyzz", "bcd", "bc"});
    test_f("abc(abc|fg)xyzz|bc(dd?x|kk?y|(f))k|bc(f|g|h?)z", "", {"abcabcxyzz", "abcfgxyzz", "bcd", "bck", "bcfk", "bc"});
    test_f("((?:abc|efg|xyz)/[a-zA-Z0-9]{1-50})(/?[^ ]*|)", "", {"abc/", "efg/", "xyz/"});
    test_f(R"([Bb]ai[Dd]u[Ss]pider(?:-[A-Za-z]{1,30})(?:-[A-Za-z]{1,30}|)|bingbot|\bYeti(?:-[a-z]{1,30}|)|Catchpoint(?: bot|)|[Cc]harlotte|Daumoa(?:-feedfetcher|)|(?:[a-zA-Z]{1,30}-|)Googlebot(?:-[a-zA-Z]{1,30}|))", "", {"pider-", "bingbot", "Yeti-", "Yeti", "Catchpoint bot", "Catchpoint", "harlotte", "Daumoa-feedfetcher", "Daumoa", "-Googlebot", "Googlebot"});
    test_f("abc|(:?xx|yy|zz|x?)def", "", {"abc", "def"});
    test_f("abc|(:?xx|yy|zz|x?){1,2}def", "", {"abc", "def"});
}
