#include <gtest/gtest.h>

#include <Common/OptimizedRegularExpression.h>

TEST(OptimizeRE, analyze)
{
    auto test_f = [](const std::string & regexp,
                     const std::string & required,
                     std::vector<std::string> expect_alternatives = {},
                     bool trival_expected = false,
                     bool has_capture_expected = false,
                     bool prefix_expected = false)
    {
        auto [answer, is_trivial, has_capture, is_prefix, alternatives] = DB::OptimizedRegularExpression::analyze(regexp);
        std::cerr << regexp << std::endl;
        EXPECT_EQ(required, answer);
        EXPECT_EQ(alternatives, expect_alternatives);
        EXPECT_EQ(is_trivial, trival_expected);
        EXPECT_EQ(has_capture, has_capture_expected);
        EXPECT_EQ(is_prefix, prefix_expected);
    };
    test_f("abc", "abc", {}, true, false, true);
    test_f("c([^k]*)de", "", {}, false, true, false);
    test_f("a|XYZ", "", {"a", "XYZ"});
    test_f("XYZ|a", "", {"XYZ", "a"});
    test_f("[Ff]|XYZ", "", {"", "XYZ"});
    test_f("XYZ|[Ff]", "", {"XYZ", ""});
    test_f("XYZ|ABC|[Ff]", "", {"XYZ", "ABC", ""});
    test_f("(?-s)bob", "bob", {}, false, true, true);
    test_f("(?s)bob", "bob", {}, false, true, true);
    test_f("(?ssss", "");
    test_f("[asdf]ss(?:ss)ss", "ssssss");
    test_f("abc(de)fg", "abcdefg", {}, false, true, true);
    test_f("abc(de|xyz)fg", "abc", {"abcdefg", "abcxyzfg"}, false, true, true);
    test_f("abc(de?f|xyz)fg", "abc", {"abcd", "abcxyzfg"}, false, true, true);
    test_f("abc|fgk|xyz", "", {"abc","fgk", "xyz"});
    test_f("(abc)", "abc", {}, false, true, true);
    test_f("(abc|fgk)", "", {"abc","fgk"}, false, true, false);
    test_f("(abc|fgk)(e|f|zkh|)", "", {"abc","fgk"}, false, true, false);
    test_f("abc(abc|fg)xyzz", "xyzz", {"abcabcxyzz","abcfgxyzz"}, false, true, false);
    test_f("((abc|fg)kkk*)xyzz", "xyzz", {"abckk", "fgkk"}, false, true, false);
    test_f("abc(*(abc|fg)*)xyzz", "xyzz", {}, false, true, false);
    test_f("abc[k]xyzz", "xyzz");
    test_f("(abc[k]xyzz)", "xyzz", {}, false, true, false);
    test_f("abc((de)fg(hi))jk", "abcdefghijk", {}, false, true, true);
    test_f("abc((?:de)fg(?:hi))jk", "abcdefghijk", {}, false, true, true);
    test_f("abc((de)fghi+zzz)jk", "abcdefghi", {}, false, true, true);
    test_f("abc((de)fg(hi))?jk", "abc", {}, false, true, true);
    test_f("abc((de)fghi?zzz)jk", "abcdefgh", {}, false, true, true);
    test_f("abc(*cd)jk", "cdjk", {}, false, true, false);
    test_f(R"(abc(de|xyz|(\{xx\}))fg)", "abc", {"abcdefg", "abcxyzfg", "abc{xx}fg"}, false, true, true);
    test_f("abc(abc|fg)?xyzz", "xyzz", {}, false, true, false);
    test_f("abc(abc|fg){0,1}xyzz", "xyzz", {}, false, true, false);
    test_f("abc(abc|fg)xyzz|bcdd?k|bc(f|g|h?)z", "", {"abcabcxyzz", "abcfgxyzz", "bcd", "bc"});
    test_f("abc(abc|fg)xyzz|bc(dd?x|kk?y|(f))k|bc(f|g|h?)z", "", {"abcabcxyzz", "abcfgxyzz", "bcd", "bck", "bcfk", "bc"}, false, true, false);
    test_f("((?:abc|efg|xyz)/[a-zA-Z0-9]{1-50})(/?[^ ]*|)", "", {"abc/", "efg/", "xyz/"}, false, true, false);
    test_f(R"([Bb]ai[Dd]u[Ss]pider(?:-[A-Za-z]{1,30})(?:-[A-Za-z]{1,30}|)|bingbot|\bYeti(?:-[a-z]{1,30}|)|Catchpoint(?: bot|)|[Cc]harlotte|Daumoa(?:-feedfetcher|)|(?:[a-zA-Z]{1,30}-|)Googlebot(?:-[a-zA-Z]{1,30}|))", "", {"pider-", "bingbot", "Yeti-", "Yeti", "Catchpoint bot", "Catchpoint", "harlotte", "Daumoa-feedfetcher", "Daumoa", "-Googlebot", "Googlebot"});
    test_f("abc|(:?xx|yy|zz|x?)def", "", {"abc", "def"});
    test_f("abc|(:?xx|yy|zz|x?){1,2}def", "", {"abc", "def"});
    test_f(R"(\\A(?:(?:[-0-9_a-z]+(?:\\.[-0-9_a-z]+)*)/k8s1)\\z)", "/k8s1");
    test_f("[a-zA-Z]+(?P<num>\\d+)", "", {}, false, true, false);
    test_f("[a-zA-Z]+(?<num>\\d+)", "", {}, false, true, false);
    test_f("[a-zA-Z]+(?'num'\\d+)", "", {}, false, true, false);
    test_f("[a-zA-Z]+(?x<num>\\d+)", "x<num>", {}, false, true, false);
}
