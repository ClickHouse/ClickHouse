#include <gtest/gtest.h>

#include <Common/OptimizedRegularExpression.h>

TEST(OptimizeRE, analyze)
{
    auto test_f = [](const std::string & regexp, const std::string & answer, std::vector<std::string> expect_alternatives = {})
    {
        std::string required;
        bool is_trivial;
        bool is_prefix;
        std::vector<std::string> alternatives;
        OptimizedRegularExpression::analyze(regexp, regexp.data(), required, is_trivial, is_prefix, alternatives);
        std::cerr << regexp << std::endl;
        EXPECT_EQ(required, answer);
        EXPECT_EQ(alternatives, expect_alternatives);
    };
    test_f("abc", "abc");
    test_f("abc(de)fg", "abcdefg");
    test_f("abc(de|xyz)fg", "abc", {"de", "xyz"});
    test_f("abc(de?f|xyz)fg", "abc", {"d", "xyz"});
    test_f("abc|fgk|xyz", "", {"abc","fgk", "xyz"});
    test_f("(abc)", "abc");
    test_f("(abc|fgk)", "", {"abc","fgk"});
    test_f("abc(abc|fg)xyzz", "xyzz", {"abc","fg"});
    test_f("abc[k]xyzz", "xyzz");
    test_f("(abc[k]xyzz)", "xyzz");
    test_f("abc((de)fg(hi))jk", "abcdefghijk");
    test_f("abc((de)fghi+zzz)jk", "abcdefghi");
    test_f("abc((de)fg(hi))?jk", "abc");
    test_f("abc((de)fghi?zzz)jk", "abcdefgh");
    test_f("abc(*cd)jk", "abc");
    test_f(R"(abc(de|xyz|(\{xx\}))fg)", "abc", {"de", "xyz", "{xx}"});
    test_f("abc(abc|fg)?xyzz", "xyzz");
    test_f("abc(abc|fg){0,1}xyzz", "xyzz");
    test_f("abc(abc|fg)xyzz|bcdd?k|bc(f|g|h?)z", "", {"abc", "fg", "bcd", "bc"});
    test_f("abc(abc|fg)xyzz|bc(dd?x|kk?y|(f))k|bc(f|g|h?)z", "", {"abc", "fg", "d", "k", "f", "bc"});
    test_f("((?:abc|efg|xyz)/[a-zA-Z0-9]{1-50})(/?[^ ]*|)", "", {"abc", "efg", "xyz"});
}
