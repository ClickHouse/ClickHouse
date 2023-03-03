#include <gtest/gtest.h>

#include <Common/OptimizedRegularExpression.h>

TEST(OptimizeRE, analyze)
{
    auto test_f = [](const std::string & regexp, const std::string & answer)
    {
        std::string required;
        bool is_trivial;
        bool is_prefix;
        OptimizedRegularExpression::analyze(regexp, required, is_trivial, is_prefix);
        EXPECT_EQ(required, answer);
    };
    test_f("abc", "abc");
    test_f("abc(de)fg", "abcdefg");
    test_f("abc(de|xyz)fg", "abc");
    test_f("abc(de?f|xyz)fg", "abc");
    test_f("abc|fg", "");
    test_f("(abc)", "abc");
    test_f("(abc|fg)", "");
    test_f("abc(abc|fg)xyzz", "xyzz");
    test_f("abc[k]xyzz", "xyzz");
    /// actually the best answer should be xyzz
    test_f("(abc[k]xyzz)", "abc");
    test_f("abc((de)fg(hi))jk", "abcdefghijk");
    test_f("abc((de)fghi+zzz)jk", "abcdefghi");
    test_f("abc((de)fg(hi))?jk", "abc");
    test_f("abc((de)fghi?zzz)jk", "abcdefgh");
    test_f("abc(*cd)jk", "abc");
}
