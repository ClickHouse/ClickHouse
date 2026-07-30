#include <gtest/gtest.h>

#include <Common/RegexpUtils.h>


namespace
{

void checkImpl(std::string_view regexp, bool requires_perfect_prefix, DB::RegexpFixedPrefix expected)
{
    SCOPED_TRACE(regexp);
    auto result = DB::extractFixedPrefixFromRegularExpression(regexp, requires_perfect_prefix);
    EXPECT_EQ(result.prefix, expected.prefix);
    EXPECT_EQ(result.is_perfect, expected.is_perfect);
    EXPECT_EQ(result.is_exact, expected.is_exact);
}

void check(std::string_view regexp, DB::RegexpFixedPrefix expected)
{
    checkImpl(regexp, /* requires_perfect_prefix = */ false, expected);
}

void checkPerfectPrefix(std::string_view regexp, DB::RegexpFixedPrefix expected)
{
    checkImpl(regexp, /* requires_perfect_prefix = */ true, expected);
}

}

TEST(RegexpUtils, extractFixedPrefix)
{
    /// A literal pattern matches every string starting with it.
    check("^abc", {.prefix = "abc", .is_perfect = true});
    check("^a", {.prefix = "a", .is_perfect = true});
    check("^abc.*", {.prefix = "abc", .is_perfect = true});
    check("^abc.*$", {.prefix = "abc", .is_perfect = true});
    check("^.*", {.prefix = "", .is_perfect = true});

    /// Escapes of special characters are literals too.
    check("^abc\\.", {.prefix = "abc.", .is_perfect = true});
    check("^abc\\|def", {.prefix = "abc|def", .is_perfect = true});
    check("^abc\\\\", {.prefix = "abc\\", .is_perfect = true});
    check("^abc\\$", {.prefix = "abc$", .is_perfect = true});

    /// A trailing '$' makes the pattern an exact match.
    check("^abc$", {.prefix = "abc", .is_exact = true});
    check("^abc\\.$", {.prefix = "abc.", .is_exact = true});
    check("^$", {.prefix = "", .is_exact = true});

    /// Constructs which stop the scan.
    check("^abc.*def", {.prefix = "abc"});
    check("^abc.", {.prefix = "abc"});
    check("^abc\\d", {.prefix = "abc"});
    check("^abc\\n", {.prefix = "abc"});
    check("^abc[12]", {.prefix = "abc"});
    check("^abc+", {.prefix = "abc"});
    check("^ab$cd", {.prefix = "ab"});
    check("^a^b", {.prefix = "a"});
    check(std::string_view{"^abc\0def", 8}, {.prefix = "abc"});

    /// An invalid pattern must not be reported as exact, otherwise a point range could hide
    /// the exception the matcher throws.
    check("^abc\\", {.prefix = "abc"});
    check("^abc)", {.prefix = "abc"});
    check("^abc)$", {.prefix = "abc"});

    /// Quantifiers which allow a zero number of occurrences make the previous character optional.
    check("^abc*", {.prefix = "ab"});
    check("^abc?", {.prefix = "ab"});
    check("^abc{2}", {.prefix = "ab"});
    check("^a*", {});

    /// Groups are not analyzed, except a top-level alternation.
    check("^(abc)", {});
    check("^(?:abc)", {});

    /// Patterns without a '^' anchor guarantee nothing.
    check("", {});
    check("^", {});
    check("abc", {});
    check(".*abc", {});
}

TEST(RegexpUtils, extractFixedPrefixFromAlternation)
{
    /// The common prefix of all branches is never perfect: "abc-zz" does not match "^(abc-xx|abc-yy)".
    check("^(abc-xx|abc-yy)", {.prefix = "abc-"});
    check("^(abc-xx|abc-yy)$", {.prefix = "abc-"});
    check("^(abc-xx-1|abc-xx-2|abc-yy-1)", {.prefix = "abc-"});
    check("^(abc\\(1|abc\\(2)", {.prefix = "abc("});
    check("^(abc|abc)", {.prefix = "abc"});

    /// Branches without a common prefix.
    check("^(abc|def)", {});
    check("^(abc|)", {});

    /// Alternations which are not a single group of literals.
    check("^abc|def", {});
    check("^abc|^abd", {});
    check("^(abc|abd)+", {});
    check("^(abc|abd)x", {});
    check("^(abc|abd", {});
    check("^(ab.*|ab.)", {});
    check("^(abc(1|2)|abc(3|4))", {});
}

TEST(RegexpUtils, extractPerfectPrefix)
{
    /// A perfect or an exact prefix is returned as is.
    checkPerfectPrefix("^abc", {.prefix = "abc", .is_perfect = true});
    checkPerfectPrefix("^abc.*", {.prefix = "abc", .is_perfect = true});
    checkPerfectPrefix("^abc.*$", {.prefix = "abc", .is_perfect = true});
    checkPerfectPrefix("^abc$", {.prefix = "abc", .is_exact = true});
    checkPerfectPrefix("^$", {.prefix = "", .is_exact = true});

    /// Everything else gives no prefix at all, even when a shorter non-perfect one could be extracted.
    checkPerfectPrefix("^abc[12]", {});
    checkPerfectPrefix("^abc\\d", {});
    checkPerfectPrefix("^abc*", {});
    checkPerfectPrefix("^abc)$", {});
    checkPerfectPrefix("^(abc-xx|abc-yy)", {});
}
