#include <gtest/gtest.h>

#include <Common/RegexpUtils.h>


namespace
{

void checkImpl(
    std::string_view regexp,
    bool requires_perfect_prefix,
    std::string_view expected_prefix,
    bool expected_is_perfect,
    bool expected_is_exact)
{
    SCOPED_TRACE(regexp);
    auto prefix = DB::extractFixedPrefixFromRegularExpression(regexp, requires_perfect_prefix);
    EXPECT_EQ(prefix.prefix, expected_prefix);
    EXPECT_EQ(prefix.is_perfect, expected_is_perfect);
    EXPECT_EQ(prefix.is_exact, expected_is_exact);
}

void check(std::string_view regexp, std::string_view expected_prefix, bool expected_is_perfect, bool expected_is_exact)
{
    checkImpl(regexp, /* requires_perfect_prefix = */ false, expected_prefix, expected_is_perfect, expected_is_exact);
}

void checkPerfectPrefix(std::string_view regexp, std::string_view expected_prefix, bool expected_is_perfect, bool expected_is_exact)
{
    checkImpl(regexp, /* requires_perfect_prefix = */ true, expected_prefix, expected_is_perfect, expected_is_exact);
}

}

TEST(RegexpUtils, extractFixedPrefix)
{
    /// A literal pattern matches every string starting with it.
    check("^abc", "abc", /* perfect = */ true, /* exact = */ false);
    check("^a", "a", /* perfect = */ true, /* exact = */ false);
    check("^abc.*", "abc", /* perfect = */ true, /* exact = */ false);
    check("^abc.*$", "abc", /* perfect = */ true, /* exact = */ false);
    check("^.*", "", /* perfect = */ true, /* exact = */ false);

    /// Escapes of special characters are literals too.
    check("^abc\\.", "abc.", /* perfect = */ true, /* exact = */ false);
    check("^abc\\|def", "abc|def", /* perfect = */ true, /* exact = */ false);
    check("^abc\\\\", "abc\\", /* perfect = */ true, /* exact = */ false);
    check("^abc\\$", "abc$", /* perfect = */ true, /* exact = */ false);

    /// A trailing '$' makes the pattern an exact match.
    check("^abc$", "abc", /* perfect = */ false, /* exact = */ true);
    check("^abc\\.$", "abc.", /* perfect = */ false, /* exact = */ true);
    check("^$", "", /* perfect = */ false, /* exact = */ true);

    /// Constructs which stop the scan.
    check("^abc.*def", "abc", /* perfect = */ false, /* exact = */ false);
    check("^abc.", "abc", /* perfect = */ false, /* exact = */ false);
    check("^abc\\d", "abc", /* perfect = */ false, /* exact = */ false);
    check("^abc\\n", "abc", /* perfect = */ false, /* exact = */ false);
    check("^abc[12]", "abc", /* perfect = */ false, /* exact = */ false);
    check("^abc+", "abc", /* perfect = */ false, /* exact = */ false);
    check("^ab$cd", "ab", /* perfect = */ false, /* exact = */ false);
    check("^a^b", "a", /* perfect = */ false, /* exact = */ false);
    check(std::string_view{"^abc\0def", 8}, "abc", /* perfect = */ false, /* exact = */ false);

    /// A trailing escape is an invalid pattern, so it must not be reported as exact.
    check("^abc\\", "abc", /* perfect = */ false, /* exact = */ false);

    /// Quantifiers which allow a zero number of occurrences make the previous character optional.
    check("^abc*", "ab", /* perfect = */ false, /* exact = */ false);
    check("^abc?", "ab", /* perfect = */ false, /* exact = */ false);
    check("^abc{2}", "ab", /* perfect = */ false, /* exact = */ false);
    check("^a*", "", /* perfect = */ false, /* exact = */ false);

    /// Groups are not analyzed, except a top-level alternation.
    check("^(abc)", "", /* perfect = */ false, /* exact = */ false);
    check("^(?:abc)", "", /* perfect = */ false, /* exact = */ false);

    /// Patterns without a '^' anchor guarantee nothing.
    check("", "", /* perfect = */ false, /* exact = */ false);
    check("^", "", /* perfect = */ false, /* exact = */ false);
    check("abc", "", /* perfect = */ false, /* exact = */ false);
    check(".*abc", "", /* perfect = */ false, /* exact = */ false);
}

TEST(RegexpUtils, extractFixedPrefixFromAlternation)
{
    /// The common prefix of all branches is never perfect: "abc-zz" does not match "^(abc-xx|abc-yy)".
    check("^(abc-xx|abc-yy)", "abc-", /* perfect = */ false, /* exact = */ false);
    check("^(abc-xx|abc-yy)$", "abc-", /* perfect = */ false, /* exact = */ false);
    check("^(abc-xx-1|abc-xx-2|abc-yy-1)", "abc-", /* perfect = */ false, /* exact = */ false);
    check("^(abc\\(1|abc\\(2)", "abc(", /* perfect = */ false, /* exact = */ false);
    check("^(abc|abc)", "abc", /* perfect = */ false, /* exact = */ false);

    /// Branches without a common prefix.
    check("^(abc|def)", "", /* perfect = */ false, /* exact = */ false);
    check("^(abc|)", "", /* perfect = */ false, /* exact = */ false);

    /// Alternations which are not a single group of literals.
    check("^abc|def", "", /* perfect = */ false, /* exact = */ false);
    check("^abc|^abd", "", /* perfect = */ false, /* exact = */ false);
    check("^(abc|abd)+", "", /* perfect = */ false, /* exact = */ false);
    check("^(abc|abd)x", "", /* perfect = */ false, /* exact = */ false);
    check("^(abc|abd", "", /* perfect = */ false, /* exact = */ false);
    check("^(ab.*|ab.)", "", /* perfect = */ false, /* exact = */ false);
    check("^(abc(1|2)|abc(3|4))", "", /* perfect = */ false, /* exact = */ false);
}

TEST(RegexpUtils, extractPerfectPrefix)
{
    /// A perfect or an exact prefix is returned as is.
    checkPerfectPrefix("^abc", "abc", /* perfect = */ true, /* exact = */ false);
    checkPerfectPrefix("^abc.*", "abc", /* perfect = */ true, /* exact = */ false);
    checkPerfectPrefix("^abc.*$", "abc", /* perfect = */ true, /* exact = */ false);
    checkPerfectPrefix("^abc$", "abc", /* perfect = */ false, /* exact = */ true);
    checkPerfectPrefix("^$", "", /* perfect = */ false, /* exact = */ true);

    /// Everything else gives no prefix at all, even when a shorter non-perfect one could be extracted.
    checkPerfectPrefix("^abc[12]", "", /* perfect = */ false, /* exact = */ false);
    checkPerfectPrefix("^abc\\d", "", /* perfect = */ false, /* exact = */ false);
    checkPerfectPrefix("^abc*", "", /* perfect = */ false, /* exact = */ false);
    checkPerfectPrefix("^(abc-xx|abc-yy)", "", /* perfect = */ false, /* exact = */ false);
}
