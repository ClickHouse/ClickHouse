#include <gtest/gtest.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/LiteralEscapingStyle.h>
#include <IO/WriteBufferFromString.h>

#include <optional>
#include <string>

/** Regression coverage for `LiteralEscapingStyle` applied to nested literals.
  *
  * When a literal is formatted for an external database, the escaping style of the target dialect
  * has to be used all the way down: a string nested inside an `Array` / `Tuple` / `Map` (for
  * example the elements of a pushed-down `IN` list) must be escaped with the same rules as a
  * top-level string. Previously only the top-level `String` case was overridden and container
  * elements fell back to the regular ClickHouse escaping, so a predicate like
  * `val IN ('it''s', 'a\tb')` was sent to PostgreSQL or SQLite with backslash escapes that those
  * dialects interpret differently or reject.
  */

namespace
{

using namespace DB;

std::string format(const Field & value, LiteralEscapingStyle style)
{
    ASTLiteral literal(value);
    WriteBufferFromOwnString buf;
    IAST::FormatSettings settings(
        /* one_line_= */ true,
        IdentifierQuotingRule::WhenNecessary,
        IdentifierQuotingStyle::Backticks,
        /* show_secrets_= */ true,
        style);
    IAST::FormatState state;
    IAST::FormatStateStacked frame;
    literal.format(buf, settings, state, frame);
    return buf.str();
}

/** The bugfix validation CI job compiles this test against the merge-base sources, where
  * `LiteralEscapingStyle::SQLite` does not exist yet. Keep every reference to that enumerator
  * dependent on a template parameter, so the test compiles there (the SQLite expectations
  * evaporate) while the PostgreSQL expectations still reproduce the nested-literal bug.
  */
template <typename Style = LiteralEscapingStyle>
std::optional<std::string> formatSQLite(const Field & value)
{
    if constexpr (requires { Style::SQLite; })
        return format(value, Style::SQLite);
    else
        return std::nullopt;
}

#define EXPECT_SQLITE_EQ(value, expected) \
    do \
    { \
        if (auto formatted = formatSQLite((value))) \
            EXPECT_EQ(*formatted, (expected)); \
    } while (false)

}

TEST(LiteralEscapingStyle, TopLevelString)
{
    /// A backslash and a single quote: ClickHouse escapes both with a backslash, PostgreSQL uses
    /// doubling for the quote and keeps the backslash literal (the `E''` form is not used),
    /// SQLite only doubles the quote.
    Field value = std::string("a\\b'c");

    EXPECT_EQ(format(value, LiteralEscapingStyle::Regular), "'a\\\\b\\'c'");
    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "'a\\b''c'");
    EXPECT_SQLITE_EQ(value, "'a\\b''c'");
}

TEST(LiteralEscapingStyle, StringInsideTuple)
{
    Tuple tuple;
    tuple.push_back(std::string("it's"));
    tuple.push_back(std::string("a\tb"));
    Field value = tuple;

    /// PostgreSQL doubles the quote and keeps the ClickHouse escape for the tab; SQLite embeds the
    /// tab literally. What matters here is that neither of them falls back to `\'` for the quote.
    EXPECT_EQ(format(value, LiteralEscapingStyle::Regular), "('it\\'s', 'a\\tb')");
    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "('it''s', 'a\\tb')");
    EXPECT_SQLITE_EQ(value, "('it''s', 'a\tb')");
}

TEST(LiteralEscapingStyle, StringInsideSingleElementTuple)
{
    Tuple tuple;
    tuple.push_back(std::string("it's"));
    Field value = tuple;

    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "tuple('it''s')");
    EXPECT_SQLITE_EQ(value, "tuple('it''s')");
}

TEST(LiteralEscapingStyle, StringInsideNestedContainers)
{
    Tuple inner;
    inner.push_back(std::string("it's"));
    inner.push_back(std::string("b"));

    Array array;
    array.push_back(inner);
    array.push_back(std::string("c'd"));
    Field value = array;

    EXPECT_EQ(format(value, LiteralEscapingStyle::Regular), "[('it\\'s', 'b'), 'c\\'d']");
    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "[('it''s', 'b'), 'c''d']");
    EXPECT_SQLITE_EQ(value, "[('it''s', 'b'), 'c''d']");
}

TEST(LiteralEscapingStyle, NonStringElementsAreUnchanged)
{
    Tuple tuple;
    tuple.push_back(UInt64(1));
    tuple.push_back(std::string("it's"));
    Field value = tuple;

    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "(1, 'it''s')");
    EXPECT_SQLITE_EQ(value, "(1, 'it''s')");
}

TEST(LiteralEscapingStyle, NulByteInsideContainerIsRejectedForSQLite)
{
    Tuple tuple;
    tuple.push_back(std::string("a\0b", 3));
    Field value = tuple;

    /// A NUL byte cannot be represented in a SQLite string literal, and it must not be silently
    /// emitted as the two characters `\` and `0` either. Without the fix `formatSQLite` returns
    /// an empty optional instead of throwing, so this expectation also fails on the merge-base.
    EXPECT_ANY_THROW(formatSQLite(value));
}
