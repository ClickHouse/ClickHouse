#include <gtest/gtest.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/LiteralEscapingStyle.h>
#include <IO/WriteBufferFromString.h>

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

}

TEST(LiteralEscapingStyle, TopLevelString)
{
    /// A backslash and a single quote: ClickHouse escapes both with a backslash, PostgreSQL uses
    /// doubling for the quote and keeps the backslash literal (the `E''` form is not used),
    /// SQLite only doubles the quote.
    Field value = std::string("a\\b'c");

    EXPECT_EQ(format(value, LiteralEscapingStyle::Regular), "'a\\\\b\\'c'");
    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "'a\\b''c'");
    EXPECT_EQ(format(value, LiteralEscapingStyle::SQLite), "'a\\b''c'");
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
    EXPECT_EQ(format(value, LiteralEscapingStyle::SQLite), "('it''s', 'a\tb')");
}

TEST(LiteralEscapingStyle, StringInsideSingleElementTuple)
{
    Tuple tuple;
    tuple.push_back(std::string("it's"));
    Field value = tuple;

    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "tuple('it''s')");
    EXPECT_EQ(format(value, LiteralEscapingStyle::SQLite), "tuple('it''s')");
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
    EXPECT_EQ(format(value, LiteralEscapingStyle::SQLite), "[('it''s', 'b'), 'c''d']");
}

TEST(LiteralEscapingStyle, NonStringElementsAreUnchanged)
{
    Tuple tuple;
    tuple.push_back(UInt64(1));
    tuple.push_back(std::string("it's"));
    Field value = tuple;

    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "(1, 'it''s')");
    EXPECT_EQ(format(value, LiteralEscapingStyle::SQLite), "(1, 'it''s')");
}

TEST(LiteralEscapingStyle, NulByteInsideContainerIsRejectedForSQLite)
{
    Tuple tuple;
    tuple.push_back(std::string("a\0b", 3));
    Field value = tuple;

    /// A NUL byte cannot be represented in a SQLite string literal, and it must not be silently
    /// emitted as the two characters `\` and `0` either.
    EXPECT_ANY_THROW(format(value, LiteralEscapingStyle::SQLite));
}
