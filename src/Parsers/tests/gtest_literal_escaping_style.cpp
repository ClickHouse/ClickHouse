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
    /// A backslash and a single quote: ClickHouse escapes both with a backslash, PostgreSQL
    /// switches to the escape string constant form `E'...'` (a plain '...' literal would read a
    /// doubled backslash back as two characters under `standard_conforming_strings = on`, and as
    /// one under `off`), SQLite only doubles the quote and embeds the backslash literally.
    Field value = std::string("a\\b'c");

    EXPECT_EQ(format(value, LiteralEscapingStyle::Regular), "'a\\\\b\\'c'");
    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "E'a\\\\b''c'");
    EXPECT_SQLITE_EQ(value, "'a\\b''c'");
}

TEST(LiteralEscapingStyle, ControlCharactersRoundTripForPostgreSQL)
{
    /// A real tab and newline: PostgreSQL must read back exactly the original bytes. A plain
    /// '...' literal with `\t` / `\n` inside is read back as two characters each, so the
    /// `E'...'` form (whose escapes are interpreted on every PostgreSQL configuration) is used.
    Field value = std::string("a\tb\nc");

    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "E'a\\tb\\nc'");
    /// A string without quotes, backslashes and control characters keeps the regular form.
    EXPECT_EQ(format(Field(std::string("plain")), LiteralEscapingStyle::PostgreSQL), "'plain'");
}

TEST(LiteralEscapingStyle, StringInsideTuple)
{
    Tuple tuple;
    tuple.push_back(std::string("it's"));
    tuple.push_back(std::string("a\tb"));
    Field value = tuple;

    /// PostgreSQL doubles the quote, and the element with a real tab uses the `E'...'` form so
    /// the tab round-trips; SQLite embeds the tab literally. What matters here is that neither of
    /// them falls back to `\'` for the quote.
    EXPECT_EQ(format(value, LiteralEscapingStyle::Regular), "('it\\'s', 'a\\tb')");
    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "('it''s', E'a\\tb')");
    EXPECT_SQLITE_EQ(value, "('it''s', 'a\tb')");
}

TEST(LiteralEscapingStyle, SingleElementTupleIsRejectedForDialects)
{
    Tuple tuple;
    tuple.push_back(std::string("it's"));
    Field value = tuple;

    /// A single-element tuple can only be written back as `tuple(...)`, which is ClickHouse
    /// syntax: PostgreSQL / SQLite would fail to parse it. Without the fix it was emitted
    /// verbatim (e.g. into a user-provided `(SELECT ...)` table argument), so on the merge-base
    /// these expectations fail because formatting succeeds.
    EXPECT_EQ(format(value, LiteralEscapingStyle::Regular), "tuple('it\\'s')");
    EXPECT_ANY_THROW(format(value, LiteralEscapingStyle::PostgreSQL));
    EXPECT_ANY_THROW(formatSQLite(value));
}

TEST(LiteralEscapingStyle, ArrayIsRejectedForDialects)
{
    Tuple inner;
    inner.push_back(std::string("it's"));
    inner.push_back(std::string("b"));

    Array array;
    array.push_back(inner);
    array.push_back(std::string("c'd"));
    Field value = array;

    /// An `Array` literal has only the ClickHouse `[...]` text form, which PostgreSQL / SQLite
    /// cannot parse. Without the fix it was emitted verbatim, so on the merge-base these
    /// expectations fail because formatting succeeds.
    EXPECT_EQ(format(value, LiteralEscapingStyle::Regular), "[('it\\'s', 'b'), 'c\\'d']");
    EXPECT_ANY_THROW(format(value, LiteralEscapingStyle::PostgreSQL));
    EXPECT_ANY_THROW(formatSQLite(value));
}

TEST(LiteralEscapingStyle, MapIsRejectedForDialects)
{
    Tuple entry;
    entry.push_back(std::string("k"));
    entry.push_back(std::string("v"));

    Map map;
    map.push_back(entry);
    Field value = map;

    /// Same as for `Array`: a `Map` literal has no PostgreSQL / SQLite text form.
    EXPECT_ANY_THROW(format(value, LiteralEscapingStyle::PostgreSQL));
    EXPECT_ANY_THROW(formatSQLite(value));
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

TEST(LiteralEscapingStyle, NulByteInsideContainerIsRejectedForPostgreSQL)
{
    Tuple tuple;
    tuple.push_back(std::string("a\0b", 3));
    Field value = tuple;

    /// A NUL byte cannot appear in a PostgreSQL string value (E'\0' is rejected by the server),
    /// and it must not be silently emitted as the two characters `\` and `0` either. Such
    /// predicates are not pushed down; formatting one must fail explicitly.
    EXPECT_ANY_THROW(format(value, LiteralEscapingStyle::PostgreSQL));
}
