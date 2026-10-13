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
  * `val IN ('it''s', 'x')` was sent to PostgreSQL with a backslash escape. Under the default
  * `standard_conforming_strings`, `\'` does not escape the quote there, so the embedded quote
  * terminated the literal.
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
    /// A backslash and a single quote: ClickHouse escapes both with a backslash, PostgreSQL keeps
    /// the backslash literal and doubles the quote.
    Field value = std::string("a\\b'c");

    EXPECT_EQ(format(value, LiteralEscapingStyle::Regular), "'a\\\\b\\'c'");
    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "'a\\b''c'");
}

TEST(LiteralEscapingStyle, StringInsideTuple)
{
    /// This is the shape produced by an `IN` list of two or more elements.
    Field value = Tuple{std::string("it's"), std::string("y")};

    EXPECT_EQ(format(value, LiteralEscapingStyle::Regular), "('it\\'s', 'y')");
    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "('it''s', 'y')");
}

TEST(LiteralEscapingStyle, StringInsideSingleElementTuple)
{
    /// A single-element tuple keeps the explicit `tuple(...)` form so it is parsed back as a tuple.
    Field value = Tuple{std::string("it's")};

    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "tuple('it''s')");
}

TEST(LiteralEscapingStyle, StringInsideArray)
{
    Field value = Array{std::string("it's"), std::string("y")};

    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "['it''s', 'y']");
}

TEST(LiteralEscapingStyle, StringInsideNestedContainers)
{
    /// The dialect has to survive an arbitrary number of container levels.
    Field value = Tuple{Array{Tuple{std::string("it's")}}};

    EXPECT_EQ(format(value, LiteralEscapingStyle::PostgreSQL), "tuple([tuple('it''s')])");
}

TEST(LiteralEscapingStyle, QuoteCannotEscapeTheLiteral)
{
    /// The whole point: a `'` in a container element must never be emitted as `\'` for PostgreSQL,
    /// because that does not escape the quote under `standard_conforming_strings`.
    Field value = Tuple{std::string("x'"), std::string("y")};

    const auto formatted = format(value, LiteralEscapingStyle::PostgreSQL);
    EXPECT_EQ(formatted.find("\\'"), std::string::npos) << formatted;
}
