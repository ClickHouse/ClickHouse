#include <gtest/gtest.h>

#include <Common/quoteString.h>

using namespace DB;

/// `quoteStringPostgreSQL` is what the PostgreSQL metadata and introspection queries embed
/// attacker-influenced names with (schema, table, column, publication). The property that matters is
/// that no embedded byte can terminate the literal early: the quote is always doubled, and a value
/// carrying a backslash switches to the `E'...'` escape-string constant form, where the backslash is
/// doubled too. Both forms read back the original bytes irrespective of the remote server's
/// `standard_conforming_strings`, which is what `quoteString`'s backslash escaping fails to do.
TEST(QuoteString, PostgreSQL)
{
    EXPECT_EQ(quoteStringPostgreSQL("plain"), "'plain'");
    EXPECT_EQ(quoteStringPostgreSQL("it's"), "'it''s'");

    /// A backslash forces the E'...' form, where it is doubled.
    EXPECT_EQ(quoteStringPostgreSQL("a\\b"), "E'a\\\\b'");

    /// The break-out attempt: a backslash before the quote must not escape the quote. In the E'...'
    /// form the backslash is doubled and the quote is still doubled, so the literal survives.
    EXPECT_EQ(quoteStringPostgreSQL("\\'"), "E'\\\\'''");

    /// The shape from the `CREATE DATABASE ... PostgreSQL(collection, schema = ...)` injection:
    /// the payload must come back as one literal, not as a literal followed by statements.
    EXPECT_EQ(quoteStringPostgreSQL("public'; DROP TABLE t; -- "), "'public''; DROP TABLE t; -- '");
}
