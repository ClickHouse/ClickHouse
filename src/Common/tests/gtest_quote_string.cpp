#include <gtest/gtest.h>

#include <Common/quoteString.h>

using namespace DB;

/// quoteStringPostgreSQL builds an E'...' escape-string constant for embedding a string in a query
/// sent to a PostgreSQL server. Both the single quote and the backslash are doubled, so an embedded
/// quote or backslash cannot terminate the literal early regardless of the remote server's
/// standard_conforming_strings — the SQL-injection class that a plain '...' literal leaves open on
/// the introspection/metadata queries (fetchPostgreSQLTableStructure).
TEST(QuoteString, PostgreSQLEscapeStringConstant)
{
    EXPECT_EQ(quoteStringPostgreSQL("plain"), "E'plain'");
    EXPECT_EQ(quoteStringPostgreSQL("it's"), "E'it''s'");
    EXPECT_EQ(quoteStringPostgreSQL("a\\b"), "E'a\\\\b'");
    /// A backslash followed by a quote must not be able to escape the quote and break out.
    EXPECT_EQ(quoteStringPostgreSQL("\\'"), "E'\\\\'''");
}
