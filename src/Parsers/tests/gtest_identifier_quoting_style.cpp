#include <gtest/gtest.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <IO/WriteBufferFromString.h>

#include <string>

/** Coverage for `IdentifierQuotingStyle` applied to an identifier sent to an external database.
  *
  * PostgreSQL gives a backslash no meaning inside a quoted identifier - there the only escape is a
  * doubled `"` - so `DoubleQuotes`, which emits an embedded `"` as `\"`, closes the identifier at
  * that quote and PostgreSQL parses the remainder as SQL of its own. The PostgreSQL read path hands
  * its query to `pqxx::stream_from`, i.e. `COPY (<query>) TO STDOUT` over the simple-query protocol,
  * where every `;`-separated statement executes, so a remote table name could run arbitrary
  * read-only SQL on the server. `DoubleQuotesPostgreSQL` escapes the way PostgreSQL reads.
  *
  * `DoubleQuotesPostgreSQL` is deliberately absent from the
  * `show_create_query_identifier_quoting_style` value map, so no SQL-visible setting selects it.
  * Every case below also asserts the `DoubleQuotes` output, which must stay exactly as it was for
  * its other consumers (that setting, SQLite, Cassandra and the ODBC/JDBC bridge).
  */

namespace
{

using namespace DB;

std::string format(const std::string & name, IdentifierQuotingStyle style, IdentifierQuotingRule rule)
{
    ASTIdentifier identifier(name);
    WriteBufferFromOwnString buf;
    IAST::FormatSettings settings(/* one_line_= */ true, rule, style);
    IAST::FormatState state;
    IAST::FormatStateStacked frame;
    identifier.format(buf, settings, state, frame);
    return buf.str();
}

std::string formatAlways(const std::string & name, IdentifierQuotingStyle style)
{
    return format(name, style, IdentifierQuotingRule::Always);
}

}

TEST(IdentifierQuotingStyle, PlainNameIsUnaffectedByTheDialect)
{
    EXPECT_EQ(formatAlways("plain", IdentifierQuotingStyle::DoubleQuotes), R"("plain")");
    EXPECT_EQ(formatAlways("plain", IdentifierQuotingStyle::DoubleQuotesPostgreSQL), R"("plain")");
}

TEST(IdentifierQuotingStyle, EmbeddedQuoteIsDoubledForPostgreSQL)
{
    EXPECT_EQ(formatAlways("a\"b", IdentifierQuotingStyle::DoubleQuotes), R"("a\"b")");
    EXPECT_EQ(formatAlways("a\"b", IdentifierQuotingStyle::DoubleQuotesPostgreSQL), R"("a""b")");
}

TEST(IdentifierQuotingStyle, BackslashIsLiteralForPostgreSQL)
{
    /// A relation genuinely named `a\` is reachable only when the backslash is passed through as
    /// one byte: doubling it makes PostgreSQL look up the two-character name `a\\` instead.
    EXPECT_EQ(formatAlways("a\\", IdentifierQuotingStyle::DoubleQuotes), R"("a\\")");
    EXPECT_EQ(formatAlways("a\\", IdentifierQuotingStyle::DoubleQuotesPostgreSQL), R"("a\")");
}

TEST(IdentifierQuotingStyle, StatementBreakOutPayloadStaysASingleIdentifier)
{
    const std::string payload = R"(a") TO STDOUT; SELECT 1; --)";

    /// Under `DoubleQuotes` the payload's own quote ends the identifier, so `; SELECT 1; --`
    /// leaves the identifier and becomes a statement PostgreSQL executes.
    EXPECT_EQ(formatAlways(payload, IdentifierQuotingStyle::DoubleQuotes), R"("a\") TO STDOUT; SELECT 1; --")");
    EXPECT_EQ(
        formatAlways(payload, IdentifierQuotingStyle::DoubleQuotesPostgreSQL),
        R"("a"") TO STDOUT; SELECT 1; --")");
}

TEST(IdentifierQuotingStyle, WhenNecessaryQuotesOnlyWhatNeedsQuoting)
{
    /// `WhenNecessary` is the rule the re-serialized engine/table-function argument is formatted
    /// with, so the dialect needs a correct `writeProbably...` writer too, not only the quoting one.
    EXPECT_EQ(format("plain", IdentifierQuotingStyle::DoubleQuotesPostgreSQL, IdentifierQuotingRule::WhenNecessary), "plain");
    EXPECT_EQ(
        format("a\"b", IdentifierQuotingStyle::DoubleQuotesPostgreSQL, IdentifierQuotingRule::WhenNecessary),
        R"("a""b")");
}
