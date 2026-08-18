#include <Parsers/ParserUseQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/StatementFactory.h>


namespace DB
{

bool ParserUseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_use(Keyword::USE);
    ParserKeyword s_database(Keyword::DATABASE);
    ParserIdentifier name_p{/*allow_query_parameter*/ true};

    if (!s_use.ignore(pos, expected))
        return false;

    ASTPtr database;
    Expected test_expected;

    /// test if we have DATABASE <identifier> pattern without moving pos
    Pos test_pos = pos;
    ASTPtr test_node;

    bool has_database_keyword_pattern =
        s_database.parse(test_pos, test_node, test_expected) &&
        name_p.parse(test_pos, test_node, test_expected);

    // now the actual parsing
    if (has_database_keyword_pattern)
    {
        // Parse DATABASE <identifier>
        s_database.ignore(pos, expected);
        if (!name_p.parse(pos, database, expected))
            return false;
    }
    else
    {
        // Parse identifier directly (handles "USE database" where database is a name)
        if (!name_p.parse(pos, database, expected))
            return false;
    }

    auto query = make_intrusive<ASTUseQuery>();
    query->set(query->database, database);
    node = query;

    return true;
}

}

namespace DB
{

REGISTER_STATEMENTS(Use)
{
    factory.registerStatement("USE", "",
    {
        .description = R"(
Sets the current database of the session. The current database is used to look up the tables which are not qualified
with a database name.

The statement cannot be used over the HTTP protocol, because there is no concept of a session there; pass the
`database` parameter instead.
)",
        .syntax = R"(
USE [DATABASE] db
)",
        .examples = {{"Switch the current database", "USE system;", ""}},
        .related = {"CREATE DATABASE", "SHOW", "SET"},
    });
}

}
