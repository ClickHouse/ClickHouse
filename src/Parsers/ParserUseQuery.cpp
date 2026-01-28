#include <Parsers/ParserUseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTUseQuery.h>


namespace DB
{

bool ParserUseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_use(Keyword::USE);
    ParserKeyword s_database(Keyword::DATABASE);
    ParserIdentifier name_p{/*allow_query_parameter*/ true};
    ParserToken s_dot(TokenType::Dot);

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

    /// Support USE db.prefix syntax for DataLakeCatalog databases
    /// Parse additional dot-separated parts and join them into the database name
    if (s_dot.ignore(pos, expected))
    {
        String database_name;
        tryGetIdentifierNameInto(database, database_name);

        do
        {
            ASTPtr next_part;
            if (!name_p.parse(pos, next_part, expected))
                return false;
            String part_name;
            tryGetIdentifierNameInto(next_part, part_name);
            database_name += "." + part_name;
        } while (s_dot.ignore(pos, expected));

        database = make_intrusive<ASTIdentifier>(database_name);
    }

    auto query = make_intrusive<ASTUseQuery>();

    query->set(query->database, database);
    node = query;

    return true;
}

}
