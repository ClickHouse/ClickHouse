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

    s_database.ignore(pos, expected);

    ASTPtr database;
    if (!name_p.parse(pos, database, expected))
        return false;

    /// Support USE db.prefix syntax for DataLakeCatalog databases
    /// Parse additional dot-separated parts and join them into the database name
    String database_name;
    tryGetIdentifierNameInto(database, database_name);

    while (s_dot.ignore(pos, expected))
    {
        ASTPtr next_part;
        if (!name_p.parse(pos, next_part, expected))
            return false;
        String part_name;
        tryGetIdentifierNameInto(next_part, part_name);
        database_name += "." + part_name;
    }

    auto query = std::make_shared<ASTUseQuery>();
    query->set(query->database, std::make_shared<ASTIdentifier>(database_name));
    node = query;

    return true;
}

}
