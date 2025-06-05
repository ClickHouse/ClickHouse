#include <Parsers/ParserUseQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
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

    if (!s_use.ignore(pos, expected))
        return false;

    s_database.ignore(pos, expected);

    ASTPtr database;
    if (!name_p.parse(pos, database, expected))
        return false;

    auto query = std::make_shared<ASTUseQuery>();
    query->set(query->database, database);
    node = query;

    return true;
}

}
