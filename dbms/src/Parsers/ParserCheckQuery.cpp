#include <Parsers/ParserCheckQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTCheckQuery.h>


namespace DB
{

bool ParserCheckQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check_table("CHECK TABLE");
    ParserToken s_dot(TokenType::Dot);

    ParserIdentifier table_parser;

    ASTPtr table;
    ASTPtr database;

    if (!s_check_table.ignore(pos, expected))
        return false;
    if (!table_parser.parse(pos, database, expected))
        return false;

    if (s_dot.ignore(pos))
    {
        if (!table_parser.parse(pos, table, expected))
            return false;

        auto query = std::make_shared<ASTCheckQuery>();
        getIdentifierName(database, query->database);
        getIdentifierName(table, query->table);
        node = query;
    }
    else
    {
        table = database;
        auto query = std::make_shared<ASTCheckQuery>();
        getIdentifierName(table, query->table);
        node = query;
    }

    return true;
}

}
