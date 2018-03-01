#include <Parsers/ParserCheckQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTCheckQuery.h>

#include <Common/typeid_cast.h>


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
        query->database = typeid_cast<const ASTIdentifier &>(*database).name;
        query->table = typeid_cast<const ASTIdentifier &>(*table).name;
        node = query;
    }
    else
    {
        table = database;
        auto query = std::make_shared<ASTCheckQuery>();
        query->table = typeid_cast<const ASTIdentifier &>(*table).name;
        node = query;
    }

    return true;
}

}
