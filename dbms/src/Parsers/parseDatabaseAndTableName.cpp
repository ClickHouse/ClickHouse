#include "parseDatabaseAndTableName.h"
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Common/typeid_cast.h>


namespace DB
{

bool parseDatabaseAndTableName(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str)
{
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier table_parser;

    ASTPtr database;
    ASTPtr table;

    database_str = "";
    table_str = "";

    if (!table_parser.parse(pos, database, expected))
        return false;

    if (s_dot.ignore(pos))
    {
        if (!table_parser.parse(pos, table, expected))
        {
            database_str = "";
            return false;
        }

        database_str = typeid_cast<ASTIdentifier &>(*database).name;
        table_str = typeid_cast<ASTIdentifier &>(*table).name;
    }
    else
    {
        database_str = "";
        table_str = typeid_cast<ASTIdentifier &>(*database).name;
    }

    return true;
}

}
