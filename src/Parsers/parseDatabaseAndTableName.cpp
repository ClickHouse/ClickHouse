#include "parseDatabaseAndTableName.h"
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>


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

        tryGetIdentifierNameInto(database, database_str);
        tryGetIdentifierNameInto(table, table_str);
    }
    else
    {
        database_str = "";
        tryGetIdentifierNameInto(database, table_str);
    }

    return true;
}


bool parseDatabaseAndTableNameOrAsterisks(IParser::Pos & pos, Expected & expected, String & database, bool & any_database, String & table, bool & any_table)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
        {
            auto pos_before_dot = pos;
            if (ParserToken{TokenType::Dot}.ignore(pos, expected)
                    && ParserToken{TokenType::Asterisk}.ignore(pos, expected))
            {
                /// *.*
                any_database = true;
                database.clear();
                any_table = true;
                table.clear();
                return true;
            }

            /// *
            pos = pos_before_dot;
            any_database = false;
            database.clear();
            any_table = true;
            table.clear();
            return true;
        }

        ASTPtr ast;
        ParserIdentifier identifier_parser;
        if (identifier_parser.parse(pos, ast, expected))
        {
            String first_identifier = getIdentifierName(ast);
            auto pos_before_dot = pos;

            if (ParserToken{TokenType::Dot}.ignore(pos, expected))
            {
                if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                {
                    /// db.*
                    any_database = false;
                    database = std::move(first_identifier);
                    any_table = true;
                    table.clear();
                    return true;
                }
                else if (identifier_parser.parse(pos, ast, expected))
                {
                    /// db.table
                    any_database = false;
                    database = std::move(first_identifier);
                    any_table = false;
                    table = getIdentifierName(ast);
                    return true;
                }
            }

            /// table
            pos = pos_before_dot;
            any_database = false;
            database.clear();
            any_table = false;
            table = std::move(first_identifier);
            return true;
        }

        return false;
    });
}

}
