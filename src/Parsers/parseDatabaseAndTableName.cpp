#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>


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

bool parseDatabaseAndTableAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database, ASTPtr & table)
{
    ASTPtr table_identifier;
    if (!ParserCompoundIdentifier({.table_name = true, .allow_query_parameter = true}).parse(pos, table_identifier, expected))
        return false;
    const auto & identifier = table_identifier->as<ASTTableIdentifier &>();
    database = identifier.getDatabase();
    table = identifier.getTable();
    return true;
}


bool parseTableNameWithOptionalFromDatabase(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str)
{
    ASTPtr table_identifier;
    if (!ParserCompoundIdentifier({.table_name = true}).parse(pos, table_identifier, expected))
        return false;
    const auto & identifier = table_identifier->as<ASTTableIdentifier &>();
    table_str = identifier.shortName();
    if (identifier.compound())
    {
        database_str = identifier.getDatabaseName();
        return true;
    }

    ASTPtr database;
    if (ParserKeyword(Keyword::FROM).ignore(pos, expected) || ParserKeyword(Keyword::IN).ignore(pos, expected))
        if (!ParserIdentifier().parse(pos, database, expected))
            return false;
    tryGetIdentifierNameInto(database, database_str);
    return true;
}


bool parseDatabaseAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database)
{
    ParserIdentifier identifier_parser(/* allow_query_parameter */true);
    return identifier_parser.parse(pos, database, expected);
}


bool parseDatabaseAndTableNameOrAsterisks(IParser::Pos & pos, Expected & expected, String & database, String & table, bool & wildcard, bool & default_database)
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
                database.clear();
                table.clear();
                return true;
            }

            /// *
            pos = pos_before_dot;
            database.clear();
            table.clear();
            default_database = true;
            return true;
        }

        ASTPtr ast;
        ParserIdentifier identifier_parser;
        if (identifier_parser.parse(pos, ast, expected))
        {
            String first_identifier = getIdentifierName(ast);
            if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                wildcard = true;

            auto pos_before_dot = pos;

            if (ParserToken{TokenType::Dot}.ignore(pos, expected))
            {
                if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                {
                    /// db.*
                    database = std::move(first_identifier);
                    table.clear();
                    return true;
                }
                /// db*.table is ambiguous: a database wildcard cannot be combined with a table name.
                if (wildcard)
                    return false;
                if (identifier_parser.parse(pos, ast, expected))
                {
                    /// db.table
                    database = std::move(first_identifier);
                    table = getIdentifierName(ast);
                    if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                        wildcard = true;

                    return true;
                }
            }

            /// table
            pos = pos_before_dot;
            database.clear();
            table = std::move(first_identifier);
            default_database = true;

            if (!wildcard && ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                wildcard = true;

            return true;
        }

        return false;
    });
}

}
