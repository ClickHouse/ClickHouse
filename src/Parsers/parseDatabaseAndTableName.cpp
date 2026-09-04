#include <Parsers/parseDatabaseAndTableName.h>
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
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier table_parser(true);

    if (!table_parser.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos))
    {
        database = table;
        if (!table_parser.parse(pos, table, expected))
            return false;
    }

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
            bool database_has_wildcard = false;
            if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                database_has_wildcard = true;

            auto pos_before_dot = pos;

            if (ParserToken{TokenType::Dot}.ignore(pos, expected))
            {
                if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                {
                    /// db.* or db*.* — both are valid
                    /// Note: wildcard is NOT set here; db.* uses empty table name
                    /// to represent "all tables", consistent with original behavior.
                    database = std::move(first_identifier);
                    table.clear();
                    wildcard = database_has_wildcard;
                    return true;
                }
                if (database_has_wildcard)
                {
                    /// db*.table or db*.table* — invalid, ambiguous syntax.
                    /// A database wildcard cannot be combined with a specific table name.
                    return false;
                }
                if (identifier_parser.parse(pos, ast, expected))
                {
                    /// db.table or db.table*
                    database = std::move(first_identifier);
                    table = getIdentifierName(ast);
                    if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                        wildcard = true;

                    return true;
                }
            }

            /// table or table* (no dot found)
            pos = pos_before_dot;
            database.clear();
            table = std::move(first_identifier);
            default_database = true;

            /// If asterisk was already consumed as part of "table*" pattern,
            /// carry it forward as table wildcard.
            /// e.g. "foo*" → table="foo", wildcard=true
            if (database_has_wildcard)
                wildcard = true;
            else if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                wildcard = true;

            return true;
        }

        return false;
    });
}

}
