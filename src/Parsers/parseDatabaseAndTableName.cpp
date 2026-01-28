#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTIdentifier.h>
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

        /// Support db.namespace1.namespace2...table for DataLakeCatalog databases
        /// Join all additional parts into the table name
        while (s_dot.ignore(pos))
        {
            ASTPtr next_part;
            if (!table_parser.parse(pos, next_part, expected))
                return false;

            String next_part_name;
            tryGetIdentifierNameInto(next_part, next_part_name);
            table_str += "." + next_part_name;
        }
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

        /// Support db.namespace1.namespace2...table for DataLakeCatalog databases
        /// Join all additional parts into the table name
        while (s_dot.ignore(pos, expected))
        {
            ASTPtr next_part;
            if (!table_parser.parse(pos, next_part, expected))
                return false;

            String current_table_name;
            String next_part_name;
            tryGetIdentifierNameInto(table, current_table_name);
            tryGetIdentifierNameInto(next_part, next_part_name);
            table = make_intrusive<ASTIdentifier>(current_table_name + "." + next_part_name);
        }
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
                if (identifier_parser.parse(pos, ast, expected))
                {
                    /// db.table (or db.namespace1.namespace2...table)
                    database = std::move(first_identifier);
                    table = getIdentifierName(ast);

                    /// Support db.namespace1.namespace2...table for DataLakeCatalog databases
                    /// Join all additional parts into the table name
                    while (ParserToken{TokenType::Dot}.ignore(pos, expected))
                    {
                        if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                        {
                            /// db.namespace.*
                            wildcard = true;
                            return true;
                        }
                        if (!identifier_parser.parse(pos, ast, expected))
                            return false;
                        table += "." + getIdentifierName(ast);
                    }

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
