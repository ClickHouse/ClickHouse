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
    ParserIdentifier identifier_parser;

    database_str = "";
    table_str = "";

    /// Any number of dot-separated parts is accepted: `a.b.c` is a hierarchical name, and the database is
    /// everything but the last part (`a.b`), the same way as `ASTTableIdentifier::getTableId` splits it.
    /// The catalog tries the other splits when it resolves the name.
    std::vector<String> parts;
    do
    {
        ASTPtr identifier;
        if (!identifier_parser.parse(pos, identifier, expected))
            return false;
        parts.push_back(getIdentifierName(identifier));
    } while (s_dot.ignore(pos));

    table_str = parts.back();
    for (size_t i = 0; i + 1 < parts.size(); ++i)
    {
        if (i > 0)
            database_str += '.';
        database_str += parts[i];
    }

    return true;
}

bool parseDatabaseAndTableAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database, ASTPtr & table)
{
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier identifier_parser(true);

    /// The same as above, with query parameters allowed in the parts.
    ASTs identifiers;
    do
    {
        ASTPtr identifier;
        if (!identifier_parser.parse(pos, identifier, expected))
            return false;
        identifiers.push_back(std::move(identifier));
    } while (s_dot.ignore(pos));

    table = identifiers.back();
    if (identifiers.size() == 2)
    {
        database = identifiers.front();
    }
    else if (identifiers.size() > 2)
    {
        std::vector<String> database_parts;
        ASTs database_params;
        for (size_t i = 0; i + 1 < identifiers.size(); ++i)
        {
            const auto & identifier = identifiers[i]->as<ASTIdentifier &>();
            if (identifier.isParam())
            {
                database_parts.emplace_back();
                database_params.push_back(identifier.getParam());
            }
            else
            {
                database_parts.push_back(identifier.name());
            }
        }
        database = make_intrusive<ASTIdentifier>(std::move(database_parts), false, std::move(database_params));
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
