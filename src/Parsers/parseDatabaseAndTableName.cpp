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
    /// A database name can be hierarchical (`a.b`), the same as in `CREATE DATABASE` and `USE`.
    ParserCompoundIdentifier identifier_parser(/*table_name_with_optional_uuid*/ false, /*allow_query_parameter*/ true);
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

        /// Any number of dot-separated parts is accepted before the optional `.*`: `a.b.c` is a hierarchical name
        /// (see `parseDatabaseAndTableName`), and the database is everything but the last part. The interpreter of the
        /// statement resolves the name against the catalog, so that a privilege names the object a query reads.
        const auto join_parts = [](const std::vector<String> & parts, size_t count)
        {
            String res;
            for (size_t i = 0; i < count; ++i)
            {
                if (i > 0)
                    res += '.';
                res += parts[i];
            }
            return res;
        };

        ParserIdentifier identifier_parser;
        std::vector<String> parts;

        while (true)
        {
            ASTPtr ast;
            if (!identifier_parser.parse(pos, ast, expected))
                return false;
            parts.push_back(getIdentifierName(ast));

            if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                wildcard = true;

            auto pos_before_dot = pos;
            if (!ParserToken{TokenType::Dot}.ignore(pos, expected))
                break;

            if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
            {
                /// `db.*`, or `a.b.*` for a hierarchical database name.
                database = join_parts(parts, parts.size());
                table.clear();
                return true;
            }

            if (!identifier_parser.checkWithoutMoving(pos, expected))
            {
                pos = pos_before_dot;
                break;
            }
        }

        /// `table`, `db.table`, or a hierarchical name `a.b.c`.
        table = parts.back();
        database = join_parts(parts, parts.size() - 1);
        default_database = parts.size() == 1;
        return true;
    });
}

}
