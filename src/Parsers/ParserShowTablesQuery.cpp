#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTShowTablesQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserShowTablesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    ParserKeyword s_show("SHOW");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_tables("TABLES");
    ParserKeyword s_databases("DATABASES");
    ParserKeyword s_dictionaries("DICTIONARIES");
    ParserKeyword s_from("FROM");
    ParserKeyword s_in("IN");
    ParserKeyword s_not("NOT");
    ParserKeyword s_like("LIKE");
    ParserKeyword s_where("WHERE");
    ParserKeyword s_limit("LIMIT");
    ParserStringLiteral like_p;
    ParserIdentifier name_p;
    ParserExpressionWithOptionalAlias exp_elem(false);

    ASTPtr like;
    ASTPtr database;

    auto query = std::make_shared<ASTShowTablesQuery>();

    if (!s_show.ignore(pos, expected, ranges))
        return false;

    if (s_databases.ignore(pos, expected, ranges))
    {
        query->databases = true;
    }
    else
    {
        if (s_temporary.ignore(pos, expected, ranges))
            query->temporary = true;

        if (!s_tables.ignore(pos, expected, ranges))
        {
            if (s_dictionaries.ignore(pos, expected, ranges))
                query->dictionaries = true;
            else
                return false;
        }

        if (s_from.ignore(pos, expected, ranges) || s_in.ignore(pos, expected, ranges))
        {
            if (!name_p.parse(pos, database, expected, ranges))
                return false;
        }

        if (s_not.ignore(pos, expected, ranges))
            query->not_like = true;

        if (s_like.ignore(pos, expected, ranges))
        {
            if (!like_p.parse(pos, like, expected, ranges))
                return false;
        }
        else if (query->not_like)
            return false;
        else if (s_where.ignore(pos, expected, ranges))
        {
            if (!exp_elem.parse(pos, query->where_expression, expected, ranges))
                return false;
        }

        if (s_limit.ignore(pos, expected, ranges))
        {
            if (!exp_elem.parse(pos, query->limit_length, expected, ranges))
                return false;
        }
    }

    tryGetIdentifierNameInto(database, query->from);
    if (like)
        query->like = safeGet<const String &>(like->as<ASTLiteral &>().value);

    node = query;

    return true;
}


}
