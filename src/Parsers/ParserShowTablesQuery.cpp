#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTShowTablesQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserShowTablesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_show("SHOW");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_tables("TABLES");
    ParserKeyword s_databases("DATABASES");
    ParserKeyword s_clusters("CLUSTERS");
    ParserKeyword s_cluster("CLUSTER");
    ParserKeyword s_dictionaries("DICTIONARIES");
    ParserKeyword s_from("FROM");
    ParserKeyword s_in("IN");
    ParserKeyword s_not("NOT");
    ParserKeyword s_like("LIKE");
    ParserKeyword s_ilike("ILIKE");
    ParserKeyword s_where("WHERE");
    ParserKeyword s_limit("LIMIT");
    ParserStringLiteral like_p;
    ParserIdentifier name_p;
    ParserExpressionWithOptionalAlias exp_elem(false);

    ASTPtr like;
    ASTPtr database;

    auto query = std::make_shared<ASTShowTablesQuery>();

    if (!s_show.ignore(pos, expected))
        return false;

    if (s_databases.ignore(pos))
    {
        query->databases = true;
    }
    else if (s_clusters.ignore(pos))
    {
        query->clusters = true;

        if (s_not.ignore(pos, expected))
            query->not_like = true;

        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else if (query->not_like)
            return false;
        if (s_limit.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->limit_length, expected))
                return false;
        }
    }
    else if (s_cluster.ignore(pos))
    {
        query->cluster = true;

        String cluster_str;
        if (!parseIdentifierOrStringLiteral(pos, expected, cluster_str))
            return false;

        query->cluster_str = std::move(cluster_str);
    }
    else
    {
        if (s_temporary.ignore(pos))
            query->temporary = true;

        if (!s_tables.ignore(pos, expected))
        {
            if (s_dictionaries.ignore(pos, expected))
                query->dictionaries = true;
            else
                return false;
        }

        if (s_from.ignore(pos, expected) || s_in.ignore(pos, expected))
        {
            if (!name_p.parse(pos, database, expected))
                return false;
        }

        if (s_not.ignore(pos, expected))
            query->not_like = true;

        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else if (query->not_like)
            return false;
        else if (s_where.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->where_expression, expected))
                return false;
        }

        if (s_limit.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->limit_length, expected))
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
