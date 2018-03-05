#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTShowTablesQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ExpressionElementParsers.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserShowTablesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_show("SHOW");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_tables("TABLES");
    ParserKeyword s_databases("DATABASES");
    ParserKeyword s_from("FROM");
    ParserKeyword s_not("NOT");
    ParserKeyword s_like("LIKE");
    ParserStringLiteral like_p;
    ParserIdentifier name_p;

    ASTPtr like;
    ASTPtr database;

    auto query = std::make_shared<ASTShowTablesQuery>();

    if (!s_show.ignore(pos, expected))
        return false;

    if (s_databases.ignore(pos))
    {
        query->databases = true;
    }
    else
    {
        if (s_temporary.ignore(pos))
            query->temporary = true;

        if (s_tables.ignore(pos, expected))
        {
            if (s_from.ignore(pos, expected))
            {
                if (!name_p.parse(pos, database, expected))
                    return false;
            }

            if (s_not.ignore(pos, expected))
                query->not_like = true;

            if (s_like.ignore(pos, expected))
            {
                if (!like_p.parse(pos, like, expected))
                    return false;
            }
            else if (query->not_like)
                return false;
        }
        else
            return false;
    }

    if (database)
        query->from = typeid_cast<ASTIdentifier &>(*database).name;
    if (like)
        query->like = safeGet<const String &>(typeid_cast<ASTLiteral &>(*like).value);

    node = query;

    return true;
}


}
