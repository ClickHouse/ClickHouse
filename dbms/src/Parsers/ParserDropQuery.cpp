#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTDropQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDropQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
}

bool ParserDropQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_detach("DETACH");
    ParserKeyword s_truncate("TRUNCATE");

    if (s_drop.ignore(pos, expected))
        return parseDropQuery(pos, node, expected);
    else if (s_detach.ignore(pos, expected))
        return parseDetachQuery(pos, node, expected);
    else if (s_truncate.ignore(pos, expected))
        return parseTruncateQuery(pos, node, expected);
    else
        return false;
}

bool ParserDropQuery::parseDetachQuery(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (parseDropQuery(pos, node, expected))
    {
        auto * drop_query = node->as<ASTDropQuery>();
        drop_query->kind = ASTDropQuery::Kind::Detach;
        return true;
    }
    return false;
}

bool ParserDropQuery::parseTruncateQuery(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (parseDropQuery(pos, node, expected))
    {
        auto * drop_query = node->as<ASTDropQuery>();
        drop_query->kind = ASTDropQuery::Kind::Truncate;
        return true;
    }
    return false;
}

bool ParserDropQuery::parseDropQuery(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (parseDropACLQuery(pos, node, expected))
        return true;

    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_database("DATABASE");
    ParserToken s_dot(TokenType::Dot);
    ParserKeyword s_if_exists("IF EXISTS");
    ParserIdentifier name_p;

    ASTPtr database;
    ASTPtr table;
    String cluster_str;
    bool if_exists = false;
    bool temporary = false;

    if (s_database.ignore(pos, expected))
    {
        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (!name_p.parse(pos, database, expected))
            return false;

        if (ParserKeyword{"ON"}.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
        }
    }
    else
    {
        if (s_temporary.ignore(pos, expected))
            temporary = true;

        if (!s_table.ignore(pos, expected))
            return false;

        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (!name_p.parse(pos, table, expected))
            return false;

        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
        }

        if (ParserKeyword{"ON"}.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
        }
    }

    auto query = std::make_shared<ASTDropQuery>();
    node = query;

    query->kind = ASTDropQuery::Kind::Drop;
    query->if_exists = if_exists;
    query->temporary = temporary;

    tryGetIdentifierNameInto(database, query->database);
    tryGetIdentifierNameInto(table, query->table);

    query->cluster = cluster_str;

    return true;
}


bool ParserDropQuery::parseDropACLQuery(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool drop_roles = false;
    bool drop_users = false;
    ParserKeyword role_p("ROLE");
    ParserKeyword user_p("USER");
    if (role_p.ignore(pos, expected))
        drop_roles = true;
    else if (user_p.ignore(pos, expected))
        drop_users = true;
    else
        return false;

    ParserKeyword if_exists_p("IF EXISTS");
    bool if_exists = false;
    if (if_exists_p.ignore(pos, expected))
        if_exists = true;

    Strings names;
    ParserToken comma_p{TokenType::Comma};
    do
    {
        ParserIdentifier name_p;
        ASTPtr name;
        if (!name_p.parse(pos, name, expected))
            return false;
        names.emplace_back(getIdentifierName(name));
    }
    while (comma_p.ignore(pos, expected));

    auto query = std::make_shared<ASTDropQuery>();
    node = query;
    query->kind = ASTDropQuery::Kind::Drop;
    query->if_exists = if_exists;
    if (drop_roles)
        query->roles = std::move(names);
    else if (drop_users)
        query->users = std::move(names);

    return true;
}
}
