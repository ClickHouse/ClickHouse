#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTDropQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDropQuery.h>


namespace DB
{

namespace
{

bool parseDropQuery(IParser::Pos & pos, ASTPtr & node, Expected & expected, bool optional_table_keyword = false)
{
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_dictionary("DICTIONARY");
    ParserKeyword s_view("VIEW");
    ParserKeyword s_database("DATABASE");
    ParserToken s_dot(TokenType::Dot);
    ParserKeyword s_if_exists("IF EXISTS");
    ParserIdentifier name_p;
    ParserKeyword s_no_delay("NO DELAY");
    ParserKeyword s_sync("SYNC");

    ASTPtr database;
    ASTPtr table;
    String cluster_str;
    bool if_exists = false;
    bool temporary = false;
    bool is_dictionary = false;
    bool is_view = false;
    bool no_delay = false;

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

        if (s_no_delay.ignore(pos, expected) || s_sync.ignore(pos, expected))
            no_delay = true;
    }
    else
    {
        if (s_view.ignore(pos, expected))
            is_view = true;
        else if (s_dictionary.ignore(pos, expected))
            is_dictionary = true;
        else if (s_temporary.ignore(pos, expected))
            temporary = true;

        if (!is_view && !is_dictionary && (!s_table.ignore(pos, expected) && !optional_table_keyword))
        {
            return false;
        }

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

        if (s_no_delay.ignore(pos, expected) || s_sync.ignore(pos, expected))
            no_delay = true;
    }

    auto query = std::make_shared<ASTDropQuery>();
    node = query;

    query->kind = ASTDropQuery::Kind::Drop;
    query->if_exists = if_exists;
    query->temporary = temporary;
    query->is_dictionary = is_dictionary;
    query->is_view = is_view;
    query->no_delay = no_delay;

    tryGetIdentifierNameInto(database, query->database);
    tryGetIdentifierNameInto(table, query->table);

    query->cluster = cluster_str;

    return true;
}

bool parseDetachQuery(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    if (parseDropQuery(pos, node, expected))
    {
        auto * drop_query = node->as<ASTDropQuery>();
        drop_query->kind = ASTDropQuery::Kind::Detach;
        return true;
    }
    return false;
}

bool parseTruncateQuery(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    if (parseDropQuery(pos, node, expected, true))
    {
        auto * drop_query = node->as<ASTDropQuery>();
        drop_query->kind = ASTDropQuery::Kind::Truncate;
        return true;
    }
    return false;
}

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

}
