#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTRenameQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserRenameQuery.h>


namespace DB
{


/// Parse database.table or table.
static bool parseDatabaseAndTable(
    ASTRenameQuery::Table & db_and_table, IParser::Pos & pos, Expected & expected)
{
    ParserIdentifier name_p;
    ParserToken s_dot(TokenType::Dot);

    ASTPtr database;
    ASTPtr table;

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    db_and_table.database.clear();
    tryGetIdentifierNameInto(database, db_and_table.database);
    tryGetIdentifierNameInto(table, db_and_table.table);

    return true;
}


bool ParserRenameQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_rename_table("RENAME TABLE");
    ParserKeyword s_exchange_tables("EXCHANGE TABLES");
    ParserKeyword s_rename_dictionary("RENAME DICTIONARY");
    ParserKeyword s_exchange_dictionaries("EXCHANGE DICTIONARIES");
    ParserKeyword s_rename_database("RENAME DATABASE");
    ParserKeyword s_to("TO");
    ParserKeyword s_and("AND");
    ParserToken s_comma(TokenType::Comma);

    bool exchange = false;
    bool dictionary = false;

    if (s_rename_table.ignore(pos, expected))
        ;
    else if (s_exchange_tables.ignore(pos, expected))
        exchange = true;
    else if (s_rename_dictionary.ignore(pos, expected))
        dictionary = true;
    else if (s_exchange_dictionaries.ignore(pos, expected))
    {
        exchange = true;
        dictionary = true;
    }
    else if (s_rename_database.ignore(pos, expected))
    {
        ASTPtr from_db;
        ASTPtr to_db;
        ParserIdentifier db_name_p;
        if (!db_name_p.parse(pos, from_db, expected))
            return false;
        if (!s_to.ignore(pos, expected))
            return false;
        if (!db_name_p.parse(pos, to_db, expected))
            return false;

        String cluster_str;
        if (ParserKeyword{"ON"}.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
        }

        auto query = std::make_shared<ASTRenameQuery>();
        query->database = true;
        query->elements.emplace({});
        tryGetIdentifierNameInto(from_db, query->elements.front().from.database);
        tryGetIdentifierNameInto(to_db, query->elements.front().to.database);
        query->cluster = cluster_str;
        node = query;
        return true;
    }
    else
        return false;

    ASTRenameQuery::Elements elements;

    const auto ignore_delim = [&] { return exchange ? s_and.ignore(pos) : s_to.ignore(pos); };

    while (true)
    {
        if (!elements.empty() && !s_comma.ignore(pos))
            break;

        ASTRenameQuery::Element& ref = elements.emplace_back();

        if (!parseDatabaseAndTable(ref.from, pos, expected)
            || !ignore_delim()
            || !parseDatabaseAndTable(ref.to, pos, expected))
            return false;
    }

    String cluster_str;
    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = std::make_shared<ASTRenameQuery>();
    query->cluster = cluster_str;
    node = query;

    query->elements = elements;
    query->exchange = exchange;
    query->dictionary = dictionary;
    return true;
}


}
