#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTRenameQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserRenameQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_rename_table("RENAME TABLE");
    ParserKeyword s_exchange_tables("EXCHANGE TABLES");
    ParserKeyword s_rename_dictionary("RENAME DICTIONARY");
    ParserKeyword s_exchange_dictionaries("EXCHANGE DICTIONARIES");
    ParserKeyword s_rename_database("RENAME DATABASE");
    ParserKeyword s_if_exists("IF EXISTS");
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
        ParserIdentifier db_name_p(true);
        bool if_exists = s_if_exists.ignore(pos, expected);
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
        query->elements.front().if_exists = if_exists;
        query->elements.front().from.database = from_db;
        query->elements.front().to.database = to_db;
        query->children.push_back(std::move(from_db));
        query->children.push_back(std::move(to_db));
        query->cluster = cluster_str;
        node = query;
        return true;
    }
    else
        return false;

    const auto ignore_delim = [&] { return exchange ? s_and.ignore(pos) : s_to.ignore(pos); };

    auto query = std::make_shared<ASTRenameQuery>();

    ASTRenameQuery::Elements & elements = query->elements;

    while (true)
    {
        if (!elements.empty() && !s_comma.ignore(pos))
            break;

        ASTRenameQuery::Element & ref = elements.emplace_back();

        if (!exchange)
            ref.if_exists = s_if_exists.ignore(pos, expected);

        if (!parseDatabaseAndTableAsAST(pos, expected, ref.from.database, ref.from.table)
            || !ignore_delim()
            || !parseDatabaseAndTableAsAST(pos, expected, ref.to.database, ref.to.table))
            return false;

        if (ref.from.database)
            query->children.push_back(ref.from.database);
        if (ref.from.table)
            query->children.push_back(ref.from.table);
        if (ref.to.database)
            query->children.push_back(ref.to.database);
        if (ref.to.table)
            query->children.push_back(ref.to.table);
    }

    String cluster_str;
    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    query->cluster = cluster_str;
    query->exchange = exchange;
    query->dictionary = dictionary;
    node = query;
    return true;
}


}
