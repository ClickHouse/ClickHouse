#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTRenameQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserRenameQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_rename(Keyword::RENAME);
    ParserKeyword s_rename_table(Keyword::RENAME_TABLE);
    ParserKeyword s_exchange_tables(Keyword::EXCHANGE_TABLES);
    ParserKeyword s_rename_dictionary(Keyword::RENAME_DICTIONARY);
    ParserKeyword s_exchange_dictionaries(Keyword::EXCHANGE_DICTIONARIES);
    ParserKeyword s_rename_database(Keyword::RENAME_DATABASE);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_and(Keyword::AND);
    ParserToken s_comma(TokenType::Comma);

    bool exchange = false;
    bool dictionary = false;

    if (s_rename_database.ignore(pos, expected))
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
        if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
        }
        ASTRenameQuery::Elements rename_elements;
        rename_elements.emplace_back();
        rename_elements.back().if_exists = if_exists;
        rename_elements.back().from.database = from_db;
        rename_elements.back().to.database = to_db;

        auto query = std::make_shared<ASTRenameQuery>(std::move(rename_elements));
        query->database = true;
        query->cluster = cluster_str;
        node = query;
        return true;
    }
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
    else if (s_rename.ignore(pos, expected))
        ;
    else
        return false;

    const auto ignore_delim = [&] { return exchange ? s_and.ignore(pos, expected) : s_to.ignore(pos, expected); };

    ASTRenameQuery::Elements elements;

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
    }

    String cluster_str;
    if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = std::make_shared<ASTRenameQuery>(std::move(elements));
    query->cluster = cluster_str;
    query->exchange = exchange;
    query->dictionary = dictionary;
    node = query;
    return true;
}


}
