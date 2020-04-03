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
    ParserKeyword s_to("TO");
    ParserKeyword s_exchange_tables("EXCHANGE TABLES");
    ParserKeyword s_and("AND");
    ParserToken s_comma(TokenType::Comma);

    bool exchange = false;

    if (!s_rename_table.ignore(pos, expected))
    {
        if (s_exchange_tables.ignore(pos, expected))
            exchange = true;
        else
            return false;
    }

    ASTRenameQuery::Elements elements;

    auto ignore_delim = [&]()
    {
        return exchange ? s_and.ignore(pos) : s_to.ignore(pos);
    };

    while (true)
    {
        if (!elements.empty() && !s_comma.ignore(pos))
            break;

        elements.push_back(ASTRenameQuery::Element());

        if (!parseDatabaseAndTable(elements.back().from, pos, expected)
            || !ignore_delim()
            || !parseDatabaseAndTable(elements.back().to, pos, expected))
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
    return true;
}


}
