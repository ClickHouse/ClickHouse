#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTRenameQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserRenameQuery.h>


namespace DB
{


/// Parse database.table or table.
static bool parseDatabaseAndTable(
    ASTRenameQuery::Table & db_and_table, IParser::Pos & pos, Expected & expected, IParser::Ranges * ranges)
{
    ParserIdentifier name_p;
    ParserToken s_dot(TokenType::Dot);

    ASTPtr database;
    ASTPtr table;

    if (!name_p.parse(pos, table, expected, ranges))
        return false;

    if (s_dot.ignore(pos, expected, ranges))
    {
        database = table;
        if (!name_p.parse(pos, table, expected, ranges))
            return false;
    }

    db_and_table.database.clear();
    tryGetIdentifierNameInto(database, db_and_table.database);
    tryGetIdentifierNameInto(table, db_and_table.table);

    return true;
}


bool ParserRenameQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    ParserKeyword s_rename_table("RENAME TABLE");
    ParserKeyword s_to("TO");
    ParserKeyword s_exchange_tables("EXCHANGE TABLES");
    ParserKeyword s_and("AND");
    ParserToken s_comma(TokenType::Comma);

    bool exchange = false;

    if (!s_rename_table.ignore(pos, expected, ranges))
    {
        if (s_exchange_tables.ignore(pos, expected, ranges))
            exchange = true;
        else
            return false;
    }

    ASTRenameQuery::Elements elements;

    auto ignore_delim = [&]()
    {
        return exchange ? s_and.ignore(pos, expected, ranges) : s_to.ignore(pos, expected, ranges);
    };

    while (true)
    {
        if (!elements.empty() && !s_comma.ignore(pos, expected, ranges))
            break;

        elements.push_back(ASTRenameQuery::Element());

        if (!parseDatabaseAndTable(elements.back().from, pos, expected, ranges)
            || !ignore_delim()
            || !parseDatabaseAndTable(elements.back().to, pos, expected, ranges))
            return false;
    }

    String cluster_str;
    if (ParserKeyword{"ON"}.ignore(pos, expected, ranges))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected, ranges))
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
