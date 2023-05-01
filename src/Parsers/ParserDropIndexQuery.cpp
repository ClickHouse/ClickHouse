#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropIndexQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>

namespace DB
{

bool ParserDropIndexQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTDropIndexQuery>();
    node = query;

    ParserKeyword s_drop("DROP");
    ParserKeyword s_index("INDEX");
    ParserKeyword s_on("ON");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserIdentifier index_name_p;

    String cluster_str;
    bool if_exists = false;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_index.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!index_name_p.parse(pos, query->index_name, expected))
        return false;

    /// ON [db.] table_name
    if (!s_on.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
        return false;

    /// [ON cluster_name]
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;

        query->cluster = std::move(cluster_str);
    }

    if (query->index_name)
        query->children.push_back(query->index_name);

    query->if_exists = if_exists;

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    return true;
}

}
