#include <Parsers/ASTCreateClusterQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateClusterQuery.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>


namespace DB
{

bool ParserCreateClusterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;
    ParserToken s_comma(TokenType::Comma);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    if (!s_create.ignore(pos, expected))
        return false;
    if (!s_cluster.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    ASTPtr cluster_ast;
    if (!name_p.parse(pos, cluster_ast, expected))
        return false;

    if (!s_lparen.ignore(pos, expected))
        return false;

    std::vector<String> members;
    ASTPtr member_id;
    if (!name_p.parse(pos, member_id, expected))
        return false;
    tryGetIdentifierNameInto(member_id, members.emplace_back());

    while (s_comma.ignore(pos, expected))
    {
        if (!name_p.parse(pos, member_id, expected))
            return false;
        tryGetIdentifierNameInto(member_id, members.emplace_back());
    }

    if (!s_rparen.ignore(pos, expected))
        return false;

    SettingsChanges cluster_properties;
    bool parsed_options [[maybe_unused]] = false;
    if (!parseSQLClusterCatalogOptionalProperties(cluster_properties, parsed_options, pos, expected))
        return false;

    String cluster_str;
    bool sync = false;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
        ParserKeyword s_sync(Keyword::SYNC);
        if (s_sync.ignore(pos, expected))
            sync = true;
    }

    auto query = make_intrusive<ASTCreateClusterQuery>();
    tryGetIdentifierNameInto(cluster_ast, query->cluster_name);
    query->members = std::move(members);
    query->cluster_properties = std::move(cluster_properties);
    query->if_not_exists = if_not_exists;
    query->cluster = std::move(cluster_str);
    query->sync = sync;
    node = query;
    return true;
}

}
