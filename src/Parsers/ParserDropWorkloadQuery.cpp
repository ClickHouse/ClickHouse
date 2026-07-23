#include <Parsers/ASTDropWorkloadQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropWorkloadQuery.h>

namespace DB
{

bool ParserDropWorkloadQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_workload(Keyword::WORKLOAD);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier workload_name_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr workload_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_workload.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!workload_name_p.parse(pos, workload_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto drop_workload_query = std::make_shared<ASTDropWorkloadQuery>();
    drop_workload_query->if_exists = if_exists;
    drop_workload_query->cluster = std::move(cluster_str);

    node = drop_workload_query;

    drop_workload_query->workload_name = workload_name->as<ASTIdentifier &>().name();

    return true;
}

}
