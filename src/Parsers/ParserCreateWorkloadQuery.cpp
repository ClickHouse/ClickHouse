#include <Parsers/ParserCreateWorkloadQuery.h>

#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>

#include <Common/SettingsChanges.h>

namespace DB
{

namespace
{

bool parseWorkloadSetting(
    ASTCreateWorkloadQuery::SettingChange & change, IParser::Pos & pos, Expected & expected)
{
    ParserIdentifier name_p;
    ParserLiteral value_p;
    ParserToken s_eq(TokenType::Equals);
    ParserIdentifier resource_name_p;

    ASTPtr name_node;
    ASTPtr value_node;
    ASTPtr resource_name_node;

    String name;
    String resource_name;

    if (!name_p.parse(pos, name_node, expected))
        return false;
    tryGetIdentifierNameInto(name_node, name);

    if (!s_eq.ignore(pos, expected))
        return false;

    if (!value_p.parse(pos, value_node, expected))
        return false;

    if (ParserKeyword(Keyword::FOR).ignore(pos, expected))
    {
        if (!resource_name_p.parse(pos, resource_name_node, expected))
            return false;
        tryGetIdentifierNameInto(resource_name_node, resource_name);
    }

    change.name = std::move(name);
    change.value = value_node->as<ASTLiteral &>().value;
    change.resource = std::move(resource_name);

    return true;
}

bool parseSettings(IParser::Pos & pos, Expected & expected, ASTCreateWorkloadQuery::SettingsChanges & changes)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        if (!ParserKeyword(Keyword::SETTINGS).ignore(pos, expected))
            return false;

        ASTCreateWorkloadQuery::SettingsChanges res_changes;

        auto parse_setting = [&]
        {
            ASTCreateWorkloadQuery::SettingChange change;
            if (!parseWorkloadSetting(change, pos, expected))
                return false;
            res_changes.push_back(std::move(change));
            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_setting, false))
            return false;

        changes = std::move(res_changes);
        return true;
    });
}

}

bool ParserCreateWorkloadQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_workload(Keyword::WORKLOAD);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserIdentifier workload_name_p;
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_in(Keyword::IN);

    ASTPtr workload_name;
    ASTPtr workload_parent;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!s_workload.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!workload_name_p.parse(pos, workload_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (s_in.ignore(pos, expected))
    {
        if (!workload_name_p.parse(pos, workload_parent, expected))
            return false;
    }

    ASTCreateWorkloadQuery::SettingsChanges changes;
    parseSettings(pos, expected, changes);

    auto create_workload_query = std::make_shared<ASTCreateWorkloadQuery>();
    node = create_workload_query;

    create_workload_query->workload_name = workload_name;
    create_workload_query->children.push_back(workload_name);

    if (workload_parent)
    {
        create_workload_query->workload_parent = workload_parent;
        create_workload_query->children.push_back(workload_parent);
    }

    create_workload_query->or_replace = or_replace;
    create_workload_query->if_not_exists = if_not_exists;
    create_workload_query->cluster = std::move(cluster_str);
    create_workload_query->changes = std::move(changes);


    return true;
}

}
