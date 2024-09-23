#include <Parsers/ParserCreateWorkloadQuery.h>

#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>

#include <Common/SettingsChanges.h>

namespace DB
{

namespace
{

bool parseSettings(IParser::Pos & pos, Expected & expected, ASTPtr & settings)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        if (!ParserKeyword(Keyword::SETTINGS).ignore(pos, expected))
            return false;

        SettingsChanges settings_changes;
        Strings default_settings;

        auto parse_setting = [&]
        {
            SettingChange setting;
            String default_setting;
            std::pair<String, String> parameter;

            if (ParserSetQuery::parseNameValuePairWithParameterOrDefault(setting, default_setting, parameter, pos, expected))
            {
                if (!default_setting.empty())
                {
                    default_settings.push_back(std::move(default_setting));
                    return true;
                }
                if (!setting.name.empty())
                {
                    settings_changes.push_back(std::move(setting));
                    return true;
                }
                // TODO(serxa): parse optional clause: [FOR resource_name]
                return false; // We do not support parameters
            }

            return false;
        };

        if (!ParserList::parseUtil(pos, expected, parse_setting, false))
            return false;

        ASTPtr res_settings;
        if (!settings_changes.empty())
        {
            auto settings_changes_ast = std::make_shared<ASTSetQuery>();
            settings_changes_ast->changes = std::move(settings_changes);
            settings_changes_ast->is_standalone = false;
            res_settings = settings_changes_ast;
        }

        settings = std::move(res_settings);
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

    ASTPtr settings;
    parseSettings(pos, expected, settings);

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

    create_workload_query->settings = std::move(settings);

    return true;
}

}
