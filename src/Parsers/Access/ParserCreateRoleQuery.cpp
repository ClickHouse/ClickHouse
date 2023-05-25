#include <Parsers/Access/ParserCreateRoleQuery.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ParserSettingsProfileElement.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <base/insertAtEnd.h>


namespace DB
{
namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"RENAME TO"}.ignore(pos, expected))
                return false;

            return parseRoleName(pos, expected, new_name);
        });
    }

    bool parseSettings(IParserBase::Pos & pos, Expected & expected, bool id_mode, std::vector<std::shared_ptr<ASTSettingsProfileElement>> & settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"SETTINGS"}.ignore(pos, expected))
                return false;

            ASTPtr new_settings_ast;
            ParserSettingsProfileElements elements_p;
            elements_p.useIDMode(id_mode);
            if (!elements_p.parse(pos, new_settings_ast, expected))
                return false;

            settings = std::move(new_settings_ast->as<ASTSettingsProfileElements &>().elements);
            return true;
        });
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{"ON"}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }
}


bool ParserCreateRoleQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{"ATTACH ROLE"}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{"ALTER ROLE"}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{"CREATE ROLE"}.ignore(pos, expected))
            return false;
    }

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    if (alter)
    {
        if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
            if_exists = true;
    }
    else
    {
        if (ParserKeyword{"IF NOT EXISTS"}.ignore(pos, expected))
            if_not_exists = true;
        else if (ParserKeyword{"OR REPLACE"}.ignore(pos, expected))
            or_replace = true;
    }

    Strings names;
    if (!parseRoleNames(pos, expected, names))
        return false;

    String new_name;
    std::shared_ptr<ASTSettingsProfileElements> settings;
    String cluster;

    while (true)
    {
        if (alter && new_name.empty() && (names.size() == 1) && parseRenameTo(pos, expected, new_name))
            continue;

        std::vector<std::shared_ptr<ASTSettingsProfileElement>> new_settings;
        if (parseSettings(pos, expected, attach_mode, new_settings))
        {
            if (!settings)
                settings = std::make_shared<ASTSettingsProfileElements>();

            insertAtEnd(settings->elements, std::move(new_settings));
            continue;
        }

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        break;
    }

    auto query = std::make_shared<ASTCreateRoleQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->new_name = std::move(new_name);
    query->settings = std::move(settings);

    return true;
}
}
