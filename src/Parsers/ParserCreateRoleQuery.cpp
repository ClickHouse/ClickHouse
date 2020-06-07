#include <Parsers/ParserCreateRoleQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSettingsProfileElement.h>
#include <Parsers/ParserSettingsProfileElement.h>
#include <Parsers/parseUserName.h>


namespace DB
{
namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, String & new_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"RENAME TO"}.ignore(pos, expected, ranges))
                return false;

            return parseRoleName(pos, expected, ranges, new_name);
        });
    }

    bool parseSettings(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, bool id_mode,
                       std::shared_ptr<ASTSettingsProfileElements> & settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"SETTINGS"}.ignore(pos, expected, ranges))
                return false;

            ASTPtr new_settings_ast;
            if (!ParserSettingsProfileElements{}.useIDMode(id_mode).parse(pos, new_settings_ast, expected, ranges))
                return false;

            if (!settings)
                settings = std::make_shared<ASTSettingsProfileElements>();
            const auto & new_settings = new_settings_ast->as<const ASTSettingsProfileElements &>();
            settings->elements.insert(settings->elements.end(), new_settings.elements.begin(), new_settings.elements.end());
            return true;
        });
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{"ON"}.ignore(pos, expected, ranges) && ASTQueryWithOnCluster::parse(pos, cluster, expected, ranges);
        });
    }
}


bool ParserCreateRoleQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{"ATTACH ROLE"}.ignore(pos, expected, ranges))
            return false;
    }
    else
    {
        if (ParserKeyword{"ALTER ROLE"}.ignore(pos, expected, ranges))
            alter = true;
        else if (!ParserKeyword{"CREATE ROLE"}.ignore(pos, expected, ranges))
            return false;
    }

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    if (alter)
    {
        if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected, ranges))
            if_exists = true;
    }
    else
    {
        if (ParserKeyword{"IF NOT EXISTS"}.ignore(pos, expected, ranges))
            if_not_exists = true;
        else if (ParserKeyword{"OR REPLACE"}.ignore(pos, expected, ranges))
            or_replace = true;
    }

    String name;
    if (!parseRoleName(pos, expected, ranges, name))
        return false;

    String new_name;
    std::shared_ptr<ASTSettingsProfileElements> settings;
    String cluster;

    while (true)
    {
        if (alter && parseRenameTo(pos, expected, ranges, new_name))
            continue;

        if (parseSettings(pos, expected, ranges, attach_mode, settings))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, ranges, cluster))
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
    query->name = std::move(name);
    query->new_name = std::move(new_name);
    query->settings = std::move(settings);

    return true;
}
}
