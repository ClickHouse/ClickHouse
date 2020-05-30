#include <Parsers/ParserCreateSettingsProfileQuery.h>
#include <Parsers/ASTCreateSettingsProfileQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSettingsProfileElement.h>
#include <Parsers/ParserSettingsProfileElement.h>
#include <Parsers/ParserExtendedRoleSet.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


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

            return parseIdentifierOrStringLiteral(pos, expected, new_name);
        });
    }

    bool parseSettings(IParserBase::Pos & pos, Expected & expected, bool id_mode, std::shared_ptr<ASTSettingsProfileElements> & settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"SETTINGS"}.ignore(pos, expected))
                return false;

            ASTPtr new_settings_ast;
            if (!ParserSettingsProfileElements{}.useIDMode(id_mode).enableInheritKeyword(true).parse(pos, new_settings_ast, expected))
                return false;

            if (!settings)
                settings = std::make_shared<ASTSettingsProfileElements>();
            const auto & new_settings = new_settings_ast->as<const ASTSettingsProfileElements &>();
            settings->elements.insert(settings->elements.end(), new_settings.elements.begin(), new_settings.elements.end());
            return true;
        });
    }

    bool parseToRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, std::shared_ptr<ASTExtendedRoleSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (roles || !ParserKeyword{"TO"}.ignore(pos, expected)
                || !ParserExtendedRoleSet{}.useIDMode(id_mode).parse(pos, ast, expected))
                return false;

            roles = std::static_pointer_cast<ASTExtendedRoleSet>(ast);
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


bool ParserCreateSettingsProfileQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{"ATTACH SETTINGS PROFILE"}.ignore(pos, expected) && !ParserKeyword{"ATTACH PROFILE"}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{"ALTER SETTINGS PROFILE"}.ignore(pos, expected) || ParserKeyword{"ALTER PROFILE"}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{"CREATE SETTINGS PROFILE"}.ignore(pos, expected) && !ParserKeyword{"CREATE PROFILE"}.ignore(pos, expected))
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
    if (!parseIdentifiersOrStringLiterals(pos, expected, names))
        return false;

    String new_name;
    std::shared_ptr<ASTSettingsProfileElements> settings;
    String cluster;

    while (true)
    {
        if (alter && new_name.empty() && (names.size() == 1) && parseRenameTo(pos, expected, new_name))
            continue;

        if (parseSettings(pos, expected, attach_mode, settings))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        break;
    }

    std::shared_ptr<ASTExtendedRoleSet> to_roles;
    parseToRoles(pos, expected, attach_mode, to_roles);

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    auto query = std::make_shared<ASTCreateSettingsProfileQuery>();
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
    query->to_roles = std::move(to_roles);

    return true;
}
}
