#include <Access/IAccessStorage.h>
#include <Parsers/Access/ParserCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ParserSettingsProfileElement.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/StatementFactory.h>
#include <base/insertAtEnd.h>


namespace DB
{
namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::RENAME_TO}.ignore(pos, expected))
                return false;

            return parseIdentifierOrStringLiteral(pos, expected, new_name);
        });
    }

    bool parseSettings(IParserBase::Pos & pos, Expected & expected, bool id_mode, boost::intrusive_ptr<ASTSettingsProfileElements> & settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            ParserSettingsProfileElements elements_p;
            elements_p.useInheritKeyword(true).useIDMode(id_mode);
            if (!elements_p.parse(pos, ast, expected))
                return false;

            settings = boost::static_pointer_cast<ASTSettingsProfileElements>(ast);
            return true;
        });
    }

    bool parseAlterSettings(IParserBase::Pos & pos, Expected & expected, boost::intrusive_ptr<ASTAlterSettingsProfileElements> & alter_settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            ParserAlterSettingsProfileElements elements_p;
            elements_p.useInheritKeyword(true);
            if (!elements_p.parse(pos, ast, expected))
                return false;

            alter_settings = boost::static_pointer_cast<ASTAlterSettingsProfileElements>(ast);
            return true;
        });
    }

    bool parseToRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, boost::intrusive_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (!ParserKeyword{Keyword::TO}.ignore(pos, expected))
                return false;

            ParserRolesOrUsersSet roles_p;
            roles_p.allowAll().allowRoles().allowUsers().allowCurrentUser().useIDMode(id_mode);
            if (!roles_p.parse(pos, ast, expected))
                return false;

            roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(ast);
            return true;
        });
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::ON}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }
}


bool ParserCreateSettingsProfileQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{Keyword::ATTACH_SETTINGS_PROFILE}.ignore(pos, expected) && !ParserKeyword{Keyword::ATTACH_PROFILE}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{Keyword::ALTER_SETTINGS_PROFILE}.ignore(pos, expected) || ParserKeyword{Keyword::ALTER_PROFILE}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{Keyword::CREATE_SETTINGS_PROFILE}.ignore(pos, expected) && !ParserKeyword{Keyword::CREATE_PROFILE}.ignore(pos, expected))
            return false;
    }

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    if (alter)
    {
        if (ParserKeyword{Keyword::IF_EXISTS}.ignore(pos, expected))
            if_exists = true;
    }
    else
    {
        if (ParserKeyword{Keyword::IF_NOT_EXISTS}.ignore(pos, expected))
            if_not_exists = true;
        else if (ParserKeyword{Keyword::OR_REPLACE}.ignore(pos, expected))
            or_replace = true;
    }

    Strings names;
    if (!parseIdentifiersOrStringLiterals(pos, expected, names))
        return false;

    String new_name;
    boost::intrusive_ptr<ASTSettingsProfileElements> settings;
    boost::intrusive_ptr<ASTAlterSettingsProfileElements> alter_settings;
    String cluster;
    String storage_name;

    while (true)
    {
        if (alter && new_name.empty() && (names.size() == 1) && parseRenameTo(pos, expected, new_name))
            continue;

        if (alter)
        {
            boost::intrusive_ptr<ASTAlterSettingsProfileElements> new_alter_settings;
            if (parseAlterSettings(pos, expected, new_alter_settings))
            {
                if (!alter_settings)
                    alter_settings = make_intrusive<ASTAlterSettingsProfileElements>();
                alter_settings->add(std::move(*new_alter_settings));
                continue;
            }
        }
        else
        {
            boost::intrusive_ptr<ASTSettingsProfileElements> new_settings;
            if (parseSettings(pos, expected, attach_mode, new_settings))
            {
                if (!settings)
                    settings = make_intrusive<ASTSettingsProfileElements>();
                settings->add(std::move(*new_settings));
                continue;
            }
        }

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        if (storage_name.empty() && ParserKeyword{Keyword::IN}.ignore(pos, expected) && parseAccessStorageName(pos, expected, storage_name))
            continue;

        break;
    }

    boost::intrusive_ptr<ASTRolesOrUsersSet> to_roles;
    parseToRoles(pos, expected, attach_mode, to_roles);

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    auto query = make_intrusive<ASTCreateSettingsProfileQuery>();
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
    query->alter_settings = std::move(alter_settings);
    query->to_roles = std::move(to_roles);
    query->storage_name = std::move(storage_name);

    return true;
}
}

namespace DB
{

REGISTER_STATEMENTS(SettingsProfile)
{
    factory.registerStatement("CREATE SETTINGS PROFILE",
    {
        .description = R"(
Creates a settings profile - a named set of settings with optional constraints, which can be assigned to users and
roles.
)",
        .syntax = R"(
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]]
    [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
)",
        .examples = {{"Create a settings profile and assign it to a user", R"(
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin;
)", ""}},
        .parent = "CREATE",
        .related = {"ALTER SETTINGS PROFILE", "CREATE USER", "CREATE ROLE", "SET", "DROP", "SHOW"},
    });

    factory.registerStatement("ALTER SETTINGS PROFILE",
    {
        .description = R"(
Changes a settings profile: renames it, changes its settings and constraints, and the users and roles it is assigned
to. A bare `SETTINGS` or `INHERIT` clause replaces all previously defined settings of the profile, whereas
`ADD SETTINGS` and `MODIFY SETTINGS` change individual settings.
)",
        .syntax = R"(
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
)",
        .examples = {{"Replace the settings of a profile", "ALTER SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360;", ""}},
        .parent = "ALTER",
        .related = {"CREATE SETTINGS PROFILE", "ALTER", "SET", "SHOW"},
    });
}

}
