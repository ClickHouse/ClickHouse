#include <Parsers/ParserCreateUserQuery.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseUserName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTUserNameWithHost.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/ParserUserNameWithHost.h>
#include <Parsers/ParserRolesOrUsersSet.h>
#include <Parsers/ASTSettingsProfileElement.h>
#include <Parsers/ParserSettingsProfileElement.h>
#include <common/range.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>


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

            return parseUserName(pos, expected, new_name);
        });
    }


    bool parseAuthentication(IParserBase::Pos & pos, Expected & expected, Authentication & authentication)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword{"NOT IDENTIFIED"}.ignore(pos, expected))
            {
                authentication = Authentication{Authentication::NO_PASSWORD};
                return true;
            }

            if (!ParserKeyword{"IDENTIFIED"}.ignore(pos, expected))
                return false;

            std::optional<Authentication::Type> type;
            bool expect_password = false;
            bool expect_hash = false;
            bool expect_ldap_server_name = false;
            bool expect_kerberos_realm = false;

            if (ParserKeyword{"WITH"}.ignore(pos, expected))
            {
                for (auto check_type : collections::range(Authentication::MAX_TYPE))
                {
                    if (ParserKeyword{Authentication::TypeInfo::get(check_type).raw_name}.ignore(pos, expected))
                    {
                        type = check_type;

                        if (check_type == Authentication::LDAP)
                            expect_ldap_server_name = true;
                        else if (check_type == Authentication::KERBEROS)
                            expect_kerberos_realm = true;
                        else if (check_type != Authentication::NO_PASSWORD)
                            expect_password = true;

                        break;
                    }
                }

                if (!type)
                {
                    if (ParserKeyword{"SHA256_HASH"}.ignore(pos, expected))
                    {
                        type = Authentication::SHA256_PASSWORD;
                        expect_hash = true;
                    }
                    else if (ParserKeyword{"DOUBLE_SHA1_HASH"}.ignore(pos, expected))
                    {
                        type = Authentication::DOUBLE_SHA1_PASSWORD;
                        expect_hash = true;
                    }
                    else
                        return false;
                }
            }

            if (!type)
            {
                type = Authentication::SHA256_PASSWORD;
                expect_password = true;
            }

            String value;
            if (expect_password || expect_hash)
            {
                ASTPtr ast;
                if (!ParserKeyword{"BY"}.ignore(pos, expected) || !ParserStringLiteral{}.parse(pos, ast, expected))
                    return false;

                value = ast->as<const ASTLiteral &>().value.safeGet<String>();
            }
            else if (expect_ldap_server_name)
            {
                ASTPtr ast;
                if (!ParserKeyword{"SERVER"}.ignore(pos, expected) || !ParserStringLiteral{}.parse(pos, ast, expected))
                    return false;

                value = ast->as<const ASTLiteral &>().value.safeGet<String>();
            }
            else if (expect_kerberos_realm)
            {
                if (ParserKeyword{"REALM"}.ignore(pos, expected))
                {
                    ASTPtr ast;
                    if (!ParserStringLiteral{}.parse(pos, ast, expected))
                        return false;

                    value = ast->as<const ASTLiteral &>().value.safeGet<String>();
                }
            }

            authentication = Authentication{*type};
            if (expect_password)
                authentication.setPassword(value);
            else if (expect_hash)
                authentication.setPasswordHashHex(value);
            else if (expect_ldap_server_name)
                authentication.setLDAPServerName(value);
            else if (expect_kerberos_realm)
                authentication.setKerberosRealm(value);

            return true;
        });
    }


    bool parseHostsWithoutPrefix(IParserBase::Pos & pos, Expected & expected, AllowedClientHosts & hosts)
    {
        AllowedClientHosts res_hosts;

        auto parse_host = [&]
        {
            if (ParserKeyword{"NONE"}.ignore(pos, expected))
                return true;

            if (ParserKeyword{"ANY"}.ignore(pos, expected))
            {
                res_hosts.addAnyHost();
                return true;
            }

            if (ParserKeyword{"LOCAL"}.ignore(pos, expected))
            {
                res_hosts.addLocalHost();
                return true;
            }

            if (ParserKeyword{"REGEXP"}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected))
                    return false;

                for (const auto & name_regexp_ast : ast->children)
                    res_hosts.addNameRegexp(name_regexp_ast->as<const ASTLiteral &>().value.safeGet<String>());
                return true;
            }

            if (ParserKeyword{"NAME"}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected))
                    return false;

                for (const auto & name_ast : ast->children)
                    res_hosts.addName(name_ast->as<const ASTLiteral &>().value.safeGet<String>());

                return true;
            }

            if (ParserKeyword{"IP"}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected))
                    return false;

                for (const auto & subnet_ast : ast->children)
                    res_hosts.addSubnet(subnet_ast->as<const ASTLiteral &>().value.safeGet<String>());

                return true;
            }

            if (ParserKeyword{"LIKE"}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected))
                    return false;

                for (const auto & pattern_ast : ast->children)
                    res_hosts.addLikePattern(pattern_ast->as<const ASTLiteral &>().value.safeGet<String>());

                return true;
            }

            return false;
        };

        if (!ParserList::parseUtil(pos, expected, parse_host, false))
            return false;

        hosts = std::move(res_hosts);
        return true;
    }


    bool parseHosts(IParserBase::Pos & pos, Expected & expected, const String & prefix, AllowedClientHosts & hosts)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!prefix.empty() && !ParserKeyword{prefix.c_str()}.ignore(pos, expected))
                return false;

            if (!ParserKeyword{"HOST"}.ignore(pos, expected))
                return false;

            AllowedClientHosts res_hosts;
            if (!parseHostsWithoutPrefix(pos, expected, res_hosts))
                return false;

            hosts.add(std::move(res_hosts));
            return true;
        });
    }


    bool parseDefaultRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, std::shared_ptr<ASTRolesOrUsersSet> & default_roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"DEFAULT ROLE"}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            ParserRolesOrUsersSet default_roles_p;
            default_roles_p.allowAll().allowRoles().useIDMode(id_mode);
            if (!default_roles_p.parse(pos, ast, expected))
                return false;

            default_roles = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(ast);
            default_roles->allow_users = false;
            return true;
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

            settings = std::move(new_settings_ast->as<const ASTSettingsProfileElements &>().elements);
            return true;
        });
    }

    bool parseGrantees(IParserBase::Pos & pos, Expected & expected, bool id_mode, std::shared_ptr<ASTRolesOrUsersSet> & grantees)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"GRANTEES"}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            ParserRolesOrUsersSet grantees_p;
            grantees_p.allowAny().allowUsers().allowCurrentUser().allowRoles().useIDMode(id_mode);
            if (!grantees_p.parse(pos, ast, expected))
                return false;

            grantees = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(ast);
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


bool ParserCreateUserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{"ATTACH USER"}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{"ALTER USER"}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{"CREATE USER"}.ignore(pos, expected))
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

    ASTPtr names_ast;
    if (!ParserUserNamesWithHost{}.parse(pos, names_ast, expected))
        return false;
    auto names = typeid_cast<std::shared_ptr<ASTUserNamesWithHost>>(names_ast);
    auto names_ref = names->names;

    String new_name;
    std::optional<Authentication> authentication;
    std::optional<AllowedClientHosts> hosts;
    std::optional<AllowedClientHosts> add_hosts;
    std::optional<AllowedClientHosts> remove_hosts;
    std::shared_ptr<ASTRolesOrUsersSet> default_roles;
    std::shared_ptr<ASTSettingsProfileElements> settings;
    std::shared_ptr<ASTRolesOrUsersSet> grantees;
    String cluster;

    while (true)
    {
        if (!authentication)
        {
            Authentication new_authentication;
            if (parseAuthentication(pos, expected, new_authentication))
            {
                authentication = std::move(new_authentication);
                continue;
            }
        }

        AllowedClientHosts new_hosts;
        if (parseHosts(pos, expected, "", new_hosts))
        {
            if (!hosts)
                hosts.emplace();
            hosts->add(new_hosts);
            continue;
        }

        std::vector<std::shared_ptr<ASTSettingsProfileElement>> new_settings;
        if (parseSettings(pos, expected, attach_mode, new_settings))
        {
            if (!settings)
                settings = std::make_shared<ASTSettingsProfileElements>();
            boost::range::push_back(settings->elements, std::move(new_settings));
            continue;
        }

        if (!default_roles && parseDefaultRoles(pos, expected, attach_mode, default_roles))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        if (!grantees && parseGrantees(pos, expected, attach_mode, grantees))
            continue;

        if (alter)
        {
            if (new_name.empty() && (names->size() == 1) && parseRenameTo(pos, expected, new_name))
                continue;

            if (parseHosts(pos, expected, "ADD", new_hosts))
            {
                if (!add_hosts)
                    add_hosts.emplace();
                add_hosts->add(new_hosts);
                continue;
            }

            if (parseHosts(pos, expected, "DROP", new_hosts))
            {
                if (!remove_hosts)
                    remove_hosts.emplace();
                remove_hosts->add(new_hosts);
                continue;
            }
        }

        break;
    }

    if (!alter && !hosts)
    {
        String common_host_pattern;
        if (names->getHostPatternIfCommon(common_host_pattern) && !common_host_pattern.empty())
        {
            hosts.emplace().addLikePattern(common_host_pattern);
            names->concatParts();
        }
    }
    else if (alter)
        names->concatParts();

    auto query = std::make_shared<ASTCreateUserQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->new_name = std::move(new_name);
    query->authentication = std::move(authentication);
    query->hosts = std::move(hosts);
    query->add_hosts = std::move(add_hosts);
    query->remove_hosts = std::move(remove_hosts);
    query->default_roles = std::move(default_roles);
    query->settings = std::move(settings);
    query->grantees = std::move(grantees);

    return true;
}
}
