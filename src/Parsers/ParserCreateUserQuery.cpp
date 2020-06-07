#include <Parsers/ParserCreateUserQuery.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseUserName.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Parsers/ParserExtendedRoleSet.h>
#include <Parsers/ASTSettingsProfileElement.h>
#include <Parsers/ParserSettingsProfileElement.h>
#include <ext/range.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{
namespace ErrorCodes
{
}


namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, String & new_name,
                       std::optional<String> & new_host_pattern)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"RENAME TO"}.ignore(pos, expected, ranges))
                return false;

            return parseUserName(pos, expected, ranges, new_name, new_host_pattern);
        });
    }


    bool parseAuthentication(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges,
                             std::optional<Authentication> & authentication)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword{"NOT IDENTIFIED"}.ignore(pos, expected, ranges))
            {
                authentication = Authentication{Authentication::NO_PASSWORD};
                return true;
            }

            if (!ParserKeyword{"IDENTIFIED"}.ignore(pos, expected, ranges))
                return false;

            std::optional<Authentication::Type> type;
            bool expect_password = false;
            bool expect_hash = false;

            if (ParserKeyword{"WITH"}.ignore(pos, expected, ranges))
            {
                for (auto check_type : ext::range(Authentication::MAX_TYPE))
                {
                    if (ParserKeyword{Authentication::TypeInfo::get(check_type).raw_name}.ignore(pos, expected, ranges))
                    {
                        type = check_type;
                        expect_password = (check_type != Authentication::NO_PASSWORD);
                        break;
                    }
                }

                if (!type)
                {
                    if (ParserKeyword{"SHA256_HASH"}.ignore(pos, expected, ranges))
                    {
                        type = Authentication::SHA256_PASSWORD;
                        expect_hash = true;
                    }
                    else if (ParserKeyword{"DOUBLE_SHA1_HASH"}.ignore(pos, expected, ranges))
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

            String password;
            if (expect_password || expect_hash)
            {
                ASTPtr ast;
                if (!ParserKeyword{"BY"}.ignore(pos, expected, ranges) || !ParserStringLiteral{}.parse(pos, ast, expected, ranges))
                    return false;

                password = ast->as<const ASTLiteral &>().value.safeGet<String>();
            }

            authentication = Authentication{*type};
            if (expect_password)
                authentication->setPassword(password);
            else if (expect_hash)
                authentication->setPasswordHashHex(password);

            return true;
        });
    }


    bool parseHosts(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, const char * prefix,
                    std::optional<AllowedClientHosts> & hosts)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (prefix && !ParserKeyword{prefix}.ignore(pos, expected, ranges))
                return false;

            if (!ParserKeyword{"HOST"}.ignore(pos, expected, ranges))
                return false;

            if (ParserKeyword{"ANY"}.ignore(pos, expected, ranges))
            {
                if (!hosts)
                    hosts.emplace();
                hosts->addAnyHost();
                return true;
            }

            if (ParserKeyword{"NONE"}.ignore(pos, expected, ranges))
            {
                if (!hosts)
                    hosts.emplace();
                return true;
            }

            AllowedClientHosts new_hosts;
            do
            {
                if (ParserKeyword{"LOCAL"}.ignore(pos, expected, ranges))
                {
                    new_hosts.addLocalHost();
                }
                else if (ParserKeyword{"REGEXP"}.ignore(pos, expected, ranges))
                {
                    ASTPtr ast;
                    if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected, ranges))
                        return false;

                    for (const auto & name_regexp_ast : ast->children)
                        new_hosts.addNameRegexp(name_regexp_ast->as<const ASTLiteral &>().value.safeGet<String>());
                }
                else if (ParserKeyword{"NAME"}.ignore(pos, expected, ranges))
                {
                    ASTPtr ast;
                    if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected, ranges))
                        return false;

                    for (const auto & name_ast : ast->children)
                        new_hosts.addName(name_ast->as<const ASTLiteral &>().value.safeGet<String>());
                }
                else if (ParserKeyword{"IP"}.ignore(pos, expected, ranges))
                {
                    ASTPtr ast;
                    if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected, ranges))
                        return false;

                    for (const auto & subnet_ast : ast->children)
                        new_hosts.addSubnet(subnet_ast->as<const ASTLiteral &>().value.safeGet<String>());
                }
                else if (ParserKeyword{"LIKE"}.ignore(pos, expected, ranges))
                {
                    ASTPtr ast;
                    if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected, ranges))
                        return false;

                    for (const auto & pattern_ast : ast->children)
                        new_hosts.addLikePattern(pattern_ast->as<const ASTLiteral &>().value.safeGet<String>());
                }
                else
                    return false;
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected, ranges));

            if (!hosts)
                hosts.emplace();
            hosts->add(new_hosts);
            return true;
        });
    }


    bool parseDefaultRoles(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, bool id_mode,
                           std::shared_ptr<ASTExtendedRoleSet> & default_roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"DEFAULT ROLE"}.ignore(pos, expected, ranges))
                return false;

            ASTPtr ast;
            if (!ParserExtendedRoleSet{}.enableCurrentUserKeyword(false).useIDMode(id_mode).parse(pos, ast, expected, ranges))
                return false;

            default_roles = typeid_cast<std::shared_ptr<ASTExtendedRoleSet>>(ast);
            default_roles->can_contain_users = false;
            return true;
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


bool ParserCreateUserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{"ATTACH USER"}.ignore(pos, expected, ranges))
            return false;
    }
    else
    {
        if (ParserKeyword{"ALTER USER"}.ignore(pos, expected, ranges))
            alter = true;
        else if (!ParserKeyword{"CREATE USER"}.ignore(pos, expected, ranges))
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
    std::optional<String> host_pattern;
    if (!parseUserName(pos, expected, ranges, name, host_pattern))
        return false;

    String new_name;
    std::optional<String> new_host_pattern;
    std::optional<Authentication> authentication;
    std::optional<AllowedClientHosts> hosts;
    std::optional<AllowedClientHosts> add_hosts;
    std::optional<AllowedClientHosts> remove_hosts;
    std::shared_ptr<ASTExtendedRoleSet> default_roles;
    std::shared_ptr<ASTSettingsProfileElements> settings;
    String cluster;

    while (true)
    {
        if (!authentication && parseAuthentication(pos, expected, ranges, authentication))
            continue;

        if (parseHosts(pos, expected, ranges, nullptr, hosts))
            continue;

        if (parseSettings(pos, expected, ranges, attach_mode, settings))
            continue;

        if (!default_roles && parseDefaultRoles(pos, expected, ranges, attach_mode, default_roles))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, ranges, cluster))
            continue;

        if (alter)
        {
            if (new_name.empty() && parseRenameTo(pos, expected, ranges, new_name, new_host_pattern))
                continue;

            if (parseHosts(pos, expected, ranges, "ADD", add_hosts) || parseHosts(pos, expected, ranges, "DROP", remove_hosts))
                continue;
        }

        break;
    }

    if (!hosts)
    {
        if (!alter && host_pattern)
            hosts.emplace().addLikePattern(*host_pattern);
        else if (alter && new_host_pattern)
            hosts.emplace().addLikePattern(*new_host_pattern);
    }

    auto query = std::make_shared<ASTCreateUserQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->name = std::move(name);
    query->new_name = std::move(new_name);
    query->authentication = std::move(authentication);
    query->hosts = std::move(hosts);
    query->add_hosts = std::move(add_hosts);
    query->remove_hosts = std::move(remove_hosts);
    query->default_roles = std::move(default_roles);
    query->settings = std::move(settings);

    return true;
}
}
