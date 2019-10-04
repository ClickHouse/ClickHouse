#include <Parsers/ParserCreateUserQuery.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseUserName.h>
#include <Access/EncryptedPassword.h>


namespace DB
{
bool ParserCreateUserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter;
    if (ParserKeyword{"CREATE"}.ignore(pos, expected))
        alter = false;
    else if (ParserKeyword{"ALTER"}.ignore(pos, expected))
        alter = true;
    else
        return false;

    if (!ParserKeyword{"USER"}.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTCreateUserQuery>();
    query->alter = alter;

    query->if_not_exists = false;
    if (!alter)
    {
        ParserKeyword if_not_exists_p("IF NOT EXISTS");
        if (if_not_exists_p.ignore(pos, expected))
            query->if_not_exists = true;
    }

    AllowedHosts allowed_hosts_from_user_name;
    if (!parseUserName(pos, expected, query->user_name, allowed_hosts_from_user_name))
        return false;

    do
    {
        if (!query->authentication && ParserKeyword{"IDENTIFY"}.ignore(pos, expected))
        {
            if (!parseAuthentication(pos, expected, *query))
                return false;
            continue;
        }
        if (!query->allowed_hosts && ParserKeyword{"HOST"}.ignore(pos, expected))
        {
            if (!parseAllowedHosts(pos, expected, *query))
                return false;
            continue;
        }
        if (!query->default_roles && ParserKeyword{"DEFAULT ROLE"}.ignore(pos, expected))
        {
            if (!parseDefaultRoles(pos, expected, *query))
                return false;
            continue;
        }
        if (!query->settings && ParserKeyword{"SETTINGS"}.ignore(pos, expected))
        {
            if (!parseSettings(pos, expected, *query))
                return false;
            continue;
        }
        if (!query->account_lock && ParserKeyword{"ACCOUNT"}.ignore(pos, expected))
        {
            if (!parseAccountLock(pos, expected, *query))
                return false;
            continue;
        }
    }
    while (false);

    if (!query->allowed_hosts)
    {
        query->allowed_hosts.emplace();
        auto & ah = *(query->allowed_hosts);
        for (const auto & host_name : allowed_hosts_from_user_name.getHostNames())
            ah.host_names.emplace_back(std::make_shared<ASTLiteral>(host_name));
        for (const auto & host_regexp : allowed_hosts_from_user_name.getHostRegexps())
            ah.host_regexps.emplace_back(std::make_shared<ASTLiteral>(host_regexp));
        for (const auto & ip_address : allowed_hosts_from_user_name.getIPAddresses())
            ah.ip_addresses.emplace_back(std::make_shared<ASTLiteral>(ip_address.toString()));
        for (const auto & ip_subnet : allowed_hosts_from_user_name.getIPSubnets())
            ah.ip_addresses.emplace_back(std::make_shared<ASTLiteral>(ip_subnet.toString()));
    }

    node = query;
    return true;
}


bool ParserCreateUserQuery::parseAuthentication(Pos & pos, Expected & expected, ASTCreateUserQuery & query)
{
    using Authentication = ASTCreateUserQuery::Authentication;
    Authentication::Type type = Authentication::SHA256_PASSWORD;
    ParserKeyword with_p("WITH");
    if (with_p.ignore(pos, expected))
    {
        if (ParserKeyword{"PLAINTEXT_PASSWORD"}.ignore(pos, expected))
            type = Authentication::PLAINTEXT_PASSWORD;
        else if (ParserKeyword{"SHA256_PASSWORD"}.ignore(pos, expected))
            type = Authentication::SHA256_PASSWORD;
        else if (ParserKeyword{"SHA256_HASH"}.ignore(pos, expected))
            type = Authentication::SHA256_HASH;
        else
            return false;
    }

    ParserKeyword by_p("BY");
    ParserExpression password_p;
    ASTPtr password_ast;
    if (!by_p.ignore(pos, expected) || !password_p.parse(pos, password_ast, expected))
        return false;

    String password_string;
    auto * password_literal = password_ast->as<ASTLiteral>();
    if (password_literal && password_literal->value.tryGet(password_string))
    {
        if (type == Authentication::SHA256_PASSWORD)
        {
            type = Authentication::SHA256_HASH;
            password_literal->value = EncryptedPassword{}.setPassword(EncryptedPassword::SHA256, password_string).getHashHex();
        }
    }

    query.authentication.emplace();
    query.authentication->type = type;
    query.authentication->password = std::move(password_ast);
    return true;
}


bool ParserCreateUserQuery::parseAllowedHosts(Pos & pos, Expected & expected, ASTCreateUserQuery & query)
{
    ASTs host_names, host_regexps, ip_addresses;

    if (ParserKeyword{"NONE"}.ignore(pos, expected))
    {
    }
    else if (ParserKeyword{"ANY"}.ignore(pos, expected))
    {
        ip_addresses.emplace_back(std::make_shared<ASTLiteral>("::/0"));
    }
    else
    {
        ParserList list_p{std::make_unique<ParserExpression>(), std::make_unique<ParserToken>(TokenType::Comma), false};
        do
        {
            if (ParserKeyword{"NAME"}.ignore(pos, expected))
            {
                ASTPtr list;
                if (!list_p.parse(pos, list, expected))
                    return false;
                host_names.insert(host_names.end(), list->children.begin(), list->children.end());
                continue;
            }

            if (ParserKeyword{"REGEXP"}.ignore(pos, expected))
            {
                ASTPtr list;
                if (!list_p.parse(pos, list, expected))
                    return false;
                host_regexps.insert(host_regexps.end(), list->children.begin(), list->children.end());
                continue;
            }

            if (ParserKeyword{"IP"}.ignore(pos, expected))
            {
                ASTPtr list;
                if (!list_p.parse(pos, list, expected))
                    return false;
                ip_addresses.insert(ip_addresses.end(), list->children.begin(), list->children.end());
                continue;
            }
        }
        while(false);
    }

    query.allowed_hosts.emplace();
    query.allowed_hosts->host_names = std::move(host_names);
    query.allowed_hosts->host_regexps = std::move(host_regexps);
    query.allowed_hosts->ip_addresses = std::move(ip_addresses);
    return true;
}


bool ParserCreateUserQuery::parseDefaultRoles(Pos & pos, Expected & expected, ASTCreateUserQuery & query)
{
    Strings role_names;
    bool all_granted = false;
    if (ParserKeyword{"NONE"}.ignore(pos, expected))
    {
    }
    else if (query.alter && ParserKeyword{"ALL"}.ignore(pos, expected))
    {
        all_granted = true;
    }
    else
    {
        ParserToken comma{TokenType::Comma};
        do
        {
            String role_name;
            if (!parseRoleName(pos, expected, role_name))
                return false;
            role_names.emplace_back(std::move(role_name));
        }
        while (comma.ignore(pos, expected));
    }

    query.default_roles.emplace();
    query.default_roles->role_names = std::move(role_names);
    query.default_roles->all_granted = all_granted;
    return true;
}


bool ParserCreateUserQuery::parseSettings(Pos & pos, Expected & expected, ASTCreateUserQuery & query)
{
    ParserIdentifier name_p;
    ParserExpression value_p;
    ParserToken eq_p(TokenType::Equals);
    ParserKeyword min_p("MIN");
    ParserKeyword max_p("MAX");
    ParserKeyword readonly_p("READONLY");
    ParserToken comma_p(TokenType::Comma);

    using Setting = ASTCreateUserQuery::Setting;
    using Settings = ASTCreateUserQuery::Settings;
    Settings settings;

    if (ParserKeyword{"NONE"}.ignore(pos, expected))
    {
    }
    else
    {
        do
        {
            ASTPtr name;
            if (!name_p.parse(pos, name, expected))
                return false;

            Setting setting;
            setting.name = getIdentifierName(name);
            setting.read_only = false;

            do
            {
                if (eq_p.ignore(pos, expected))
                {
                    if (!value_p.parse(pos, setting.value, expected))
                        return false;
                    continue;
                }
                if (min_p.ignore(pos, expected))
                {
                    if (!value_p.parse(pos, setting.min, expected))
                        return false;
                    continue;
                }
                if (max_p.ignore(pos, expected))
                {
                    if (!value_p.parse(pos, setting.max, expected))
                        return false;
                    continue;
                }
                if (readonly_p.ignore(pos, expected))
                {
                    setting.read_only = true;
                    continue;
                }
            }
            while (false);

            settings.emplace_back(std::move(setting));
        }
        while (comma_p.ignore(pos, expected));
    }

    query.settings.emplace();
    *query.settings = std::move(settings);
    return true;
}


bool ParserCreateUserQuery::parseAccountLock(Pos & pos, Expected & expected, ASTCreateUserQuery & query)
{
    bool locked;
    if (ParserKeyword{"LOCK"}.ignore(pos, expected))
        locked = true;
    else if (ParserKeyword{"UNLOCK"}.ignore(pos, expected))
        locked = true;
    else
        return false;

    query.account_lock.emplace();
    query.account_lock->locked = locked;
    return true;
}
}
