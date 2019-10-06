#include <Parsers/ParserCreateUserQuery.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseUserName.h>
#include <Access/Authentication.h>


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
        query->allowed_hosts.emplace(allowed_hosts_from_user_name);

    node = query;
    return true;
}


bool ParserCreateUserQuery::parseAuthentication(Pos & pos, Expected & expected, ASTCreateUserQuery & query)
{
    Authentication authentication;

    bool need_password = false;
    bool need_password_hash = false;
    if (ParserKeyword{"WITH"}.ignore(pos, expected))
    {
        if (ParserKeyword{"PLAINTEXT_PASSWORD"}.ignore(pos, expected))
            authentication.setType(Authentication::PLAINTEXT_PASSWORD);
        else if (ParserKeyword{"SHA256_PASSWORD"}.ignore(pos, expected))
        {
            authentication.setType(Authentication::SHA256_PASSWORD);
            need_password = true;
        }
        else if (ParserKeyword{"SHA256_HASH"}.ignore(pos, expected))
        {
            authentication.setType(Authentication::SHA256_PASSWORD);
            need_password_hash = true;
        }
        else
            return false;
    }

    if (need_password || need_password_hash)
    {
        ASTPtr password_ast;
        if (!ParserKeyword{"BY"}.ignore(pos, expected) || !ParserStringLiteral{}.parse(pos, password_ast, expected))
            return false;

        String password = password_ast->as<const ASTLiteral &>().value.safeGet<String>();
        if (need_password)
            authentication.setPassword(password);
        else
            authentication.setPasswordHash(password);
    }

    query.authentication.emplace(authentication);
    return true;
}


bool ParserCreateUserQuery::parseAllowedHosts(Pos & pos, Expected & expected, ASTCreateUserQuery & query)
{
    AllowedHosts allowed_hosts;

    if (ParserKeyword{"NONE"}.ignore(pos, expected))
    {
    }
    else if (ParserKeyword{"ANY"}.ignore(pos, expected))
    {
        allowed_hosts.addIPSubnet(AllowedHosts::IPSubnet::ALL_ADDRESSES);
    }
    else
    {
        ParserToken comma{TokenType::Comma};
        do
        {
            if (ParserKeyword{"NAME"}.ignore(pos, expected))
            {
                ASTPtr host_name;
                if (!ParserStringLiteral().parse(pos, host_name, expected))
                    return false;

                allowed_hosts.addHostName(host_name->as<ASTLiteral &>().value.safeGet<String>());
                continue;
            }

            if (ParserKeyword{"REGEXP"}.ignore(pos, expected))
            {
                ASTPtr host_regexp;
                if (!ParserStringLiteral().parse(pos, host_regexp, expected))
                    return false;

                allowed_hosts.addHostRegexp(host_regexp->as<ASTLiteral &>().value.safeGet<String>());
                continue;
            }

            if (ParserKeyword{"IP"}.ignore(pos, expected))
            {
                ASTPtr ip_address;
                if (!ParserStringLiteral().parse(pos, ip_address, expected))
                    return false;

                allowed_hosts.addIPSubnet(ip_address->as<ASTLiteral &>().value.safeGet<String>());
                continue;
            }
        }
        while(false);
    }

    query.allowed_hosts.emplace(std::move(allowed_hosts));
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
    ParserLiteral value_p;
    ParserToken eq_p(TokenType::Equals);
    ParserKeyword min_p("MIN");
    ParserKeyword max_p("MAX");
    ParserKeyword readonly_p("READONLY");
    ParserToken comma_p(TokenType::Comma);

    SettingsChanges settings;
    SettingsConstraints settings_constraints;

    if (ParserKeyword{"NONE"}.ignore(pos, expected))
    {
    }
    else
    {
        do
        {
            ASTPtr name_ast;
            if (!name_p.parse(pos, name_ast, expected))
                return false;

            String name = getIdentifierName(name_ast);

            do
            {
                if (eq_p.ignore(pos, expected))
                {
                    ASTPtr value;
                    if (!value_p.parse(pos, value, expected))
                        return false;
                    settings.push_back({name, value->as<const ASTLiteral &>().value});
                    continue;
                }
                if (min_p.ignore(pos, expected))
                {
                    ASTPtr min;
                    if (!value_p.parse(pos, min, expected))
                        return false;
                    settings_constraints.setMinValue(name, min->as<const ASTLiteral &>().value);
                    continue;
                }
                if (max_p.ignore(pos, expected))
                {
                    ASTPtr max;
                    if (!value_p.parse(pos, max, expected))
                        return false;
                    settings_constraints.setMaxValue(name, max->as<const ASTLiteral &>().value);
                    continue;
                }
                if (readonly_p.ignore(pos, expected))
                {
                    settings_constraints.setReadOnly(name, true);
                    continue;
                }
            }
            while (false);
        }
        while (comma_p.ignore(pos, expected));
    }

    query.settings.emplace(std::move(settings));
    query.settings_constraints.emplace(std::move(settings_constraints));
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
    query.account_lock->account_locked = locked;
    return true;
}
}
