#include <Parsers/ParserCreateAccessQuery.h>
#include <Parsers/ASTCreateAccessQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Access/EncryptedPassword.h>


namespace DB
{
bool ParserAuthentication::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTAuthentication::Type type = ASTAuthentication::SHA256_PASSWORD;
    ParserKeyword with_p("WITH");
    if (with_p.ignore(pos, expected))
    {
        if (ParserKeyword{"PLAINTEXT_PASSWORD"}.ignore(pos, expected))
            type = ASTAuthentication::PLAINTEXT_PASSWORD;
        else if (ParserKeyword{"SHA256_PASSWORD"}.ignore(pos, expected))
            type = ASTAuthentication::SHA256_PASSWORD;
        else if (ParserKeyword{"SHA256_HASH"}.ignore(pos, expected))
            type = ASTAuthentication::SHA256_HASH;
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
        if (type == ASTAuthentication::SHA256_PASSWORD)
        {
            type = ASTAuthentication::SHA256_HASH;
            password_literal->value = EncryptedPassword{}.setPassword(EncryptedPassword::SHA256, password_string).getHashHex();
        }
    }

    auto query = std::make_shared<ASTAuthentication>();
    node = query;

    query->type = type;
    query->password = std::move(password_ast);

    return true;
}


bool ParserAllowedHosts::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
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

    auto query = std::make_shared<ASTAllowedHosts>();
    node = query;

    query->host_names = std::move(host_names);
    query->host_regexps = std::move(host_regexps);
    query->ip_addresses = std::move(ip_addresses);

    return true;
}


bool ParserDefaultRoles::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Strings role_names;
    bool all_granted = false;
    if (ParserKeyword{"NONE"}.ignore(pos, expected))
    {
    }
    else if (alter && ParserKeyword{"ALL"}.ignore(pos, expected))
    {
        all_granted = true;
    }
    else
    {
        ParserToken comma{TokenType::Comma};
        do
        {
            String role_name;
            if (!parseRoleNameAtHost(pos, role_name, expected))
                return false;
            role_names.emplace_back(std::move(role_name));
        }
        while (comma.ignore(pos, expected));
    }

    auto query = std::make_shared<ASTDefaultRoles>();
    node = query;

    query->role_names = std::move(role_names);
    query->alter = alter;
    query->all_granted = all_granted;

    return true;
}


bool ParserCreateRoleQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword create_role_p("CREATE ROLE");
    if (!create_role_p.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    ParserKeyword if_not_exists_p("IF NOT EXISTS");
    if (if_not_exists_p.ignore(pos, expected))
        if_not_exists = true;

    Strings role_names;
    ParserToken comma{TokenType::Comma};
    do
    {
        String role_name;
        if (!parseRoleNameAtHost(pos, role_name, expected))
            return false;
        role_names.emplace_back(std::move(role_name));
    }
    while (comma.ignore(pos, expected));

    auto query = std::make_shared<ASTCreateRoleQuery>();
    node = query;

    query->role_names = std::move(role_names);
    query->if_not_exists = if_not_exists;

    return true;
}


bool ParserCreateUserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword create_user_p("CREATE USER");
    if (!create_user_p.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    ParserKeyword if_not_exists_p("IF NOT EXISTS");
    if (if_not_exists_p.ignore(pos, expected))
        if_not_exists = true;

    String user_name;
    if (!parseUserNameAtHost(pos, user_name, expected))
        return false;

    ASTPtr authentication;
    ASTPtr allowed_hosts;
    ASTPtr default_roles;

    do
    {
        if (!authentication && ParserKeyword{"IDENTIFY"}.ignore(pos, expected))
        {
            if (!ParserAuthentication{}.parse(pos, authentication, expected))
                return false;
            continue;
        }
        if (!allowed_hosts && ParserKeyword{"HOST"}.ignore(pos, expected))
        {
            if (!ParserAllowedHosts{}.parse(pos, allowed_hosts, expected))
                return false;
            continue;
        }
        if (!default_roles && ParserKeyword{"DEFAULT ROLE"}.ignore(pos, expected))
        {
            if (!ParserDefaultRoles{}.parse(pos, default_roles, expected))
                return false;
            continue;
        }
    }
    while (false);

    if (!allowed_hosts)
        allowed_hosts = std::make_shared<ASTAllowedHosts>(user_name);

    auto query = std::make_shared<ASTCreateUserQuery>();
    node = query;

    query->user_name = std::move(user_name);
    query->if_not_exists = if_not_exists;
    query->authentication = std::move(authentication);
    query->allowed_hosts = std::move(allowed_hosts);
    query->default_roles = std::move(default_roles);

    return true;
}
}
