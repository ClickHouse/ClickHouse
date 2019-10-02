#include <Interpreters/InterpreterCreateACQuery.h>
#include <Parsers/ASTCreateACQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <AccessControl/AccessControlManager.h>
#include <AccessControl/User2.h>
#include <IO/ReadHelpers.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{
BlockIO InterpreterCreateRoleQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateRoleQuery &>();

    std::vector<AttributesPtr> roles;
    roles.reserve(query.role_names.size());
    for (const auto & role_name : query.role_names)
    {
        auto role = std::make_shared<Role>();
        role->name = serializeAST(*role_name);
        roles.emplace_back(std::move(role));
    }

    if (query.if_not_exists)
        context.getAccessControlManager().tryInsert(roles);
    else
        context.getAccessControlManager().insert(roles);

    return {};
}


BlockIO InterpreterCreateUserQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateUserQuery &>();
    User2 user;
    changeUser(user, query);

    if (query.if_not_exists)
        context.getAccessControlManager().tryInsert(user);
    else
        context.getAccessControlManager().insert(user);

    return {};
}


void InterpreterCreateUserQuery::changeUser(User2 & user, const ASTCreateUserQuery & ast) const
{
    user.name = serializeAST(*ast.user_name);

    if (ast.authentication)
        changeUser(user, ast.authentication->as<const ASTAuthentication &>());

    if (ast.allowed_hosts)
        changeUser(user, ast.allowed_hosts->as<const ASTAllowedHosts &>());

    if (ast.default_roles)
        changeUser(user, ast.default_roles->as<const ASTDefaultRoles &>());
}


void InterpreterCreateUserQuery::changeUser(User2 & user, const ASTAuthentication & ast) const
{
    switch (ast.type)
    {
        case ASTAuthentication::NO_PASSWORD:
            user.password.clear();
            break;
        case ASTAuthentication::PLAINTEXT_PASSWORD:
            user.password.setPassword(EncryptedPassword::PLAINTEXT, ast.password->as<const ASTLiteral &>().value.safeGet<String>());
            break;
        case ASTAuthentication::SHA256_PASSWORD:
            user.password.setPassword(EncryptedPassword::SHA256, ast.password->as<const ASTLiteral &>().value.safeGet<String>());
            break;
        case ASTAuthentication::SHA256_HASH:
            user.password.setHashHex(EncryptedPassword::SHA256, ast.password->as<const ASTLiteral &>().value.safeGet<String>());
            break;
    }
}


void InterpreterCreateUserQuery::changeUser(User2 & user, const ASTAllowedHosts & ast) const
{
    user.allowed_hosts.clear();
    if (ast.any_host)
    {
        user.allowed_hosts.addIPSubnet({}, 0);
    }
    else
    {
        for (const auto & name_ast : ast.host_names)
            user.allowed_hosts.addHost(name_ast->as<ASTLiteral &>().value.safeGet<String>());

        for (const auto & regexp_ast : ast.host_regexps)
            user.allowed_hosts.addHostRegexp(regexp_ast->as<ASTLiteral &>().value.safeGet<String>());

        for (const auto & ip_address_ast : ast.ip_addresses)
        {
            String ip_address = ip_address_ast->as<ASTLiteral &>().value.safeGet<String>();
            size_t slash = ip_address.find('/');
            if (slash == String::npos)
            {
                user.allowed_hosts.addIPAddress(Poco::Net::IPAddress(ip_address));
            }
            else
            {
                String prefix(ip_address, 0, slash);
                String mask(ip_address, slash + 1, ip_address.length() - slash - 1);
                if (std::all_of(mask.begin(), mask.end(), isNumericASCII))
                    user.allowed_hosts.addIPSubnet(Poco::Net::IPAddress(prefix), parseFromString<UInt8>(mask));
                else
                    user.allowed_hosts.addIPSubnet(Poco::Net::IPAddress(prefix), Poco::Net::IPAddress(mask));
            }
        }
    }
}


void InterpreterCreateUserQuery::changeUser(User2 & user, const ASTDefaultRoles & ast) const
{
    user.default_roles.clear();
    for (const auto & role_name : ast.role_names)
    {
        UUID role_id = context.getAccessControlManager().getID<Role>(serializeAST(*role_name));
        user.granted_roles_by_admin_option[false].emplace(role_id);
        user.default_roles.emplace(role_id);
    }
}


}
