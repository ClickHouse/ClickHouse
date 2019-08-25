#include <Account/Role.h>
#include <Account/IAccessStorage.h>
#include <Common/Exception.h>
#include <boost/algorithm/string/join.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int NO_SUCH_GRANT;
    extern const int NOT_GRANTED_ROLE_SET;
}


RoleAttributesPtr Role::getAttributes() const
{
    auto attrs = storage->read(id);
    attrs->as<RoleAttributes>();
    return std::static_pointer_cast<const RoleAttributes>(attrs);
}


void Role::grant(Privileges::Types access, bool with_grant_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        a.privileges.grant(access);
        if (with_grant_option)
            a.grant_options.grant(access);
    });
}


void Role::grant(Privileges::Types access, const String & database, bool with_grant_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        a.privileges.grant(access, database);
        if (with_grant_option)
            a.grant_options.grant(access, database);
    });
}


void Role::grant(Privileges::Types access, const String & database, const String & table, bool with_grant_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        a.privileges.grant(access, database, table);
        if (with_grant_option)
            a.grant_options.grant(access, database, table);
    });
}


void Role::grant(Privileges::Types access, const String & database, const String & table, const String & column, bool with_grant_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        a.privileges.grant(access, database, table, column);
        if (with_grant_option)
            a.grant_options.grant(access, database, table, column);
    });
}


void Role::grant(Privileges::Types access, const String & database, const String & table, const Strings & columns, bool with_grant_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        a.privileges.grant(access, database, table, columns);
        if (with_grant_option)
            a.grant_options.grant(access, database, table, columns);
    });
}


void Role::revoke(Privileges::Types access, bool only_grant_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        bool revoked = a.grant_options.revoke(access);
        if (!only_grant_option)
            revoked |= a.privileges.revoke(access);
        if (!revoked)
            throw Exception("No such grant: " + Privileges::accessToString(access), ErrorCodes::NO_SUCH_GRANT);
    });
}


void Role::revoke(Privileges::Types access, const String & database, bool only_grant_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        bool revoked = a.grant_options.revoke(access, database);
        if (!only_grant_option)
            revoked |= a.privileges.revoke(access, database);
        if (!revoked)
            throw Exception("No such grant: " + Privileges::accessToString(access, database), ErrorCodes::NO_SUCH_GRANT);
    });
}


void Role::revoke(Privileges::Types access, const String & database, const String & table, bool only_grant_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        bool revoked = a.grant_options.revoke(access, database, table);
        if (!only_grant_option)
            revoked |= a.privileges.revoke(access, database, table);
        if (!revoked)
            throw Exception("No such grant: " + Privileges::accessToString(access, database, table), ErrorCodes::NO_SUCH_GRANT);
    });
}


void Role::revoke(Privileges::Types access, const String & database, const String & table, const String & column, bool only_grant_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        bool revoked = a.grant_options.revoke(access, database, table, column);
        if (!only_grant_option)
            revoked |= a.privileges.revoke(access, database, table, column);
        if (!revoked)
            throw Exception("No such grant: " + Privileges::accessToString(access, database, table, column), ErrorCodes::NO_SUCH_GRANT);
    });
}


void Role::revoke(Privileges::Types access, const String & database, const String & table, const Strings & columns, bool only_grant_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        bool revoked = a.grant_options.revoke(access, database, table, columns);
        if (!only_grant_option)
            revoked |= a.privileges.revoke(access, database, table, columns);
        if (!revoked)
            throw Exception("No such grant: " + Privileges::accessToString(access, database, table, columns), ErrorCodes::NO_SUCH_GRANT);
    });
}


void Role::grantRole(const Role & role, bool with_admin_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        a.granted_roles[role.getID()].with_admin_option |= with_admin_option;
    });
}


void Role::revokeRole(const Role & role, bool only_admin_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        auto it = a.granted_roles.find(role.getID());
        bool revoked = false;
        if (it != a.granted_roles.end())
        {
            if (only_admin_option)
            {
                if (it->second.with_admin_option)
                {
                    it->second.with_admin_option = false;
                    revoked = true;
                }
            }
            else
            {
                a.granted_roles.erase(it);
                revoked = true;
            }
        }
        if (!revoked)
            throw Exception("Role " + role.getName() + " is not granted", ErrorCodes::NO_SUCH_GRANT);
    });
}


void Role::revokeRoles(const std::vector<Role> & roles, bool only_admin_option)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        bool revoked = false;
        for (const auto & role : roles)
        {
            auto it = a.granted_roles.find(role.getID());
            if (it != a.granted_roles.end())
            {
                if (only_admin_option)
                {
                    if (it->second.with_admin_option)
                    {
                        it->second.with_admin_option = false;
                        revoked = true;
                    }
                }
                else
                {
                    a.granted_roles.erase(it);
                    revoked = true;
                }
            }
        }
        if (!revoked)
        {
            String message = "None of roles ";
            for (size_t i = 0; i != roles.size(); ++i)
                message += (i ? ", " : "") + roles[i].getName();
            message += " is granted";
            throw Exception(message, ErrorCodes::NO_SUCH_GRANT);
        }
    });
}


void Role::setDefaultRoles(const std::vector<Role> & roles)
{
    storage->write(id, [&](IAccessAttributes & attrs)
    {
        auto & a = attrs.as<RoleAttributes>();
        for (const auto & role : roles)
            if (!a.granted_roles.count(role.getID()))
                throw Exception("Role " + role.getName() + " is not granted, only granted roles can be set", ErrorCodes::NOT_GRANTED_ROLE_SET);
        for (auto & params : a.granted_roles)
            params.second.enabled_by_default = false;
        for (const auto & role : roles)
            a.granted_roles[role.getID()].enabled_by_default = true;
    });
}


void Role::drop()
{
    storage->drop(id);
}
}
