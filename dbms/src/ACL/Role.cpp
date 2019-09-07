#include <ACL/Role.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ROLE_NOT_FOUND;
    extern const int ROLE_ALREADY_EXISTS;
}

using Operation = IACLAttributesStorage::Operation;


bool Role::Attributes::equal(const IACLAttributes & other) const
{
    if (!IACLAttributable::Attributes::equal(other))
        return false;
    const auto * o = dynamic_cast<const Attributes *>(&other);
    return o && (privileges == o->privileges) && (grant_options == o->grant_options) && (granted_roles == o->granted_roles);
}


std::shared_ptr<IACLAttributes> Role::Attributes::clone() const
{
    auto result = std::make_shared<Attributes>();
    *result = *this;
    return result;
}


bool Role::Attributes::hasReferences(UUID ref_id) const
{
    return granted_roles.count(ref_id);
}


void Role::Attributes::removeReferences(UUID ref_id)
{
    granted_roles.erase(ref_id);
}


Operation Role::grantOp(Privileges::Types access, const GrantParams & params) const
{
    return [access, params](Attributes & attrs)
    {
        attrs.privileges.grant(access);
        if (params.with_grant_option)
            attrs.grant_options.grant(access);
    };
}


Operation Role::grantOp(Privileges::Types access, const String & database, const GrantParams & params) const
{
    return [access, database, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database);
    };
}


Operation Role::grantOp(Privileges::Types access, const String & database, const String & table, const GrantParams & params) const
{
    return [access, database, table, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database, table);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database, table);
    };
}


Operation Role::grantOp(Privileges::Types access, const String & database, const String & table, const String & column, const GrantParams & params) const
{
    return [access, database, table, column, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database, table, column);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database, table, column);
    };
}


Operation Role::grantOp(Privileges::Types access, const String & database, const String & table, const Strings & columns, const GrantParams & params) const
{
    return [access, database, table, columns, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database, table, columns);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database, table, columns);
    };
}


Operation Role::revokeOp(Privileges::Types access, const RevokeParams & params, bool * revoked) const
{
    return [access, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access);
        if (revoked)
            *revoked = r;
    };
}


Operation Role::revokeOp(Privileges::Types access, const String & database, const RevokeParams & params, bool * revoked) const
{
    return [access, database, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database);
        if (revoked)
            *revoked = r;
    };
}


Operation Role::revokeOp(Privileges::Types access, const String & database, const String & table, const RevokeParams & params, bool * revoked) const
{
    return [access, database, table, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database, table);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database, table);
        if (revoked)
            *revoked = r;
    };
}


Operation Role::revokeOp(Privileges::Types access, const String & database, const String & table, const String & column, const RevokeParams & params, bool * revoked) const
{
    return [access, database, table, column, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database, table, column);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database, table, column);
        if (revoked)
            *revoked = r;
    };
}


Operation Role::revokeOp(Privileges::Types access, const String & database, const String & table, const Strings & columns, const RevokeParams & params, bool * revoked) const
{
    return [access, database, table, columns, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database, table, columns);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database, table, columns);
        if (revoked)
            *revoked = r;
    };
}


Privileges Role::getPrivileges() const
{
    return getAttributesStrict()->privileges;
}


Privileges Role::getGrantOptions() const
{
    return getAttributesStrict()->grant_options;
}


Operation Role::grantRoleOp(const Role & role, const GrantRoleParams & params) const
{
    return [role, params](Attributes & attrs)
    {
        attrs.granted_roles[role.getID()].admin_option |= params.with_admin_option;
    };
}


Operation Role::revokeRoleOp(const Role & role, const RevokeRoleParams & params, bool * revoked) const
{
    return [role, params, revoked](Attributes & attrs)
    {
        bool r = false;
        auto it = attrs.granted_roles.find(role.getID());
        if (it != attrs.granted_roles.end())
        {
            if (params.only_admin_option)
            {
                if (it->second.admin_option)
                {
                    it->second.admin_option = false;
                    r = true;
                }
            }
            else
            {
                attrs.granted_roles.erase(it);
                r = true;
            }
        }
        if (revoked)
            *revoked = r;
    };
}


std::vector<Role> Role::getGrantedRoles() const
{
    auto attrs = getAttributesStrict();
    std::vector<Role> result;
    result.reserve(attrs->granted_roles.size());
    for (const auto & granted_id_with_settings : attrs->granted_roles)
    {
        const auto & granted_id = granted_id_with_settings.first;
        result.push_back({granted_id, getManager()});
    }
    return result;
}


std::vector<Role> Role::getGrantedRolesWithAdminOption() const
{
    auto attrs = getAttributesStrict();
    std::vector<Role> result;
    result.reserve(attrs->granted_roles.size());
    for (const auto & [granted_id, settings] : attrs->granted_roles)
    {
        if (settings.admin_option)
            result.push_back({granted_id, getManager()});
    }
    return result;
}

const String & Role::getTypeName() const
{
    static const String type_name = "role";
    return type_name;
}

int Role::getNotFoundErrorCode() const
{
    return ErrorCodes::ROLE_NOT_FOUND;
}

int Role::getAlreadyExistsErrorCode() const
{
    return ErrorCodes::ROLE_ALREADY_EXISTS;
}
}
