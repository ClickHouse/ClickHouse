#include <ACL/Role.h>
#include <ACL/ACLAttributesType.h>
#include <Common/Exception.h>


namespace DB
{
using Operation = Role::Operation;


ACLAttributesType Role::Attributes::getType() const
{
    return ACLAttributesType::ROLE;
}


bool Role::Attributes::equal(const IACLAttributes & other) const
{
    if (!ACLAttributable::Attributes::equal(other))
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


Role::AttributesPtr Role::getAttributes() const
{
    return std::static_pointer_cast<const Attributes>(ACLAttributable::getAttributes());
}


Role::AttributesPtr Role::getAttributesStrict() const
{
    return std::static_pointer_cast<const Attributes>(ACLAttributable::getAttributesStrict());
}


std::pair<Role::AttributesPtr, IACLAttributableManager *> Role::getAttributesWithManagerStrict() const
{
    auto [attrs, manager] = ACLAttributable::getAttributesWithManagerStrict();
    return {std::static_pointer_cast<const Attributes>(attrs), manager};
}


void Role::grant(Privileges::Types access, const GrantParams & params)
{
    grantOp(access, params).execute();
}


void Role::grant(Privileges::Types access, const String & database, const GrantParams & params)
{
    grantOp(access, database, params).execute();
}


void Role::grant(Privileges::Types access, const String & database, const String & table, const GrantParams & params)
{
    grantOp(access, database, table, params).execute();
}


void Role::grant(
    Privileges::Types access, const String & database, const String & table, const String & column, const GrantParams & params)
{
    grantOp(access, database, table, column, params).execute();
}


void Role::grant(
    Privileges::Types access, const String & database, const String & table, const Strings & columns, const GrantParams & params)
{
    grantOp(access, database, table, columns, params).execute();
}


Operation Role::grantOp(Privileges::Types access, const GrantParams & params)
{
    return prepareOperation([access, params](Attributes & attrs)
    {
        attrs.privileges.grant(access);
        if (params.with_grant_option)
            attrs.grant_options.grant(access);
    });
}


Operation Role::grantOp(Privileges::Types access, const String & database, const GrantParams & params)
{
    return prepareOperation([access, database, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database);
    });
}


Operation Role::grantOp(Privileges::Types access, const String & database, const String & table, const GrantParams & params)
{
    return prepareOperation([access, database, table, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database, table);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database, table);
    });
}


Operation Role::grantOp(Privileges::Types access, const String & database, const String & table, const String & column, const GrantParams & params)
{
    return prepareOperation([access, database, table, column, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database, table, column);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database, table, column);
    });
}


Operation Role::grantOp(Privileges::Types access, const String & database, const String & table, const Strings & columns, const GrantParams & params)
{
    return prepareOperation([access, database, table, columns, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database, table, columns);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database, table, columns);
    });
}


bool Role::revoke(Privileges::Types access, const RevokeParams & params)
{
    bool revoked;
    revokeOp(access, params, &revoked).execute();
    return revoked;
}


bool Role::revoke(Privileges::Types access, const String & database, const RevokeParams & params)
{
    bool revoked;
    revokeOp(access, database, params, &revoked).execute();
    return revoked;
}


bool Role::revoke(Privileges::Types access, const String & database, const String & table, const RevokeParams & params)
{
    bool revoked;
    revokeOp(access, database, table, params, &revoked).execute();
    return revoked;
}


bool Role::revoke(
    Privileges::Types access, const String & database, const String & table, const String & column, const RevokeParams & params)
{
    bool revoked;
    revokeOp(access, database, table, column, params, &revoked).execute();
    return revoked;
}


bool Role::revoke(
    Privileges::Types access, const String & database, const String & table, const Strings & columns, const RevokeParams & params)
{
    bool revoked;
    revokeOp(access, database, table, columns, params, &revoked).execute();
    return revoked;
}


Operation Role::revokeOp(Privileges::Types access, const RevokeParams & params, bool * revoked)
{
    return prepareOperation([access, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access);
        if (revoked)
            *revoked = r;
    });
}


Operation Role::revokeOp(Privileges::Types access, const String & database, const RevokeParams & params, bool * revoked)
{
    return prepareOperation([access, database, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database);
        if (revoked)
            *revoked = r;
    });
}


Operation Role::revokeOp(Privileges::Types access, const String & database, const String & table, const RevokeParams & params, bool * revoked)
{
    return prepareOperation([access, database, table, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database, table);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database, table);
        if (revoked)
            *revoked = r;
    });
}


Operation Role::revokeOp(Privileges::Types access, const String & database, const String & table, const String & column, const RevokeParams & params, bool * revoked)
{
    return prepareOperation([access, database, table, column, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database, table, column);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database, table, column);
        if (revoked)
            *revoked = r;
    });
}


Operation Role::revokeOp(Privileges::Types access, const String & database, const String & table, const Strings & columns, const RevokeParams & params, bool * revoked)
{
    return prepareOperation([access, database, table, columns, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database, table, columns);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database, table, columns);
        if (revoked)
            *revoked = r;
    });
}


Privileges Role::getPrivileges() const
{
    return getAttributesStrict()->privileges;
}


Privileges Role::getGrantOptions() const
{
    return getAttributesStrict()->grant_options;
}


void Role::grantRole(const Role & role, const GrantRoleParams & params)
{
    grantRoleOp(role, params).execute();
}


Operation Role::grantRoleOp(const Role & role, const GrantRoleParams & params)
{
    return prepareOperation([role, params](Attributes & attrs)
    {
        attrs.granted_roles[role.getID()].admin_option |= params.with_admin_option;
    });
}


bool Role::revokeRole(const Role & role, const RevokeRoleParams & params)
{
    bool revoked;
    revokeRoleOp(role, params, &revoked).execute();
    return revoked;
}


Operation Role::revokeRoleOp(const Role & role, const RevokeRoleParams & params, bool * revoked)
{
    return prepareOperation([role, params, revoked](Attributes & attrs)
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
    });
}


std::vector<Role> Role::getGrantedRoles() const
{
    auto [attrs, manager] = getAttributesWithManagerStrict();
    std::vector<Role> result;
    result.reserve(attrs->granted_roles.size());
    for (const auto & granted_id_with_settings : attrs->granted_roles)
    {
        const auto & granted_id = granted_id_with_settings.first;
        result.push_back({granted_id, *manager});
    }
    return result;
}


std::vector<Role> Role::getGrantedRolesWithAdminOption() const
{
    auto [attrs, manager] = getAttributesWithManagerStrict();
    std::vector<Role> result;
    result.reserve(attrs->granted_roles.size());
    for (const auto & [granted_id, settings] : attrs->granted_roles)
    {
        if (settings.admin_option)
            result.push_back({granted_id, *manager});
    }
    return result;
}


ACLAttributesType Role::getType() const
{
    return ACLAttributesType::ROLE;
}
}
