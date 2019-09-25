#if 0
#include <ACL/Role.h>
//#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ROLE_NOT_FOUND;
    extern const int ROLE_ALREADY_EXISTS;
}


namespace ControlAttributesNames
{
    extern const size_t ROLE_NAMESPACE_IDX;
}


const Role::Type Role::Attributes::TYPE = {"Role",
                                           &createImpl<Attributes>,
                                           nullptr,
                                           ErrorCodes::ROLE_NOT_FOUND,
                                           ErrorCodes::ROLE_ALREADY_EXISTS,
                                           ControlAttributesNames::ROLE_NAMESPACE_IDX};

const Role::Type & Role::TYPE = Role::Attributes::TYPE;


bool Role::Attributes::equal(const IAttributes & other) const
{
    if (!IAttributes::equal(other))
        return false;
    return true;
    //const auto * o = dynamic_cast<const Attributes *>(&other);
    //return o && (privileges == o->privileges) && (grant_options == o->grant_options) && (granted_roles == o->granted_roles);
}


#if 0
ACLAttributesType Role::Attributes::getType() const
{
    return ACLAttributesType::ROLE;
}


bool Role::Attributes::equal(const IAttributes & other) const
{
    if (!ACLAttributable::Attributes::equal(other))
        return false;
    const auto * o = dynamic_cast<const Attributes *>(&other);
    return o && (privileges == o->privileges) && (grant_options == o->grant_options) && (granted_roles == o->granted_roles);
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


std::pair<Role::AttributesPtr, IControlAttributesDrivenManager *> Role::getAttributesWithManagerStrict() const
{
    auto [attrs, manager] = ACLAttributable::getAttributesWithManagerStrict();
    return {std::static_pointer_cast<const Attributes>(attrs), manager};
}


void Role::grant(Privileges::Types access, const GrantParams & params)
{
    grantChanges(access, params).apply();
}


void Role::grant(Privileges::Types access, const String & database, const GrantParams & params)
{
    grantChanges(access, database, params).apply();
}


void Role::grant(Privileges::Types access, const String & database, const String & table, const GrantParams & params)
{
    grantChanges(access, database, table, params).apply();
}


void Role::grant(
    Privileges::Types access, const String & database, const String & table, const String & column, const GrantParams & params)
{
    grantChanges(access, database, table, column, params).apply();
}


void Role::grant(
    Privileges::Types access, const String & database, const String & table, const Strings & columns, const GrantParams & params)
{
    grantChanges(access, database, table, columns, params).apply();
}


Role::Changes Role::grantChanges(Privileges::Types access, const GrantParams & params)
{
    return prepareChanges([access, params](Attributes & attrs)
    {
        attrs.privileges.grant(access);
        if (params.with_grant_option)
            attrs.grant_options.grant(access);
    });
}


Role::Changes Role::grantChanges(Privileges::Types access, const String & database, const GrantParams & params)
{
    return prepareChanges([access, database, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database);
    });
}


Role::Changes Role::grantChanges(Privileges::Types access, const String & database, const String & table, const GrantParams & params)
{
    return prepareChanges([access, database, table, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database, table);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database, table);
    });
}


Role::Changes Role::grantChanges(Privileges::Types access, const String & database, const String & table, const String & column, const GrantParams & params)
{
    return prepareChanges([access, database, table, column, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database, table, column);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database, table, column);
    });
}


Role::Changes Role::grantChanges(Privileges::Types access, const String & database, const String & table, const Strings & columns, const GrantParams & params)
{
    return prepareChanges([access, database, table, columns, params](Attributes & attrs)
    {
        attrs.privileges.grant(access, database, table, columns);
        if (params.with_grant_option)
            attrs.grant_options.grant(access, database, table, columns);
    });
}


bool Role::revoke(Privileges::Types access, const RevokeParams & params)
{
    bool revoked;
    revokeChanges(access, params, &revoked).apply();
    return revoked;
}


bool Role::revoke(Privileges::Types access, const String & database, const RevokeParams & params)
{
    bool revoked;
    revokeChanges(access, database, params, &revoked).apply();
    return revoked;
}


bool Role::revoke(Privileges::Types access, const String & database, const String & table, const RevokeParams & params)
{
    bool revoked;
    revokeChanges(access, database, table, params, &revoked).apply();
    return revoked;
}


bool Role::revoke(
    Privileges::Types access, const String & database, const String & table, const String & column, const RevokeParams & params)
{
    bool revoked;
    revokeChanges(access, database, table, column, params, &revoked).apply();
    return revoked;
}


bool Role::revoke(
    Privileges::Types access, const String & database, const String & table, const Strings & columns, const RevokeParams & params)
{
    bool revoked;
    revokeChanges(access, database, table, columns, params, &revoked).apply();
    return revoked;
}


Role::Changes Role::revokeChanges(Privileges::Types access, const RevokeParams & params, bool * revoked)
{
    return prepareChanges([access, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access);
        if (revoked)
            *revoked = r;
    });
}


Role::Changes Role::revokeChanges(Privileges::Types access, const String & database, const RevokeParams & params, bool * revoked)
{
    return prepareChanges([access, database, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database);
        if (revoked)
            *revoked = r;
    });
}


Role::Changes Role::revokeChanges(Privileges::Types access, const String & database, const String & table, const RevokeParams & params, bool * revoked)
{
    return prepareChanges([access, database, table, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database, table);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database, table);
        if (revoked)
            *revoked = r;
    });
}


Role::Changes Role::revokeChanges(Privileges::Types access, const String & database, const String & table, const String & column, const RevokeParams & params, bool * revoked)
{
    return prepareChanges([access, database, table, column, params, revoked](Attributes & attrs)
    {
        bool r = attrs.grant_options.revoke(access, database, table, column);
        if (!params.only_grant_option)
            r |= attrs.privileges.revoke(access, database, table, column);
        if (revoked)
            *revoked = r;
    });
}


Role::Changes Role::revokeChanges(Privileges::Types access, const String & database, const String & table, const Strings & columns, const RevokeParams & params, bool * revoked)
{
    return prepareChanges([access, database, table, columns, params, revoked](Attributes & attrs)
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
    grantRoleChanges(role, params).apply();
}


Role::Changes Role::grantRoleChanges(const Role & role, const GrantRoleParams & params)
{
    return prepareChanges([role, params](Attributes & attrs)
    {
        attrs.granted_roles[role.getID()].admin_option |= params.with_admin_option;
    });
}


bool Role::revokeRole(const Role & role, const RevokeRoleParams & params)
{
    bool revoked;
    revokeRoleChanges(role, params, &revoked).apply();
    return revoked;
}


Role::Changes Role::revokeRoleChanges(const Role & role, const RevokeRoleParams & params, bool * revoked)
{
    return prepareChanges([role, params, revoked](Attributes & attrs)
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
#endif
}
#endif
