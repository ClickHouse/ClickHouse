#pragma once

#include <ACL/IAccessAttributes.h>
#include <ACL/Privileges.h>
#include <Core/UUID.h>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

/// Attributes of a role.
struct RoleAttributes : public IAccessAttributes
{
    /// Granted privileges. This doesn't include the privileges from the granted roles.
    Privileges privileges;
    Privileges grant_options;

    struct GrantedRoleParams
    {
        bool with_admin_option = false;
        bool enabled_by_default = false;

        bool operator==(const GrantedRoleParams & other) const { return (with_admin_option == other.with_admin_option) && (enabled_by_default == other.enabled_by_default); }
        bool operator!=(const GrantedRoleParams & other) const { return !(*this == other); }
    };

    /// Granted roles.
    std::unordered_map<UUID, GrantedRoleParams> granted_roles;

    std::shared_ptr<IAccessAttributes> clone() const override;
    bool isEqual(const IAccessAttributes & other) const override;

    RoleAttributes() : RoleAttributes(Type::ROLE) {}
protected:
    RoleAttributes(Type type_) : IAccessAttributes(type_) {}
    void copyTo(RoleAttributes & dest) const;
};

using RoleAttributesPtr = std::shared_ptr<const RoleAttributes>;
}
