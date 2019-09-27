#pragma once

#include <ACL/Role.h>


namespace DB
{
/// Represents an user in Role-based Access Control.
struct User2 : public Role
{
    //PasswordHash password_hash;
    //AllowedHosts allowed_hosts;

    //std::unordered_set<UUID> default_roles;
    //bool default_all_roles = false;

    //SettingsChanges settings;

    static const Type TYPE;
    const Type & getType() const override { return TYPE; }
    std::shared_ptr<IAttributes> clone() const override { return cloneImpl<User2>(); }
    bool equal(const IAttributes & other) const override;
    bool hasReferences(const UUID & id) const override;
    void removeReferences(const UUID & id) override;
};

using User2Ptr = std::shared_ptr<const User2>;
}
