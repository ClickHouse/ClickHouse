#pragma once

#include <ACL/IAttributes.h>
#include <ACL/AllowedDatabases.h>
#include <unordered_set>


namespace DB
{
/// Represents a role in Role-based Access Control.
/// Syntax:
/// CREATE ROLE [IF NOT EXISTS] name
///
/// DROP ROLE [IF EXISTS] name
struct Role : public IAttributes
{
    AllowedDatabases allowed_databases_by_grant_option[2 /* 0 - without grant option, 1 - with grant option */];
    std::unordered_set<UUID> granted_roles_by_admin_option[2 /* 0 - without admin option, 1 - with admin option */];

    static const Type TYPE;
    const Type & getType() const override { return TYPE; }
    std::shared_ptr<IAttributes> clone() const override { return cloneImpl<Role>(); }
    bool equal(const IAttributes & other) const override;
    bool hasReferences(const UUID & id) const override;
    void removeReferences(const UUID & id) override;
};

using RolePtr = std::shared_ptr<const Role>;
}
