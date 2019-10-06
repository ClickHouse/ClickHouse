#pragma once

#include <Access/IAttributes.h>
#include <Access/AccessPrivileges.h>
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
    AccessPrivileges privileges[2 /* Grant option: false - without grant option, true - with grant option */];
    std::unordered_set<UUID> granted_roles[2 /* Admin option: false - without admin option, true - with admin option */];

    static const Type TYPE;
    const Type & getType() const override { return TYPE; }
    std::shared_ptr<IAttributes> clone() const override { return cloneImpl<Role>(); }
    bool equal(const IAttributes & other) const override;
    bool hasReferences(const UUID & id) const override;
    void removeReferences(const UUID & id) override;
};

using RolePtr = std::shared_ptr<const Role>;
}
