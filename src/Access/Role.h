#pragma once

#include <Access/IAccessEntity.h>
#include <Access/GrantedAccess.h>
#include <Access/GrantedRoles.h>
#include <Access/SettingsProfileElement.h>


namespace DB
{

struct Role : public IAccessEntity
{
    GrantedAccess access;
    GrantedRoles granted_roles;
    SettingsProfileElements settings;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<Role>(); }
    static constexpr const Type TYPE = Type::ROLE;
    Type getType() const override { return TYPE; }
};

using RolePtr = std::shared_ptr<const Role>;
}
