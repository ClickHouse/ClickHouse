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
};

using RolePtr = std::shared_ptr<const Role>;
}
