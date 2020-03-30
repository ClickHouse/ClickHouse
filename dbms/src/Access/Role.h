#pragma once

#include <Access/IAccessEntity.h>
#include <Access/AccessRights.h>
#include <Access/SettingsProfileElement.h>
#include <Core/UUID.h>
#include <boost/container/flat_set.hpp>


namespace DB
{

struct Role : public IAccessEntity
{
    AccessRights access;
    AccessRights access_with_grant_option;
    boost::container::flat_set<UUID> granted_roles;
    boost::container::flat_set<UUID> granted_roles_with_admin_option;
    SettingsProfileElements settings;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<Role>(); }
};

using RolePtr = std::shared_ptr<const Role>;
}
