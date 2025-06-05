#pragma once

#include <Access/IAccessEntity.h>
#include <Access/AccessRights.h>
#include <Access/GrantedRoles.h>
#include <Access/SettingsProfileElement.h>


namespace DB
{

struct Role : public IAccessEntity
{
    AccessRights access;
    GrantedRoles granted_roles;
    SettingsProfileElements settings;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<Role>(); }
    static constexpr const auto TYPE = AccessEntityType::ROLE;
    AccessEntityType getType() const override { return TYPE; }

    std::vector<UUID> findDependencies() const override;
    void doReplaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids) override;
    bool isBackupAllowed() const override { return settings.isBackupAllowed(); }
};

using RolePtr = std::shared_ptr<const Role>;
}
