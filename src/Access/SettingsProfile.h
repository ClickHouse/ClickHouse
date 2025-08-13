#pragma once

#include <Access/IAccessEntity.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/SettingsProfileElement.h>


namespace DB
{
/// Represents a settings profile created by command
/// CREATE SETTINGS PROFILE name SETTINGS x=value MIN=min MAX=max READONLY... TO roles
struct SettingsProfile : public IAccessEntity
{
    SettingsProfileElements elements;

    /// Which roles or users should use this settings profile.
    RolesOrUsersSet to_roles;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<SettingsProfile>(); }
    static constexpr const auto TYPE = AccessEntityType::SETTINGS_PROFILE;
    AccessEntityType getType() const override { return TYPE; }

    std::vector<UUID> findDependencies() const override;
    void doReplaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids) override;
    bool isBackupAllowed() const override { return elements.isBackupAllowed(); }
};

using SettingsProfilePtr = std::shared_ptr<const SettingsProfile>;
}
