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
    bool hasDependencies(const std::unordered_set<UUID> & ids) const override;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids) override;
    void copyDependenciesFrom(const IAccessEntity & src, const std::unordered_set<UUID> & ids) override;
    void removeDependencies(const std::unordered_set<UUID> & ids) override;
    void clearAllExceptDependencies() override;

    bool isBackupAllowed() const override { return elements.isBackupAllowed(); }
};

using SettingsProfilePtr = std::shared_ptr<const SettingsProfile>;
}
