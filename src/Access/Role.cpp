#include <Access/Role.h>
#include <base/insertAtEnd.h>


namespace DB
{

bool Role::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_role = typeid_cast<const Role &>(other);
    return (access == other_role.access) && (granted_roles == other_role.granted_roles) && (settings == other_role.settings);
}

std::vector<UUID> Role::findDependencies() const
{
    std::vector<UUID> res;
    insertAtEnd(res, granted_roles.findDependencies());
    insertAtEnd(res, settings.findDependencies());
    return res;
}

bool Role::hasDependencies(const std::unordered_set<UUID> & ids) const
{
    return granted_roles.hasDependencies(ids) || settings.hasDependencies(ids);
}

void Role::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    granted_roles.replaceDependencies(old_to_new_ids);
    settings.replaceDependencies(old_to_new_ids);
}

void Role::copyDependenciesFrom(const IAccessEntity & src, const std::unordered_set<UUID> & ids)
{
    if (getType() != src.getType())
        return;
    const auto & src_role = typeid_cast<const Role &>(src);
    granted_roles.copyDependenciesFrom(src_role.granted_roles, ids);
    settings.copyDependenciesFrom(src_role.settings, ids);
}

void Role::removeDependencies(const std::unordered_set<UUID> & ids)
{
    granted_roles.removeDependencies(ids);
    settings.removeDependencies(ids);
}

void Role::clearAllExceptDependencies()
{
    access = {};
    settings.removeSettingsKeepProfiles();
}

}
