#include <Access/SettingsProfile.h>
#include <base/insertAtEnd.h>


namespace DB
{

bool SettingsProfile::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_profile = typeid_cast<const SettingsProfile &>(other);
    return (elements == other_profile.elements) && (to_roles == other_profile.to_roles);
}

std::vector<UUID> SettingsProfile::findDependencies() const
{
    std::vector<UUID> res;
    insertAtEnd(res, elements.findDependencies());
    insertAtEnd(res, to_roles.findDependencies());
    return res;
}

bool SettingsProfile::hasDependencies(const std::unordered_set<UUID> & ids) const
{
    return elements.hasDependencies(ids) || to_roles.hasDependencies(ids);
}

void SettingsProfile::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    elements.replaceDependencies(old_to_new_ids);
    to_roles.replaceDependencies(old_to_new_ids);
}

void SettingsProfile::copyDependenciesFrom(const IAccessEntity & src, const std::unordered_set<UUID> & ids)
{
    if (getType() != src.getType())
        return;
    const auto & src_profile = typeid_cast<const SettingsProfile &>(src);
    elements.copyDependenciesFrom(src_profile.elements, ids);
    to_roles.copyDependenciesFrom(src_profile.to_roles, ids);
}

void SettingsProfile::removeDependencies(const std::unordered_set<UUID> & ids)
{
    elements.removeDependencies(ids);
    to_roles.removeDependencies(ids);
}

void SettingsProfile::clearAllExceptDependencies()
{
    elements.removeSettingsKeepProfiles();
}

}
