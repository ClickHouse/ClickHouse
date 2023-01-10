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

void SettingsProfile::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    elements.replaceDependencies(old_to_new_ids);
    to_roles.replaceDependencies(old_to_new_ids);
}

}
