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

void Role::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    granted_roles.replaceDependencies(old_to_new_ids);
    settings.replaceDependencies(old_to_new_ids);
}

}
