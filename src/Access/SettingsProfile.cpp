#include <Access/SettingsProfile.h>


namespace DB
{

bool SettingsProfile::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_profile = typeid_cast<const SettingsProfile &>(other);
    return (elements == other_profile.elements) && (to_roles == other_profile.to_roles);
}

}
