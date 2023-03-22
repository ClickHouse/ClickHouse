#include <Access/SettingsProfilesInfo.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <base/removeDuplicates.h>


namespace DB
{

bool operator==(const SettingsProfilesInfo & lhs, const SettingsProfilesInfo & rhs)
{
    if (lhs.settings != rhs.settings)
        return false;

    if (lhs.constraints != rhs.constraints)
        return false;

    if (lhs.profiles != rhs.profiles)
        return false;

    if (lhs.profiles_with_implicit != rhs.profiles_with_implicit)
        return false;

    if (lhs.names_of_profiles != rhs.names_of_profiles)
        return false;

    return true;
}

std::shared_ptr<const SettingsConstraintsAndProfileIDs>
SettingsProfilesInfo::getConstraintsAndProfileIDs(const std::shared_ptr<const SettingsConstraintsAndProfileIDs> & previous) const
{
    auto res = std::make_shared<SettingsConstraintsAndProfileIDs>(access_control);
    res->current_profiles = profiles;

    if (previous)
    {
        res->constraints = previous->constraints;
        res->constraints.merge(constraints);
    }
    else
        res->constraints = constraints;

    if (previous)
    {
        res->enabled_profiles.reserve(previous->enabled_profiles.size() + profiles_with_implicit.size());
        res->enabled_profiles = previous->enabled_profiles;
    }
    res->enabled_profiles.insert(res->enabled_profiles.end(), profiles_with_implicit.begin(), profiles_with_implicit.end());

    /// If some profile occurs multiple times (with some other settings in between),
    /// the latest occurrence overrides all the previous ones.
    removeDuplicatesKeepLast(res->current_profiles);
    removeDuplicatesKeepLast(res->enabled_profiles);

    return res;
}

}
