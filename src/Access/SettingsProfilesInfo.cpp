#include <Access/SettingsProfilesInfo.h>
#include <Access/AccessControl.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <base/removeDuplicates.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool operator==(const SettingsProfilesInfo & lhs, const SettingsProfilesInfo & rhs)
{
    return std::tie(lhs.settings, lhs.constraints, lhs.profiles, lhs.profiles_with_implicit, lhs.names_of_profiles)
        == std::tie(rhs.settings, rhs.constraints, rhs.profiles, rhs.profiles_with_implicit, rhs.names_of_profiles);
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

Strings SettingsProfilesInfo::getProfileNames() const
{
    Strings result;
    result.reserve(profiles.size());
    for (const UUID & profile_uuid : profiles)
    {
        const auto names_it = names_of_profiles.find(profile_uuid);
        if (names_it != names_of_profiles.end())
        {
            result.push_back(names_it->second);
        }
        else
        {
            if (const auto name = access_control.tryReadName(profile_uuid))
                // We could've updated cache here, but it is a very rare case, so don't bother.
                result.push_back(*name);
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to get profile name for {}", toString(profile_uuid));
        }
    }

    return result;
}

}
