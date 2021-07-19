#include <Access/SettingsProfilesInfo.h>
#include <Access/SettingsConstraints.h>


namespace DB
{

SettingsProfilesInfo::SettingsProfilesInfo() : SettingsProfilesInfo({}, {}, {}, {}, {})
{
}

SettingsProfilesInfo::SettingsProfilesInfo(
    SettingsChanges settings_,
    SettingsConstraints constraints_,
    std::vector<UUID> profiles_,
    std::vector<UUID> profiles_including_implicit_,
    std::unordered_map<UUID, String> names_of_profiles_)
    : settings(std::move(settings_))
    , constraints(std::make_shared<SettingsConstraints>(std::move(constraints_)))
    , profiles(std::move(profiles_))
    , profiles_including_implicit(std::move(profiles_including_implicit_))
    , names_of_profiles(std::move(names_of_profiles_))
{
}

SettingsProfilesInfo::~SettingsProfilesInfo() = default;

bool operator==(const SettingsProfilesInfo & lhs, const SettingsProfilesInfo & rhs)
{
    if (lhs.settings != rhs.settings)
        return false;

    if (static_cast<bool>(lhs.constraints) != static_cast<bool>(rhs.constraints))
        return false;
    if (lhs.constraints && (*lhs.constraints != *rhs.constraints))
        return false;

    if (lhs.profiles != rhs.profiles)
        return false;

    if (lhs.profiles_including_implicit != rhs.profiles_including_implicit)
        return false;

    if (lhs.names_of_profiles != rhs.names_of_profiles)
        return false;

    return true;
}

}
