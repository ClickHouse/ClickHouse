#include <Access/EnabledSettings.h>
#include <Common/SettingsChanges.h>


namespace DB
{

EnabledSettings::EnabledSettings(const Params & params_) : params(params_)
{
}

EnabledSettings::~EnabledSettings() = default;


std::shared_ptr<const Settings> EnabledSettings::getSettings() const
{
    std::lock_guard lock{mutex};
    return settings;
}


std::shared_ptr<const SettingsConstraints> EnabledSettings::getConstraints() const
{
    std::lock_guard lock{mutex};
    return constraints;
}

std::vector<UUID> EnabledSettings::getCurrentProfiles() const
{
    return current_profiles;
}


void EnabledSettings::setSettingsAndConstraintsAndProfiles(
    const std::shared_ptr<const Settings> & settings_,
    const std::shared_ptr<const SettingsConstraints> & constraints_,
    std::vector<UUID> current_profiles_)
{
    std::lock_guard lock{mutex};
    settings = settings_;
    constraints = constraints_;
    current_profiles.swap(current_profiles_);
}

}
