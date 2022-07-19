#include <Access/EnabledSettings.h>
#include <Common/SettingsChanges.h>


namespace DB
{

EnabledSettings::EnabledSettings(const Params & params_) : params(params_)
{
}

EnabledSettings::~EnabledSettings() = default;

std::shared_ptr<const SettingsProfilesInfo> EnabledSettings::getInfo() const
{
    std::lock_guard lock{mutex};
    return info;
}

void EnabledSettings::setInfo(const std::shared_ptr<const SettingsProfilesInfo> & info_)
{
    std::lock_guard lock{mutex};
    info = info_;
}

}
