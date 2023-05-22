#include <Core/Settings.h>

namespace DB
{

IMPLEMENT_SETTINGS_TRAITS(SettingsTraits, LIST_OF_SETTINGS)

std::vector<String> Settings::getAllRegisteredNames() const
{
    std::vector<String> all_settings;
    for (const auto & setting_field : all())
    {
        all_settings.push_back(setting_field.getName());
    }
    return all_settings;
}

void Settings::set(std::string_view name, const Field & value)
{
    BaseSettings::set(name, value);
}


}
