#include <Coordination/CoordinationSettings.h>
#include <Core/Settings.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(CoordinationSettingsTraits, LIST_OF_COORDINATION_SETTINGS)

void CoordinationSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
            set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in Coordination settings config");
        throw;
    }
}

}
