#include "ServerSettings.h"
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

IMPLEMENT_SETTINGS_TRAITS(ServerSettingsTraits, SERVER_SETTINGS)

void ServerSettings::loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    for (auto setting : all())
    {
        const auto & name = setting.getName();
        if (config.has(name))
            set(name, config.getString(name));
    }
}

}
