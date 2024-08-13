#include "ServerSettings.h"
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

IMPLEMENT_SETTINGS_TRAITS(ServerSettingsTraits, SERVER_SETTINGS)

void ServerSettings::loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    // settings which can be loaded from the the default profile, see also MAKE_DEPRECATED_BY_SERVER_CONFIG in src/Core/Settings.h
    std::unordered_set<std::string> settings_from_profile_allowlist = {
        "background_pool_size",
        "background_merges_mutations_concurrency_ratio",
        "background_merges_mutations_scheduling_policy",
        "background_move_pool_size",
        "background_fetches_pool_size",
        "background_common_pool_size",
        "background_buffer_flush_schedule_pool_size",
        "background_schedule_pool_size",
        "background_message_broker_schedule_pool_size",
        "background_distributed_schedule_pool_size",

        "max_remote_read_network_bandwidth_for_server",
        "max_remote_write_network_bandwidth_for_server",
    };

    for (auto setting : all())
    {
        const auto & name = setting.getName();
        if (config.has(name))
            set(name, config.getString(name));
        else if (settings_from_profile_allowlist.contains(name) && config.has("profiles.default." + name))
            set(name, config.getString("profiles.default." + name));
    }
}

}
