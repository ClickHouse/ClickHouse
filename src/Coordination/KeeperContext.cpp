#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperConstants.h>
#include <Common/logger_useful.h>
#include <Coordination/KeeperFeatureFlags.h>
#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{

extern const int BAD_ARGUMENTS;

}

KeeperContext::KeeperContext()
{
    /// enable by default some feature flags
    feature_flags.enableFeatureFlag(KeeperFeatureFlag::FILTERED_LIST);
    feature_flags.enableFeatureFlag(KeeperFeatureFlag::MULTI_READ);
    system_nodes_with_data[keeper_api_feature_flags_path] = feature_flags.getFeatureFlags();


    /// for older clients, the default is equivalent to WITH_MULTI_READ version
    system_nodes_with_data[keeper_api_version_path] = toString(static_cast<uint8_t>(KeeperApiVersion::WITH_MULTI_READ));
}

void KeeperContext::initialize(const Poco::Util::AbstractConfiguration & config)
{
    digest_enabled = config.getBool("keeper_server.digest_enabled", false);
    ignore_system_path_on_startup = config.getBool("keeper_server.ignore_system_path_on_startup", false);

    static const std::string feature_flags_key = "keeper_server.feature_flags";
    if (config.has(feature_flags_key))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(feature_flags_key, keys);
        for (const auto & key : keys)
        {
            auto feature_flag_string = boost::to_upper_copy(key);
            auto feature_flag = magic_enum::enum_cast<KeeperFeatureFlag>(feature_flag_string);

            if (!feature_flag.has_value())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid feature flag defined in config for Keeper: {}", key);

            auto is_enabled = config.getBool(feature_flags_key + "." + key);
            if (is_enabled)
                feature_flags.enableFeatureFlag(feature_flag.value());
            else
                feature_flags.disableFeatureFlag(feature_flag.value());
        }

        system_nodes_with_data[keeper_api_feature_flags_path] = feature_flags.getFeatureFlags();
    }

    feature_flags.logFlags(&Poco::Logger::get("KeeperContext"));
}

}
