#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperConstants.h>
#include <Core/SettingsEnums.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <Core/SettingsFields.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

DECLARE_SETTING_ENUM(KeeperApiVersion);
IMPLEMENT_SETTING_ENUM(KeeperApiVersion, ErrorCodes::BAD_ARGUMENTS,
    {{"ZOOKEEPER_COMPATIBLE", KeeperApiVersion::ZOOKEEPER_COMPATIBLE},
     {"WITH_FILTERED_LIST", KeeperApiVersion::WITH_FILTERED_LIST},
     {"WITH_MULTI_READ", KeeperApiVersion::WITH_MULTI_READ},
     {"WITH_CHECK_NOT_EXISTS", KeeperApiVersion::WITH_CHECK_NOT_EXISTS}});

KeeperContext::KeeperContext()
{
    for (const auto & [path, data] : child_system_paths_with_data)
        system_nodes_with_data[std::string{path}] = data;
}

void KeeperContext::initialize(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("keeper_server.api_version"))
    {
        auto version_string = config.getString("keeper_server.api_version");
        auto api_version = SettingFieldKeeperApiVersionTraits::fromString(version_string);
        LOG_INFO(&Poco::Logger::get("KeeperContext"), "API version override used: {}", version_string);
        system_nodes_with_data[keeper_api_version_path] = toString(static_cast<uint8_t>(api_version));
    }
}

}
