#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Coordination/KeeperFeatureFlags.h>

namespace DB
{

struct KeeperContext
{
    KeeperContext();

    void initialize(const Poco::Util::AbstractConfiguration & config);

    enum class Phase : uint8_t
    {
        INIT,
        RUNNING,
        SHUTDOWN
    };

    Phase server_state{Phase::INIT};

    bool ignore_system_path_on_startup{false};
    bool digest_enabled{true};

    std::unordered_map<std::string, std::string> system_nodes_with_data;

    KeeperFeatureFlags feature_flags;
};

using KeeperContextPtr = std::shared_ptr<KeeperContext>;

}
