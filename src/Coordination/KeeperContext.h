#pragma once

namespace DB
{

struct KeeperContext
{
    enum class Phase : uint8_t
    {
        INIT,
        RUNNING,
        SHUTDOWN
    };

    Phase server_state{Phase::INIT};

    bool ignore_system_path_on_startup{false};
    bool digest_enabled{true};
};

using KeeperContextPtr = std::shared_ptr<KeeperContext>;

}
