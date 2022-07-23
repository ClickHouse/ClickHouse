#pragma once

namespace DB
{

struct KeeperContext
{
    enum class Phase : uint8_t
    {
        INIT,
        RUNNING
    };

    Phase server_state{Phase::INIT};
    bool digest_enabled{true};
};

using KeeperContextPtr = std::shared_ptr<KeeperContext>;

}
