#pragma once

#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace DB
{

struct NuKeeperRequest
{
    int64_t session_id;
    Coordination::ZooKeeperRequestPtr request;
};

using NuKeeperRequests = std::vector<NuKeeperRequest>;

struct NuKeeperResponse
{
    int64_t session_id;
    Coordination::ZooKeeperRequestPtr response;
};

using NuKeeperResponses = std::vector<NuKeeperResponse>;

}
