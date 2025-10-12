#pragma once

#include <Core/Types.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

namespace Coordination
{

struct WatchSnapshot
{
    String path;
    Int64 session_id{0};
    Int64 request_xid{0};
    OpNum op_num{OpNum::Error};
    time_t create_time{0};
};

}
