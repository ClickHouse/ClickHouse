#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <IO/WriteBufferFromString.h>
#include <Poco/DateTime.h>
#include <Common/Stopwatch.h>


/// Contains some useful interfaces which are helpful to get keeper information.
namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Keeper server related information
struct KeeperInfo
{
    bool is_leader;
    bool is_observer;
    bool is_follower;

    bool has_leader;

    uint64_t alive_connections_count;
    uint64_t outstanding_requests_count;

    uint64_t follower_count;
    uint64_t synced_follower_count;

    uint64_t total_nodes_count;
    int64_t last_zxid;

    String getRole() const
    {
        if (is_leader)
            return "leader";
        if (is_observer)
            return "observer";
        if (is_follower)
            return "follower";

        throw Exception(ErrorCodes::LOGICAL_ERROR, "RAFT server has undefined state state, it's a bug");
    }
};

}
