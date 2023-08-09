#pragma once

#include <string>

#include <base/types.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Keeper server related information for different 4lw commands
struct Keeper4LWInfo
{
    bool is_leader;
    bool is_observer;
    bool is_follower;
    bool is_standalone;

    bool has_leader;

    uint64_t alive_connections_count;
    uint64_t outstanding_requests_count;

    uint64_t follower_count;
    uint64_t synced_follower_count;

    uint64_t total_nodes_count;
    int64_t last_zxid;

    String getRole() const
    {
        if (is_standalone)
            return "standalone";
        if (is_leader)
            return "leader";
        if (is_observer)
            return "observer";
        if (is_follower)
            return "follower";

        throw Exception(ErrorCodes::LOGICAL_ERROR, "RAFT server has undefined state, it's a bug");
    }
};

}
