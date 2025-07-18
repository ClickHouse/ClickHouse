#pragma once

#include <Core/Types.h>

namespace DB
{

/** For the system table database replicas. */
struct ReplicatedDatabaseStatus
{
    bool is_readonly;
    bool is_session_expired;
    UInt32 max_log_ptr;
    String replica_name;
    String replica_path;
    String zookeeper_path;
    String shard_name;
    UInt32 log_ptr;
    UInt32 total_replicas;
    String zookeeper_exception;
};

}
