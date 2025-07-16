#pragma once

#include <Core/Types.h>

namespace DB
{

/** For the system table database replicas. */
struct ReplicatedDatabaseStatus
{
    bool is_readonly;
    bool is_session_expired;

    String replica_path;
    UInt32 max_log_ptr;
    UInt32 total_replicas;
    String zookeeper_exception;
};

}
