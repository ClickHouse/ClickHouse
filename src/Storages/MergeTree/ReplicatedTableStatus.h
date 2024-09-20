#pragma once

#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/TableZnodeInfo.h>
#include <Core/Types.h>

namespace DB
{

/** For the system table replicas. */
struct ReplicatedTableStatus
{
    bool is_leader;
    bool can_become_leader;
    bool is_readonly;
    bool is_session_expired;

    ReplicatedMergeTreeQueue::Status queue;
    UInt32 parts_to_check;
    TableZnodeInfo zookeeper_info;
    String replica_path;
    Int32 columns_version;
    UInt64 log_max_index;
    UInt64 log_pointer;
    UInt64 absolute_delay;
    UInt32 total_replicas;
    UInt32 active_replicas;
    UInt64 lost_part_count;
    String last_queue_update_exception;
    /// If the error has happened fetching the info from ZooKeeper, this field will be set.
    String zookeeper_exception;

    std::unordered_map<std::string, bool> replica_is_active;
};

}
