#pragma once

#include <vector>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/UUID.h>
#include <Core/BackgroundSchedulePool.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/CancellationCode.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class StorageReplicatedMergeTree;

/**
 * Cross shard part movement workflow orchestration.
 *
 * TODO(nv):
 *  * Usage of `format_version` when acting on the behalf of the remote shard.
 *      There needs to be sort of an API to coordinate with remote replicas.
 *  * Only one movement at a time can be coordinated. This can easily be fixed
 *      by cycling through different tasks and checking their status with a
 *      priority queue and back-off for failing tasks
 *      `min(backoff * num_tries, max_backoff)`.
 */
class PartMovesBetweenShardsOrchestrator
{
public:
    struct EntryState
    {
        // State transitions are linear. When a kill query is issued a rollback
        // flag is set and transitions order is reversed.
        //
        // SOURCE_DROP is a critical state after which rollback is not possible
        // and we must ensure that the task can always succeed after that.
        //
        // Similar for rollback. It should be always possible to rollback before
        // SOURCE_DROP state and it should terminate.
        //
        // Note: This fragile. If you change the states please add entry to
        // changelog about forward/backward compatibility. Better not to have
        // any active move tasks while doing upgrade/downgrade operations.
        enum Value
        {
            CANCELLED,
            TODO,
            SYNC_SOURCE,
            SYNC_DESTINATION,
            DESTINATION_FETCH,
            DESTINATION_ATTACH,
            SOURCE_DROP_PRE_DELAY,
            SOURCE_DROP,
            SOURCE_DROP_POST_DELAY,
            REMOVE_UUID_PIN,
            DONE,
        };

        EntryState(): value(TODO) {}
        EntryState(Value value_): value(value_) {} /// NOLINT

        Value value;

        String toString() const
        {
            switch (value)
            {
                case TODO: return "TODO";
                case SYNC_SOURCE: return "SYNC_SOURCE";
                case SYNC_DESTINATION: return "SYNC_DESTINATION";
                case DESTINATION_FETCH: return "DESTINATION_FETCH";
                case DESTINATION_ATTACH: return "DESTINATION_ATTACH";
                case SOURCE_DROP_PRE_DELAY: return "SOURCE_DROP_PRE_DELAY";
                case SOURCE_DROP: return "SOURCE_DROP";
                case SOURCE_DROP_POST_DELAY: return "SOURCE_DROP_POST_DELAY";
                case REMOVE_UUID_PIN: return "REMOVE_UUID_PIN";
                case DONE: return "DONE";
                case CANCELLED: return "CANCELLED";
            }

            throw Exception("Unknown EntryState: " + DB::toString<int>(value), ErrorCodes::LOGICAL_ERROR);
        }

        static EntryState::Value fromString(String in)
        {
            if (in == "TODO") return TODO;
            else if (in == "SYNC_SOURCE") return SYNC_SOURCE;
            else if (in == "SYNC_DESTINATION") return SYNC_DESTINATION;
            else if (in == "DESTINATION_FETCH") return DESTINATION_FETCH;
            else if (in == "DESTINATION_ATTACH") return DESTINATION_ATTACH;
            else if (in == "SOURCE_DROP_PRE_DELAY") return SOURCE_DROP_PRE_DELAY;
            else if (in == "SOURCE_DROP") return SOURCE_DROP;
            else if (in == "SOURCE_DROP_POST_DELAY") return SOURCE_DROP_POST_DELAY;
            else if (in == "REMOVE_UUID_PIN") return REMOVE_UUID_PIN;
            else if (in == "DONE") return DONE;
            else if (in == "CANCELLED") return CANCELLED;
            else throw Exception("Unknown state: " + in, ErrorCodes::LOGICAL_ERROR);
        }
    };

    struct Entry
    {
        friend class PartMovesBetweenShardsOrchestrator;

        time_t create_time = 0;
        time_t update_time = 0;

        /// Globally unique identifier used for attaching parts on destination.
        /// Using `part_uuid` results in part names being reused when moving parts back and forth.
        UUID task_uuid;

        String part_name;
        UUID part_uuid;
        String to_shard;
        String dst_part_name;

        EntryState state;
        bool rollback = false;

        /// Reset on successful transitions.
        String last_exception_msg;
        UInt64 num_tries = 0;

        String znode_name;

    private:
        /// Transient value for CAS.
        uint32_t version = 0;

        String znode_path;

    public:
        String toString() const;
        void fromString(const String & buf);
    };

private:
    static constexpr auto JSON_KEY_CREATE_TIME = "create_time";
    static constexpr auto JSON_KEY_UPDATE_TIME = "update_time";
    static constexpr auto JSON_KEY_TASK_UUID = "task_uuid";
    static constexpr auto JSON_KEY_PART_NAME = "part_name";
    static constexpr auto JSON_KEY_PART_UUID = "part_uuid";
    static constexpr auto JSON_KEY_TO_SHARD = "to_shard";
    static constexpr auto JSON_KEY_DST_PART_NAME = "dst_part_name";
    static constexpr auto JSON_KEY_STATE = "state";
    static constexpr auto JSON_KEY_ROLLBACK = "rollback";
    static constexpr auto JSON_KEY_LAST_EX_MSG = "last_exception";
    static constexpr auto JSON_KEY_NUM_TRIES = "num_tries";

public:
    explicit PartMovesBetweenShardsOrchestrator(StorageReplicatedMergeTree & storage_);

    void start() { task->activateAndSchedule(); }
    void wakeup() { task->schedule(); }
    void shutdown();

    CancellationCode killPartMoveToShard(const UUID & task_uuid);

    std::vector<Entry> getEntries();

private:
    void run();
    bool step();
    Entry stepEntry(Entry entry, zkutil::ZooKeeperPtr zk);

    Entry getEntryByUUID(const UUID & task_uuid);
    void removePins(const Entry & entry, zkutil::ZooKeeperPtr zk);
    void syncStateFromZK();

    StorageReplicatedMergeTree & storage;

    String zookeeper_path;
    String logger_name;
    Poco::Logger * log = nullptr;
    std::atomic<bool> need_stop{false};

    BackgroundSchedulePool::TaskHolder task;

    mutable std::mutex state_mutex;
    std::vector<Entry> entries;

public:
    String entries_znode_path;
};

}
