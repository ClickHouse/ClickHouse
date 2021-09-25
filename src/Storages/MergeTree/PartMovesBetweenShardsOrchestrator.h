#pragma once

#include <vector>
#include <common/logger_useful.h>
#include <common/types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/UUID.h>
#include <Core/BackgroundSchedulePool.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class StorageReplicatedMergeTree;

/// Cross shard part movement workflow orchestration.
class PartMovesBetweenShardsOrchestrator
{
public:
    struct EntryState
    {
        enum Value
        {
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
            CANCELLED,
        };

        EntryState(): value(TODO) {}
        EntryState(Value value_): value(value_) {}

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

        EntryState state;

        String last_exception_msg;

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
    static constexpr auto JSON_KEY_STATE = "state";
    static constexpr auto JSON_KEY_LAST_EX_MSG = "last_exception";

public:
    PartMovesBetweenShardsOrchestrator(StorageReplicatedMergeTree & storage_);

    void start() { task->activateAndSchedule(); }
    void wakeup() { task->schedule(); }
    void shutdown();

    void fetchStateFromZK();

    /// We could have one thread per Entry and worry about concurrency issues.
    /// Or we could have a single thread trying to run one step at a time.
    bool step();

    std::vector<Entry> getEntries() const;

private:
    void run();
    void stepEntry(const Entry & entry, zkutil::ZooKeeperPtr zk);

private:
    StorageReplicatedMergeTree & storage;

    String zookeeper_path;
    String logger_name;
    Poco::Logger * log = nullptr;
    std::atomic<bool> need_stop{false};

    BackgroundSchedulePool::TaskHolder task;

    mutable std::mutex state_mutex;
    std::map<String, Entry> entries;

public:
    String entries_znode_path;
};

}
