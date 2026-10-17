#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

class StorageReplicatedMergeTree;

// Attach table to the existing data.
// Initialize the table by creating all the necessary nodes and do the required checks.
// Initialization is repeated if an operation fails because of a ZK request or connection loss.
class ReplicatedMergeTreeAttachThread
{
public:
    explicit ReplicatedMergeTreeAttachThread(StorageReplicatedMergeTree & storage_);

    ~ReplicatedMergeTreeAttachThread();

    void start();

    void shutdown();

    void waitFirstTry() { first_try_done.wait(false); }

    void setSkipSanityChecks(bool skip_sanity_checks_);

    void setRestoreMissingMetadata(bool restore_missing_metadata_);

    static void checkHasReplicaMetadataInZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const String & replica_path);

private:
    StorageReplicatedMergeTree & storage;
    BackgroundSchedulePoolTaskHolder task;

    std::string log_name;
    LoggerPtr log;

    std::atomic<bool> first_try_done{false};

    std::atomic<bool> shutdown_called{false};

    UInt64 retry_period;

    bool skip_sanity_checks{false};
    /// With `skip_sanity_checks`, the metadata missing in ZooKeeper is restored.
    bool restore_missing_metadata{true};

    void run();
    void runImpl();

    void finalizeInitialization();
};

}
