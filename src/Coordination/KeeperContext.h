#pragma once
#include <Common/ZooKeeper/KeeperFeatureFlags.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <IO/WriteBufferFromString.h>
#include <base/defines.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <variant>

namespace rocksdb
{
struct Options;
}

namespace DB
{

class KeeperDispatcher;

struct CoordinationSettings;
using CoordinationSettingsPtr = std::shared_ptr<CoordinationSettings>;

class DiskSelector;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class KeeperContext
{
public:
    KeeperContext(bool standalone_keeper_, CoordinationSettingsPtr coordination_settings_);

    enum class Phase : uint8_t
    {
        INIT,
        RUNNING,
        SHUTDOWN
    };

    void initialize(const Poco::Util::AbstractConfiguration & config, KeeperDispatcher * dispatcher_);

    Phase getServerState() const;
    void setServerState(Phase server_state_);

    bool ignoreSystemPathOnStartup() const;

    bool digestEnabled() const;
    void setDigestEnabled(bool digest_enabled_);
    bool digestEnabledOnCommit() const;

    DiskPtr getLatestLogDisk() const;
    DiskPtr getLogDisk() const;
    std::vector<DiskPtr> getOldLogDisks() const;
    void setLogDisk(DiskPtr disk);

    DiskPtr getLatestSnapshotDisk() const;
    DiskPtr getSnapshotDisk() const;
    std::vector<DiskPtr> getOldSnapshotDisks() const;
    void setSnapshotDisk(DiskPtr disk);

    DiskPtr getStateFileDisk() const;
    void setStateFileDisk(DiskPtr disk);

    const std::unordered_map<std::string, std::string> & getSystemNodesWithData() const;
    const KeeperFeatureFlags & getFeatureFlags() const;

    void dumpConfiguration(WriteBufferFromOwnString & buf) const;

    constexpr KeeperDispatcher * getDispatcher() const { return dispatcher; }

    void setRocksDBDisk(DiskPtr disk);
    DiskPtr getTemporaryRocksDBDisk() const;

    void setRocksDBOptions(std::shared_ptr<rocksdb::Options> rocksdb_options_ = nullptr);
    std::shared_ptr<rocksdb::Options> getRocksDBOptions() const { return rocksdb_options; }

    UInt64 getKeeperMemorySoftLimit() const { return memory_soft_limit; }
    void updateKeeperMemorySoftLimit(const Poco::Util::AbstractConfiguration & config);

    bool setShutdownCalled();
    const auto & isShutdownCalled() const
    {
        return shutdown_called;
    }

    void setLocalLogsPreprocessed();
    bool localLogsPreprocessed() const;

    void waitLocalLogsPreprocessedOrShutdown();

    uint64_t lastCommittedIndex() const;
    void setLastCommitIndex(uint64_t commit_index);
    /// returns true if the log is committed, false if timeout happened
    bool waitCommittedUpto(uint64_t log_idx, uint64_t wait_timeout_ms);

    const CoordinationSettings & getCoordinationSettings() const;

    int64_t getPrecommitSleepMillisecondsForTesting() const;

    double getPrecommitSleepProbabilityForTesting() const;

    bool shouldBlockACL() const;
    void setBlockACL(bool block_acl_);

    bool isOperationSupported(Coordination::OpNum operation) const;
private:
    /// local disk defined using path or disk name
    using Storage = std::variant<DiskPtr, std::string>;

    void initializeFeatureFlags(const Poco::Util::AbstractConfiguration & config);
    void initializeDisks(const Poco::Util::AbstractConfiguration & config);

    Storage getRocksDBPathFromConfig(const Poco::Util::AbstractConfiguration & config) const;
    Storage getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config) const;
    Storage getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config) const;
    Storage getStatePathFromConfig(const Poco::Util::AbstractConfiguration & config) const;

    DiskPtr getDisk(const Storage & storage) const;

    std::mutex local_logs_preprocessed_cv_mutex;
    std::condition_variable local_logs_preprocessed_cv;

    /// set to true when we have preprocessed or committed all the logs
    /// that were already present locally during startup
    std::atomic<bool> local_logs_preprocessed = false;
    std::atomic<bool> shutdown_called = false;

    std::atomic<Phase> server_state{Phase::INIT};

    bool ignore_system_path_on_startup{false};
    bool digest_enabled{true};
    bool digest_enabled_on_commit{false};

    std::shared_ptr<DiskSelector> disk_selector;

    Storage rocksdb_storage;
    Storage log_storage;
    Storage latest_log_storage;
    Storage snapshot_storage;
    Storage latest_snapshot_storage;
    Storage state_file_storage;

    std::shared_ptr<rocksdb::Options> rocksdb_options;

    std::vector<std::string> old_log_disk_names;
    std::vector<std::string> old_snapshot_disk_names;

    bool standalone_keeper;

    std::unordered_map<std::string, std::string> system_nodes_with_data;

    KeeperFeatureFlags feature_flags;
    KeeperDispatcher * dispatcher{nullptr};

    std::atomic<UInt64> memory_soft_limit = 0;

    std::atomic<UInt64> last_committed_log_idx = 0;

    /// will be set by dispatcher when waiting for certain commits
    std::optional<UInt64> wait_commit_upto_idx = 0;
    std::mutex last_committed_log_idx_cv_mutex;
    std::condition_variable last_committed_log_idx_cv;

    int64_t precommit_sleep_ms_for_testing = 0;
    double precommit_sleep_probability_for_testing = 0.0;

    CoordinationSettingsPtr coordination_settings;

    bool block_acl = false;
};

using KeeperContextPtr = std::shared_ptr<KeeperContext>;
}
