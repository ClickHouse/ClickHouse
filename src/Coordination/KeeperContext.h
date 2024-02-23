#pragma once
#include <Coordination/KeeperFeatureFlags.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>

namespace DB
{

class KeeperDispatcher;

struct CoordinationSettings;
using CoordinationSettingsPtr = std::shared_ptr<CoordinationSettings>;

class DiskSelector;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class WriteBufferFromOwnString;

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

    uint64_t lastCommittedIndex() const
    {
        return last_committed_log_idx.load(std::memory_order_relaxed);
    }

    void setLastCommitIndex(uint64_t commit_index)
    {
        last_committed_log_idx.store(commit_index, std::memory_order_relaxed);
        last_committed_log_idx.notify_all();
    }

    void waitLastCommittedIndexUpdated(uint64_t current_last_committed_idx)
    {
        last_committed_log_idx.wait(current_last_committed_idx, std::memory_order_relaxed);
    }

    const CoordinationSettingsPtr & getCoordinationSettings() const;

private:
    /// local disk defined using path or disk name
    using Storage = std::variant<DiskPtr, std::string>;

    void initializeFeatureFlags(const Poco::Util::AbstractConfiguration & config);
    void initializeDisks(const Poco::Util::AbstractConfiguration & config);

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

    std::shared_ptr<DiskSelector> disk_selector;

    Storage log_storage;
    Storage latest_log_storage;
    Storage snapshot_storage;
    Storage latest_snapshot_storage;
    Storage state_file_storage;

    std::vector<std::string> old_log_disk_names;
    std::vector<std::string> old_snapshot_disk_names;

    bool standalone_keeper;

    std::unordered_map<std::string, std::string> system_nodes_with_data;

    KeeperFeatureFlags feature_flags;
    KeeperDispatcher * dispatcher{nullptr};

    std::atomic<UInt64> memory_soft_limit = 0;

    std::atomic<UInt64> last_committed_log_idx = 0;

    CoordinationSettingsPtr coordination_settings;
};

using KeeperContextPtr = std::shared_ptr<KeeperContext>;
}
