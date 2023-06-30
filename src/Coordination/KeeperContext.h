#pragma once

#include <Poco/Util/AbstractConfiguration.h>

#include <Coordination/KeeperFeatureFlags.h>
#include <IO/WriteBufferFromString.h>
#include <Disks/DiskSelector.h>

#include <cstdint>
#include <memory>

namespace DB
{

class KeeperContext
{
public:
    explicit KeeperContext(bool standalone_keeper_);

    enum class Phase : uint8_t
    {
        INIT,
        RUNNING,
        SHUTDOWN
    };

    void initialize(const Poco::Util::AbstractConfiguration & config);

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
private:
    /// local disk defined using path or disk name
    using Storage = std::variant<DiskPtr, std::string>;

    void initializeFeatureFlags(const Poco::Util::AbstractConfiguration & config);
    void initializeDisks(const Poco::Util::AbstractConfiguration & config);

    Storage getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config) const;
    Storage getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config) const;
    Storage getStatePathFromConfig(const Poco::Util::AbstractConfiguration & config) const;

    DiskPtr getDisk(const Storage & storage) const;

    Phase server_state{Phase::INIT};

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
};

using KeeperContextPtr = std::shared_ptr<KeeperContext>;

}
