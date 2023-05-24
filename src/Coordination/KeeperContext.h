#pragma once

#include <Poco/Util/AbstractConfiguration.h>

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

    DiskPtr getCurrentLogDisk() const;
    DiskPtr getLogDisk() const;
    DiskPtr getSnapshotsDisk() const;
    DiskPtr getStateFileDisk() const;
private:
    /// local disk defined using path or disk name
    using Storage = std::variant<DiskPtr, std::string>;

    Storage getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config) const;
    Storage getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config) const;
    Storage getStatePathFromConfig(const Poco::Util::AbstractConfiguration & config) const;

    DiskPtr getDisk(const Storage & storage) const;

    Phase server_state{Phase::INIT};

    bool ignore_system_path_on_startup{false};
    bool digest_enabled{true};

    std::shared_ptr<DiskSelector> disk_selector;

    Storage log_storage;
    Storage current_log_storage;
    Storage snapshot_storage;
    Storage state_file_storage;

    bool standalone_keeper;
};

using KeeperContextPtr = std::shared_ptr<KeeperContext>;

}
