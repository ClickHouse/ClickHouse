#pragma once

#include <Common/Clusters/ClusterMetadataStorage.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>
#include <boost/noncopyable.hpp>

#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace DB
{

/// Read-only importer for `<cluster_metadata><imports>`.
///
/// It never creates Keeper layout or replica state for imported groups. It only polls
/// `metadata/snapshot_digest`, reloads changed imported snapshots, and asks the owner to republish
/// the visible `ClusterFactory` state when imports change.
class ClusterMetadataImporter : private boost::noncopyable
{
public:
    struct ImportedSnapshot
    {
        String replica_group;
        ClusterMetadataStorage::Snapshot snapshot;
    };

    using PublishCallback = std::function<void()>;

    ClusterMetadataImporter(
        ContextPtr context_,
        String keeper_name_,
        String root_path_,
        bool encrypted_,
        String encryption_key_hex_,
        String encryption_algorithm_,
        std::vector<String> replica_groups_,
        PublishCallback publish_callback_);

    ~ClusterMetadataImporter();

    void startup();
    void shutdown();

    std::vector<ImportedSnapshot> getLoadedSnapshots() const;

private:
    struct ImportedGroup
    {
        String replica_group;
        ClusterMetadataStoragePtr storage;
        ClusterMetadataStorage::Snapshot snapshot;
        String digest;
        bool loaded = false;
    };

    mutable std::mutex mutex;
    bool shutdown_called = false;

    ContextPtr context;
    String keeper_name;
    String root_path;
    bool encrypted = false;
    String encryption_key_hex;
    String encryption_algorithm;
    std::vector<String> replica_groups;
    PublishCallback publish_callback;

    std::vector<ImportedGroup> imported_groups;
    BackgroundSchedulePool::TaskHolder task;

    static constexpr UInt64 REFRESH_INTERVAL_MS = 5000;
    const LoggerPtr log = getLogger("ClusterMetadataImporter");

    void initializeGroups();
    void refreshTask();
    bool refreshSnapshots();
};

using ClusterMetadataImporterPtr = std::shared_ptr<ClusterMetadataImporter>;

}
