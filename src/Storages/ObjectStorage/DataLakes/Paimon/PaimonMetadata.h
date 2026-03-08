#pragma once
#include <config.h>

#if USE_AVRO

#include <atomic>
#include <mutex>
#include <optional>
#include <vector>
#include <Core/Block.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonPersistentComponents.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PartitionPruner.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Core/BackgroundSchedulePool.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>


namespace DB
{

using namespace Paimon;

class PaimonMetadata : public IDataLakeMetadata, private WithContext
{
public:
    static constexpr auto name = "Paimon";

    PaimonMetadata(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        const DB::ContextPtr & context_,
        PaimonPersistentComponents persistent_components_,
        PaimonTableClientPtr table_client_);

    static DataLakeMetadataPtr create(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context);

    static void createInitial(
        const ObjectStoragePtr & /*object_storage*/,
        const StorageObjectStorageConfigurationWeakPtr & /*configuration*/,
        const ContextPtr & /*local_context*/,
        const std::optional<ColumnsDescription> & /*columns*/,
        ASTPtr /*partition_by*/,
        ASTPtr /*order_by*/,
        bool /*if_not_exists*/,
        std::shared_ptr<DataLake::ICatalog> /*catalog*/,
        const StorageID & /*table_id_*/)
    {
    }

    const char * getName() const override { return name; }

    /// Get table schema from schema_processor (no heavy lock needed)
    NamesAndTypesList getTableSchema(ContextPtr local_context) const override;

    /// Return the current PaimonTableState as a DataLakeTableStateSnapshot
    /// for snapshot isolation (pins the exact state for query execution).
    std::optional<DataLakeTableStateSnapshot> getTableStateSnapshot(ContextPtr local_context) const override;

    /// Build StorageInMemoryMetadata (columns, etc.) from a pinned PaimonTableState.
    std::unique_ptr<StorageInMemoryMetadata> buildStorageMetadataFromState(
        const DataLakeTableStateSnapshot & state, ContextPtr local_context) const override;

    /// Paimon schema can change across snapshots, so always reload for consistency.
    bool shouldReloadSchemaForConsistency(ContextPtr local_context) const override;

    /// Simplified comparison: only compare snapshot_id
    bool operator==(const IDataLakeMetadata & other) const override;

    bool supportsUpdate() const override { return true; }

    /// Update state using COW pattern, non-blocking for reads
    void update(const ContextPtr & local_context) override;

    /// Extract state from storage_metadata for snapshot isolation
    /// For incremental read mode, this returns only new data since last committed snapshot
    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata,
        ContextPtr query_context) const override;

    /// Check if incremental read mode is enabled
    bool isIncrementalReadEnabled() const;

    /// Get the last committed snapshot ID from Keeper (for incremental read)
    std::optional<Int64> getCommittedSnapshotId() const;

    /// Commit snapshot after successful processing (for incremental read)
    /// This should be called after data has been successfully written to destination
    void commitSnapshot(Int64 snapshot_id);

private:
    enum class ManifestKind : UInt8
    {
        Base,
        Delta,
        Both,
    };

    Strings collectDataFilesFromManifests(
        const std::vector<PaimonTableStatePtr> & snapshots,
        ManifestKind kind,
        const std::optional<PartitionPruner> & partition_pruner,
        bool deduplicate,
        bool track_deletes) const;

    /// Lock-free read of current state
    PaimonTableStatePtr getCurrentState() const;

    /// Load latest state from object storage (I/O outside of any lock)
    PaimonTableStatePtr loadLatestState() const;

    /// Load state for a specific snapshot ID
    PaimonTableStatePtr loadStateForSnapshot(Int64 snapshot_id) const;

    /// Get snapshots between from_snapshot (exclusive) and to_snapshot (inclusive).
    /// If max_snapshots_to_load > 0, stop loading once the limit is reached.
    /// If skip_compact is true, snapshots with commit_kind == "COMPACT" are excluded
    /// (used by incremental read to avoid re-processing compacted data).
    std::vector<PaimonTableStatePtr> getSnapshotsBetween(
        Int64 from_snapshot_id, Int64 to_snapshot_id, UInt64 max_snapshots_to_load = 0, bool skip_compact = false) const;

    /// Extract table state from storage_metadata
    static PaimonTableStatePtr extractTableState(StorageMetadataPtr storage_metadata);

    /// Get or load manifest file list (uses cache)
    std::vector<PaimonManifestFileMeta> getManifestList(const String & manifest_list_path) const;

    /// Get or load manifest content (uses cache)
    PaimonManifest getManifest(const String & manifest_path, Int64 schema_id) const;

    /// Validate configuration
    void checkSupportedConfiguration() const;

    /// Collect data files for incremental read (from committed snapshot to current)
    Strings collectIncrementalDataFiles(
        const PaimonTableStatePtr & state,
        const std::optional<PartitionPruner> & partition_pruner,
        UInt64 max_consume_snapshots,
        std::optional<Int64> & last_consumed_snapshot_id) const;

    /// Collect data files for a specific snapshot delta (session-level targeted read)
    Strings collectDeltaFilesForSnapshot(
        const PaimonTableStatePtr & state,
        const std::optional<PartitionPruner> & partition_pruner) const;

    /// Collect data files for full scan
    Strings collectFullScanDataFiles(
        const PaimonTableStatePtr & state,
        const std::optional<PartitionPruner> & partition_pruner) const;

    /// Background refresh task entry
    void scheduleBackgroundRefresh();
    void runBackgroundRefresh();


    mutable std::shared_ptr<const PaimonTableState> current_state{nullptr};

    /// Update mutex: only held briefly during state replacement
    mutable std::mutex update_mutex;

    /// Persistent components: thread-safe or immutable
    PaimonPersistentComponents persistent_components;

    PaimonTableClientPtr table_client;

    const ObjectStoragePtr object_storage;

    LoggerPtr log;

    constexpr static String PARTITION_DEFAULT_VALUE = "__DEFAULT_PARTITION__";

    /// Background refresh
    BackgroundSchedulePoolTaskHolder refresh_task;
    const std::chrono::milliseconds refresh_interval_ms{0};
    std::atomic_bool refresh_in_progress{false};
};

}

#endif
