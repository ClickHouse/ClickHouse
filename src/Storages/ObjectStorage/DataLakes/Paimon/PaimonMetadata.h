#pragma once
#include <config.h>

#if USE_AVRO

#include <atomic>
#include <map>
#include <mutex>
#include <optional>
#include <vector>
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

    /// The columns of a data file that has a schema transform, under the names the file was written with.
    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr local_context, ObjectInfoPtr object_info) const override;

private:
    enum class ManifestKind : UInt8
    {
        Base,
        Delta,
        Both,
    };

    /// `schema_transforms[i]` is the schema transform of `paths[i]`, nullptr if the file is read by column name.
    struct DataFilesToRead
    {
        Strings paths;
        std::vector<std::shared_ptr<ActionsDAG>> schema_transforms;
    };

    DataFilesToRead collectDataFilesFromManifests(
        const std::vector<PaimonTableStatePtr> & snapshots,
        const PaimonTableSchema & read_schema,
        ManifestKind kind,
        const std::optional<Paimon::PartitionPruner> & partition_pruner,
        bool deduplicate,
        bool track_deletes,
        std::optional<Int64> incremental_cursor = {}) const;

    /// Lock-free read of current state
    PaimonTableStatePtr getCurrentState() const;

    /// Load latest state from object storage (I/O outside of any lock)
    PaimonTableStatePtr loadLatestState() const;

    /// Validate that the underlying Paimon table still has the same identity.
    void validateTableIdentity() const;

    /// Throws if the snapshot's index manifest lists deletion vectors. Pass the cursor of an incremental read for its recovery hint.
    void checkNoDeletionVectors(const PaimonTableState & state, std::optional<Int64> incremental_cursor = {}) const;

    /// The transform from the columns of a data file written with schema `file_schema_id` to the columns of `read_schema`,
    /// matched by field id. nullptr if reading the file by column name gives the same columns. Throws if a field inside
    /// a ROW column would be bound to another field id. Pass the cursor of an incremental read for its recovery hint.
    std::shared_ptr<ActionsDAG> getDataFileSchemaTransform(
        const String & file_path,
        Int64 file_schema_id,
        const PaimonTableSchema & read_schema,
        std::optional<Int64> incremental_cursor) const;

    /// `transform` is the result of `getDataFileSchemaTransform`. `in_file` and `in_read_schema` are empty if the file
    /// can be read, else they name the field inside a ROW column that makes it unreadable.
    struct FileSchemaMapping
    {
        std::shared_ptr<ActionsDAG> transform;
        String in_file;
        String in_read_schema;
    };

    FileSchemaMapping getDataFileSchemaMapping(Int64 file_schema_id, const PaimonTableSchema & read_schema) const;

    /// Load state for a specific snapshot ID
    PaimonTableStatePtr loadStateForSnapshot(Int64 snapshot_id) const;

    /// Get snapshots between from_snapshot (exclusive) and to_snapshot (inclusive).
    /// If max_snapshots_to_load > 0, stop loading once the limit is reached.
    /// If append_only is true, only APPEND snapshots are returned (and OVERWRITE ones with
    /// paimon_incremental_read_append_overwrite): COMPACT, OVERWRITE and ANALYZE ones are skipped
    /// (after checking them for deletion vectors), and any other commit kind throws.
    /// last_scanned_snapshot_id is set to the highest snapshot_id actually visited
    /// (including skipped / missing ones), so the caller can advance the watermark
    /// past gaps that produced no data files. last_scanned_state is its state, or nullptr
    /// when that id was expired.
    std::vector<PaimonTableStatePtr> getSnapshotsBetween(
        Int64 from_snapshot_id,
        Int64 to_snapshot_id,
        UInt64 max_snapshots_to_load,
        bool append_only,
        std::optional<Int64> & last_scanned_snapshot_id,
        PaimonTableStatePtr & last_scanned_state) const;

    /// Extract table state from storage_metadata
    static PaimonTableStatePtr extractTableState(StorageMetadataPtr storage_metadata);

    /// Get or load manifest file list (uses cache)
    ManifestListConstPtr getManifestList(const String & manifest_list_path) const;

    /// Get or load manifest content (uses cache)
    ManifestConstPtr getManifest(const String & manifest_path, Int64 schema_id) const;

    /// Get or load index manifest content (uses cache)
    PaimonIndexManifest getIndexManifest(const String & index_manifest_path) const;

    /// Validate configuration
    void checkSupportedConfiguration() const;

    /// Collect data files for incremental read (from committed snapshot to current).
    /// `committed` is the watermark observed under the processing lock.
    DataFilesToRead collectIncrementalDataFiles(
        const PaimonTableStatePtr & state,
        const PaimonTableSchema & read_schema,
        const std::optional<Paimon::PartitionPruner> & partition_pruner,
        UInt64 max_consume_snapshots,
        const PaimonCommittedSnapshot & committed,
        std::optional<Int64> & last_consumed_snapshot_id,
        std::optional<PaimonSnapshotIdentity> & last_consumed_identity) const;

    /// Throws INVALID_STATE if the warehouse's snapshot `snapshot_id` is not the one the cursor consumed.
    void checkCursorSnapshotUnchanged(Int64 snapshot_id, const PaimonSnapshotIdentity & consumed) const;

    /// Collect data files for a specific snapshot delta (session-level targeted read)
    DataFilesToRead collectDeltaFilesForSnapshot(
        const PaimonTableStatePtr & state,
        const PaimonTableSchema & read_schema,
        const std::optional<Paimon::PartitionPruner> & partition_pruner) const;

    /// Collect data files for full scan
    DataFilesToRead collectFullScanDataFiles(
        const PaimonTableStatePtr & state,
        const PaimonTableSchema & read_schema,
        const std::optional<Paimon::PartitionPruner> & partition_pruner) const;

    /// Background refresh task entry
    void scheduleBackgroundRefresh();
    void runBackgroundRefresh();


    mutable std::shared_ptr<const PaimonTableState> current_state{nullptr};

    /// Update mutex: only held briefly during state replacement
    mutable std::mutex update_mutex;

    /// `getDataFileSchemaMapping` by (file schema id, read schema id). A schema never changes once written.
    mutable std::mutex file_schema_transforms_mutex;
    mutable std::map<std::pair<Int64, Int64>, FileSchemaMapping> file_schema_transforms TSA_GUARDED_BY(file_schema_transforms_mutex);

    /// Persistent components: thread-safe or immutable
    PaimonPersistentComponents persistent_components;

    PaimonTableClientPtr table_client;

    const ObjectStoragePtr object_storage;

    LoggerPtr log;

    constexpr static String PARTITION_DEFAULT_VALUE = "__DEFAULT_PARTITION__";

    /// Background refresh. `refresh_task` must be declared last so it is destroyed first:
    /// ~BackgroundSchedulePoolTaskHolder calls deactivate() which synchronizes with runBackgroundRefresh()
    /// before `refresh_interval_sec`, `refresh_in_progress`, and other members are destroyed.
    size_t refresh_interval_sec = 0;
    std::atomic_bool refresh_in_progress{false};
    BackgroundSchedulePoolTaskHolder refresh_task;
};

}

#endif
