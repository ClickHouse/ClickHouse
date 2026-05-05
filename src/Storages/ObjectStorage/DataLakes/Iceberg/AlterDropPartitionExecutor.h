#pragma once

#include "config.h"

#if USE_AVRO

#include <Core/Field.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/PartitionCommands.h>
#include <Common/Logger.h>

#include <Poco/JSON/Object.h>

#include <functional>
#include <optional>
#include <unordered_set>
#include <vector>

namespace DB
{

class FileNamesGenerator;

namespace Iceberg
{

/// Runs `ALTER TABLE ... DROP PARTITION <value>` against an Iceberg table.
///
/// The operation is split into clean phases — AST parsing is done by free helpers
/// in the .cpp file, while this class operates strictly on logical values. Each
/// retry runs the sequence: fetch latest snapshot → (first time only) lock in
/// the set of data-file paths in the partition → classify the freshly fetched
/// manifests against that locked set → build a pure plan → commit. The locked
/// set is preserved across retries, so files that other writers add to the same
/// partition after the operation starts are *never* dropped.
class AlterDropPartitionExecutor
{
public:
    /// `fetch_latest_state` is invoked once per retry and must return the latest
    /// snapshot + table state (with the metadata file forcibly refreshed).
    AlterDropPartitionExecutor(
        const PartitionCommand & command_,
        ContextPtr context_,
        ObjectStoragePtr object_storage_,
        const PersistentTableComponents & components_,
        const DataLakeStorageSettings & data_lake_settings_,
        String write_format_,
        LoggerPtr log_,
        std::function<std::pair<IcebergDataSnapshotPtr, TableStateSnapshot>()> fetch_latest_state_);

    void run();

private:
    /// Self-contained view of the table at one snapshot fetch. The schema id is
    /// validated to fit Int32 here, and partition types are resolved against
    /// the snapshot's schema via the same machinery INSERT uses
    /// (`ChunkPartitioner::getResultTypes`), so downstream code never has to
    /// re-narrow or re-derive them.
    struct SnapshotState
    {
        IcebergDataSnapshotPtr snapshot;
        TableStateSnapshot table_state;
        Poco::JSON::Object::Ptr metadata_object;
        Poco::JSON::Object::Ptr partition_spec;
        Int64 partition_spec_id = 0;
        Int32 schema_id = 0;
        std::vector<String> partition_columns;
        std::vector<DataTypePtr> partition_types;
    };

    /// One manifest's entries split by whether they hit the drop target set.
    struct TargetManifest
    {
        IcebergPathFromMetadata manifest_path;
        ManifestFileContentType manifest_content_type = ManifestFileContentType::DATA;
        std::vector<ProcessedManifestFileEntryPtr> entries_to_keep;     // re-emitted as EXISTING
        std::vector<ProcessedManifestFileEntryPtr> entries_to_remove;   // dropped
    };

    /// Per-manifest split across the whole snapshot. Manifests not present in
    /// either list are unaffected and carried over verbatim from the parent's
    /// manifest list. Both groupings carry the full per-entry groups so that
    /// summary counters (records, bytes, distinct partitions) can be computed
    /// without re-reading the manifests.
    struct TargetManifests
    {
        std::vector<TargetManifest> fully_matched;     // entries_to_keep is empty
        std::vector<TargetManifest> partially_matched; // entries_to_keep is non-empty
    };

    /// Pure description of the drop. No storage handles, no buffers. The
    /// constructor walks the target manifests' entries_to_remove and accumulates
    /// the snapshot-summary counters; nothing else mutates the struct after
    /// construction.
    struct DropPlan
    {
        TargetManifests target_manifests;
        Iceberg::SnapshotSummary snapshot_summary;

        explicit DropPlan(TargetManifests && target_manifests_);
    };

    /// Storage-resolved file paths matched on the first scan of the partition.
    /// Populated once and reused across retries; files added by concurrent
    /// writers after this point are not in the set and never dropped.
    struct TargetFilePaths
    {
        std::unordered_set<String> data;
        std::unordered_set<String> position_delete;

        bool empty() const { return data.empty() && position_delete.empty(); }
    };

    /// Result of writing one replacement manifest. Needed to build the manifest list.
    struct ReplacementManifestWrite
    {
        IcebergPathFromMetadata new_manifest_path;
        Int64 manifest_length = 0;
        Int64 min_sequence_number = 0;
        Int32 existing_rows_count = 0;
        Int32 existing_files_count = 0;
        FileContentType content_type = FileContentType::DATA;
    };

    // ---- step 1: fetch ---------------------------------------------------
    std::optional<SnapshotState> fetchSnapshotState();

    // ---- step 2: scan the partition once, return the target file set ----
    TargetFilePaths discoverTargetFilePaths(const SnapshotState & state, const Row & target_partition) const;

    // ---- step 3: find the manifests that hit the target file set --------
    TargetManifests findTargetManifests(const SnapshotState & state, const TargetFilePaths & targets) const;

    /// Append each entry in `entries` to `out.entries_to_remove` if its
    /// storage path is in `target_paths`, otherwise to `out.entries_to_keep`.
    static void matchEntries(
        const std::vector<ProcessedManifestFileEntryPtr> & entries,
        const std::unordered_set<String> & target_paths,
        const IcebergPathResolver & path_resolver,
        TargetManifest & out);

    // ---- step 4: commit (three storage-I/O sub-steps) -------------------
    /// Returns true on a successful commit, false if the CAS race was lost
    /// (the caller should retry). Throws on any other failure. Mutates
    /// state.metadata_object in place via MetadataGenerator.
    bool tryCommit(SnapshotState & state, DropPlan plan);

    /// Sub-step 5a: write a replacement manifest for each partially-matched manifest.
    std::vector<ReplacementManifestWrite> writeReplacementManifests(
        const SnapshotState & state,
        const DropPlan & plan,
        FileNamesGenerator & filename_generator,
        std::vector<String> & files_for_cleanup);

    /// Sub-step 5b: write the manifest list for the new "delete" snapshot.
    /// Returns the new snapshot's metadata-file info needed by the next step.
    struct ManifestListWriteResult
    {
        Poco::JSON::Object::Ptr new_snapshot;
        GeneratedMetadataFileWithInfo metadata_info;
    };
    ManifestListWriteResult writeManifestList(
        SnapshotState & state,
        const DropPlan & plan,
        const std::vector<ReplacementManifestWrite> & replacements,
        FileNamesGenerator & filename_generator,
        std::vector<String> & files_for_cleanup);

    /// Sub-step 5c: serialize metadata.json and commit it via the version-hint
    /// CAS path. Returns false if the CAS race was lost.
    bool commitMetadataJSON(
        SnapshotState & state,
        FileNamesGenerator & filename_generator,
        const GeneratedMetadataFileWithInfo & metadata_info);

    /// Inputs (set once at construction).
    const PartitionCommand & command;
    ContextPtr context;
    ObjectStoragePtr object_storage;
    const PersistentTableComponents & components;
    const DataLakeStorageSettings & data_lake_settings;
    String write_format;
    LoggerPtr log;
    std::function<std::pair<IcebergDataSnapshotPtr, TableStateSnapshot>()> fetch_latest_state;
};

}
}

#endif
