#pragma once

#include <filesystem>
#include <vector>
#include <string>
#include <Core/Field.h>
#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include "Storages/IStorage.h"
#include <Storages/StorageInMemoryMetadata.h>
#include <config.h>

#if USE_AVRO
#include <Parsers/IAST.h>
#include <Poco/JSON/Object.h>
#endif

namespace DB
{

class MergeTreeData;
struct ExportReplicatedMergeTreePartitionManifest;

namespace ExportPartitionUtils
{
    bool isNonRetryableExportError(int code);

    std::vector<std::string> getExportedPaths(const LoggerPtr & log, const zkutil::ZooKeeperPtr & zk, const std::string & export_path);

    ContextPtr getContextCopyWithTaskSettings(const ContextPtr & context, const ExportReplicatedMergeTreePartitionManifest & manifest);

    /// Returns the representative source partition-key columns (the first active local part's
    /// minmax block) for the given partition_id. The destination recomputes the Iceberg partition
    /// tuple from this block by casting to its column types and applying the partition transform.
    ///
    /// Edge case: if the partition was dropped after export started, or this replica
    /// has not yet received any part for this partition (extreme replication lag on a
    /// recovery path), no active part will be found and the commit will fail. The task
    /// will be retried on the next poll cycle or picked up by a different replica.
    Block getPartitionSourceBlockForIcebergCommit(
        MergeTreeData & storage, const String & partition_id);

    void commit(
        const ExportReplicatedMergeTreePartitionManifest & manifest,
        const StoragePtr & destination_storage,
        const zkutil::ZooKeeperPtr & zk,
        const LoggerPtr & log,
        const std::string & entry_path,
        const ContextPtr & context,
        MergeTreeData & source_storage,
        const String & replica_name
    );

    /// Handles a commit-phase failure for a replicated partition export:
    ///  - records the exception via appendExceptionOps in the same multi
    ///  - if `exception_code` is non-retryable (see isNonRetryableExportError), sets
    ///    <entry_path>/status to FAILED (version-checked against the PENDING read)
    ///  - otherwise leaves the task PENDING so the commit is retried (by the next
    ///    last-part success or deferred-commit recovery) until the absolute task timeout
    ///
    /// There is no per-task commit-attempt budget: retryable commit failures retry until
    /// success or timeout, matching the per-part retry semantics.
    ///
    /// Returns true if this call transitioned the task to FAILED.
    bool handleCommitFailure(
        const zkutil::ZooKeeperPtr & zk,
        const std::string & entry_path,
        int exception_code,
        const std::string & replica_name,
        const std::string & exception_message,
        const LoggerPtr & log);

    /// Appends a single ZK op to `ops` that writes the per-replica leaf
    ///   <entry_path>/last_exception/<escaped replica_name>
    /// with a JSON-encoded LastExceptionEntry containing the message, part,
    /// replica, time, and an incremented count. If the leaf does not yet exist
    /// the op is a Create; otherwise it is a Set with version -1.
    ///
    /// Cross-replica updates do not race: each replica only writes its own
    /// leaf. Within a single replica the count increment is best-effort and
    /// non-atomic (synchronous tryGet + Set with version -1); concurrent
    /// failing writers may under-count by one, which is accepted.
    void appendExceptionOps(
        Coordination::Requests & ops,
        const zkutil::ZooKeeperPtr & zk,
        const std::filesystem::path & entry_path,
        const std::string & replica_name,
        const std::string & part_name,
        const std::string & exception_message,
        const LoggerPtr & log);

    void assertPartitionKeyASTAreEqual(
        const StorageMetadataPtr & source_metadata,
        const StorageMetadataPtr & destination_metadata);

    /// Validates that source columns can be exported into the destination with the
    /// same positional CAST matching as `INSERT INTO dest SELECT * FROM src`. Lossy
    /// casts are rejected unless `export_merge_tree_part_allow_lossy_cast` is set.
    ///
    /// By default the source and destination must have the same number of columns.
    /// If `export_merge_tree_part_schema_mismatch_mode = 'ignore_extra_source_columns_by_position'`, a
    /// source with more columns than the destination is allowed: the extra trailing
    /// source columns (by position) are excluded from the comparison here, matching
    /// what `ExportPartTask::addExportConvertingActions` drops from the actual data.
    ///
    /// Throws BAD_ARGUMENTS on any violation.
    void verifyExportSchemaCastable(
        const StorageMetadataPtr & source_metadata,
        const StorageMetadataPtr & destination_metadata,
        const StorageID & destination_storage_id,
        const ContextPtr & context);

#if USE_AVRO
    /// Verifies the source MergeTree partition key matches the destination Iceberg
    /// partition spec (source-ids and transforms in order). Throws BAD_ARGUMENTS on
    /// mismatch.
    void verifyIcebergPartitionCompatibility(
        const Poco::JSON::Object::Ptr & metadata_object,
        const ASTPtr & partition_key_ast);
#endif
}

}
