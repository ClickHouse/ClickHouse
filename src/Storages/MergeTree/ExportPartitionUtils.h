#pragma once

#include <filesystem>
#include <vector>
#include <string>
#include <Core/Field.h>
#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include "Storages/IStorage.h"
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
    std::vector<std::string> getExportedPaths(const LoggerPtr & log, const zkutil::ZooKeeperPtr & zk, const std::string & export_path);

    ContextPtr getContextCopyWithTaskSettings(const ContextPtr & context, const ExportReplicatedMergeTreePartitionManifest & manifest);

    /// Returns the partition key values for the given partition_id by reading from
    /// the first active local part. Throws LOGICAL_ERROR if no such part is found.
    ///
    /// Edge case: if the partition was dropped after export started, or this replica
    /// has not yet received any part for this partition (extreme replication lag on a
    /// recovery path), no active part will be found and the commit will fail. The task
    /// will be retried on the next poll cycle or picked up by a different replica.
    std::vector<Field> getPartitionValuesForIcebergCommit(
        MergeTreeData & storage, const String & partition_id);

    void commit(
        const ExportReplicatedMergeTreePartitionManifest & manifest,
        const StoragePtr & destination_storage,
        const zkutil::ZooKeeperPtr & zk,
        const LoggerPtr & log,
        const std::string & entry_path,
        const ContextPtr & context,
        MergeTreeData & source_storage
    );

    /// Handles a commit-phase failure for a replicated partition export:
    ///  - increments <entry_path>/commit_attempts (lazy-created)
    ///  - sets <entry_path>/status to FAILED once attempts >= max_attempts
    ///
    /// The counter is a best-effort, non-atomic get+set(-1), matching
    /// exceptions_per_replica/count. Concurrent failing commits may under-count by one
    /// (FAILED may fire one retry later than the threshold), which is acceptable.
    ///
    /// `replica_name` and `exception` are currently unused and reserved for future
    /// integration with per-replica diagnostics.
    ///
    /// Returns true if this call transitioned the task to FAILED.
    bool handleCommitFailure(
        const zkutil::ZooKeeperPtr & zk,
        const std::string & entry_path,
        size_t max_attempts,
        const LoggerPtr & log);

    /// Appends ZK ops to `ops` that record a per-replica exception under
    ///   <entry_path>/exceptions_per_replica/<replica_name>/last_exception/{exception,part}
    /// and increment <entry_path>/exceptions_per_replica/<replica_name>/count,
    /// creating the subtree if absent.
    ///
    /// The count increment is non-atomic (synchronous tryGet + set with version -1).
    /// Concurrent failing writers may under-count by one, which is accepted in this
    /// subsystem and matches the pre-existing behaviour.
    ///
    /// Intended to be combined with additional ops (for example a version-guarded
    /// status set) and executed as a single `tryMulti` so the exception record and
    /// the accompanying state transition commit atomically.
    void appendExceptionOps(
        Coordination::Requests & ops,
        const zkutil::ZooKeeperPtr & zk,
        const std::filesystem::path & entry_path,
        const std::string & replica_name,
        const std::string & part_name,
        const std::string & exception_message,
        const LoggerPtr & log);

#if USE_AVRO
    /// Verifies that the source MergeTree partition key is compatible with the
    /// destination Iceberg partition spec by comparing field source-ids and
    /// transforms in order. Throws BAD_ARGUMENTS if they do not match.
    void verifyIcebergPartitionCompatibility(
        const Poco::JSON::Object::Ptr & metadata_object,
        const ASTPtr & partition_key_ast);
#endif
}

}
