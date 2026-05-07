#include <Storages/MergeTree/ExportPartitionUtils.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include "Storages/ExportReplicatedMergeTreePartitionManifest.h"
#include "Storages/ExportReplicatedMergeTreePartitionTaskEntry.h"
#include <Storages/MergeTree/MergeTreeData.h>
#include <filesystem>
#include <thread>
#include <Interpreters/Context.h>

#if USE_AVRO
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#endif

namespace ProfileEvents
{
    extern const Event ExportPartitionZooKeeperRequests;
    extern const Event ExportPartitionZooKeeperGet;
    extern const Event ExportPartitionZooKeeperGetChildren;
    extern const Event ExportPartitionZooKeeperSet;
    extern const Event ExportPartitionZooKeeperMulti;
    extern const Event ExportPartitionZooKeeperExists;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FAULT_INJECTED;
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_DATA_PART;
    extern const int CORRUPTED_DATA;
    extern const int NETWORK_ERROR;
}

namespace FailPoints
{
    extern const char iceberg_export_after_commit_before_zk_completed[];
    extern const char export_partition_commit_always_throw[];
}

namespace fs = std::filesystem;

namespace ExportPartitionUtils
{
    std::vector<Field> getPartitionValuesForIcebergCommit(
        MergeTreeData & storage, const String & partition_id)
    {
        auto lock = storage.readLockParts();
        const auto parts = storage.getDataPartsVectorInPartitionForInternalUsage(
            MergeTreeDataPartState::Active, partition_id, lock);
        
        /// todo arthur: bad arguments for now, pick a better one
        if (parts.empty())
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART,
                "Cannot find active part for partition_id '{}' to derive Iceberg partition "
                "values. Edge case: the partition may have been dropped after export started, "
                "or this replica has not yet received any part for this partition. "
                "The commit will be retried.",
                partition_id);
        return parts.front()->partition.value;
    }

    ContextPtr getContextCopyWithTaskSettings(const ContextPtr & context, const ExportReplicatedMergeTreePartitionManifest & manifest)
    {
        auto context_copy = Context::createCopy(context);
        context_copy->makeQueryContextForExportPart();
        context_copy->setCurrentQueryId(manifest.query_id);
        context_copy->setSetting("output_format_parallel_formatting", manifest.parallel_formatting);
        context_copy->setSetting("output_format_parquet_parallel_encoding", manifest.parquet_parallel_encoding);
        context_copy->setSetting("max_threads", manifest.max_threads);
        context_copy->setSetting("export_merge_tree_part_file_already_exists_policy", String(magic_enum::enum_name(manifest.file_already_exists_policy)));
        context_copy->setSetting("export_merge_tree_part_max_bytes_per_file", manifest.max_bytes_per_file);
        context_copy->setSetting("export_merge_tree_part_max_rows_per_file", manifest.max_rows_per_file);
        context_copy->setSetting("iceberg_insert_max_bytes_in_data_file", manifest.max_bytes_per_file);
        context_copy->setSetting("iceberg_insert_max_rows_in_data_file", manifest.max_rows_per_file);

        /// always skip pending mutations and patch parts because we already validated the parts during query processing
        context_copy->setSetting("export_merge_tree_part_throw_on_pending_mutations", false);
        context_copy->setSetting("export_merge_tree_part_throw_on_pending_patch_parts", false);

        context_copy->setSetting("export_merge_tree_part_filename_pattern", manifest.filename_pattern);
        context_copy->setSetting("write_full_path_in_iceberg_metadata", manifest.write_full_path_in_iceberg_metadata);

	    return context_copy;
    }

    /// Collect all the exported paths from the processed parts
    /// If multiRead is supported by the keeper implementation, it is done in a single request
    /// Otherwise, multiple async requests are sent
    std::vector<std::string> getExportedPaths(const LoggerPtr & log, const zkutil::ZooKeeperPtr & zk, const std::string & export_path)
    {
        std::vector<std::string> exported_paths;

        LOG_INFO(log, "ExportPartition: Getting exported paths for {}", export_path);

        const auto processed_parts_path = fs::path(export_path) / "processed";

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
        std::vector<std::string> processed_parts;
        if (Coordination::Error::ZOK != zk->tryGetChildren(processed_parts_path, processed_parts))
        {
            /// todo arthur do something here
            LOG_INFO(log, "ExportPartition: Failed to get parts children, exiting");
            return {};
        }

        std::vector<std::string> get_paths;

        for (const auto & processed_part : processed_parts)
        {
            get_paths.emplace_back(processed_parts_path / processed_part);
        }

        auto responses = zk->tryGet(get_paths);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet, get_paths.size());

        responses.waitForResponses();

        for (size_t i = 0; i < responses.size(); ++i)
        {
            if (responses[i].error != Coordination::Error::ZOK)
            {
                /// todo arthur what to do in this case?
                /// It could be that zk is corrupt, in that case we should fail the task
                /// but it can also be some temporary network issue? not sure
                LOG_INFO(log, "ExportPartition: Failed to get exported path, exiting");
                return {};
            }

            const auto processed_part_entry = ExportReplicatedMergeTreePartitionProcessedPartEntry::fromJsonString(responses[i].data);

            for (const auto & path_in_destination : processed_part_entry.paths_in_destination)
            {
                exported_paths.emplace_back(path_in_destination);
            }
        }

        return exported_paths;
    }

    void commit(
        const ExportReplicatedMergeTreePartitionManifest & manifest,
        const StoragePtr & destination_storage,
        const zkutil::ZooKeeperPtr & zk,
        const LoggerPtr & log,
        const std::string & entry_path,
        const ContextPtr & context_in,
        MergeTreeData & source_storage)
    {
        auto context = Context::createCopy(context_in);
        context->setSetting("write_full_path_in_iceberg_metadata", manifest.write_full_path_in_iceberg_metadata);

        /// Failpoint used by integration tests to force persistent commit failure and exercise
        /// the commit-attempts budget / FAILED state transition.
        fiu_do_on(FailPoints::export_partition_commit_always_throw,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED,
                "Failpoint: export_partition_commit_always_throw");
        });

        const auto exported_paths = ExportPartitionUtils::getExportedPaths(log, zk, entry_path);

        if (exported_paths.empty())
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA, "ExportPartition: No exported paths found, will not commit export. This might be a bug");
        }

        //// not checking for an exact match because a single part might generate multiple files
        if (exported_paths.size() < manifest.parts.size())
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA, "ExportPartition: Reached the commit phase, but exported paths size is less than the number of parts, will not commit export. This might be a bug");
        }

        IStorage::IcebergCommitExportPartitionArguments iceberg_args;

        if (!manifest.iceberg_metadata_json.empty())
        {
            iceberg_args.metadata_json_string = manifest.iceberg_metadata_json;
            if (source_storage.getInMemoryMetadataPtr()->hasPartitionKey())
                iceberg_args.partition_values =
                    getPartitionValuesForIcebergCommit(source_storage, manifest.partition_id);
        }

        destination_storage->commitExportPartitionTransaction(manifest.transaction_id, manifest.partition_id, exported_paths, iceberg_args, context);

        /// Failpoint to simulate a crash after the Iceberg commit succeeds but before
        /// ZooKeeper is updated to COMPLETED. Used by idempotency integration tests.
        fiu_do_on(FailPoints::iceberg_export_after_commit_before_zk_completed,
        {
            LOG_INFO(log, "Failpoint: simulating crash after Iceberg commit, before ZK COMPLETED");
            std::this_thread::sleep_for(std::chrono::seconds(10));
            throw Exception(ErrorCodes::FAULT_INJECTED,
                "Failpoint: simulating crash after Iceberg commit, before ZK COMPLETED");
        });

        LOG_INFO(log, "ExportPartition: Committed export, mark as completed");
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperSet);
        if (Coordination::Error::ZOK == zk->trySet(fs::path(entry_path) / "status", String(magic_enum::enum_name(ExportReplicatedMergeTreePartitionTaskEntry::Status::COMPLETED)).data(), -1))
        {
            LOG_INFO(log, "ExportPartition: Marked export as completed");
        }
        else
        {
            throw Exception(ErrorCodes::NETWORK_ERROR, "ExportPartition: Failed to mark export as completed, will not try to fix it");
        }
    }

    bool handleCommitFailure(
        const zkutil::ZooKeeperPtr & zk,
        const std::string & entry_path,
        size_t max_attempts,
        const LoggerPtr & log)
    {
        const std::string status_path = fs::path(entry_path) / "status";

        /// Read /status together with its stat so we can (a) bail early if another
        /// replica has already moved the task out of PENDING and (b) use a
        /// version-checked Set later to avoid clobbering a concurrent write
        /// (e.g. a racing successful commit that marked the task COMPLETED between
        /// our read and our tryMulti).
        Coordination::Stat status_stat;
        std::string current_status;

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
        if (!zk->tryGet(status_path, current_status, &status_stat))
        {
            /// Task was removed (TTL cleanup or force-overwrite). Nothing to do.
            LOG_INFO(log, "ExportPartition: /status missing for {}, skipping commit-failure bookkeeping", entry_path);
            return false;
        }

        const auto status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(current_status);
        if (!status)
        {
            LOG_INFO(log, "ExportPartition: Invalid status {} for task {}, skipping commit-failure bookkeeping", current_status, entry_path);
            return false;
        }

        if (status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
        {
            /// Another replica already reached a terminal state (COMPLETED or FAILED).
            /// Do NOT overwrite — a successful commit by a peer must win.
            LOG_INFO(log,
                "ExportPartition: /status for {} is {} (not PENDING), skipping commit-failure bookkeeping",
                entry_path, current_status);
            return false;
        }

        Coordination::Requests ops;

        /// Bump the global commit_attempts counter (shared across replicas).
        /// Non-atomic get+set(-1), matching exceptions_per_replica/count semantics.
        /// Under a race, two replicas may see the same value and write the same +1,
        /// under-counting by one. FAILED then fires one retry later than the threshold,
        /// which is acceptable (we always converge to FAILED, never "never").
        const std::string commit_attempts_path = fs::path(entry_path) / "commit_attempts";

        size_t attempts = 0;
        std::string attempts_string;

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
        if (zk->tryGet(commit_attempts_path, attempts_string))
        {
            try
            {
                attempts = parse<size_t>(attempts_string);
            }
            catch (...)
            {
                LOG_WARNING(log, "ExportPartition: commit_attempts value '{}' at {} is not a valid integer, treating as 0", attempts_string, commit_attempts_path);
                attempts = 0;
            }

            attempts += 1;
            ops.emplace_back(zkutil::makeSetRequest(commit_attempts_path, std::to_string(attempts), -1));
        }
        else
        {
            attempts = 1;
            ops.emplace_back(zkutil::makeCreateRequest(commit_attempts_path, "1", zkutil::CreateMode::Persistent));
        }

        /// Transition to FAILED if the commit budget is exhausted.
        /// Uses the same setting as per-part retries (manifest.max_retries) per user decision.
        /// Version-checked Set: if /status has changed since we read it (e.g. a peer's
        /// commit() succeeded and wrote COMPLETED), the whole multi aborts with
        /// ZBADVERSION and we safely do nothing — the winning terminal state stands.
        const bool exhausted = attempts >= max_attempts;
        if (exhausted)
        {
            ops.emplace_back(zkutil::makeSetRequest(
                status_path,
                String(magic_enum::enum_name(ExportReplicatedMergeTreePartitionTaskEntry::Status::FAILED)).data(),
                status_stat.version));
        }

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperMulti);
        Coordination::Responses responses;
        const auto rc = zk->tryMulti(ops, responses);
        if (rc != Coordination::Error::ZOK)
        {
            /// Any error here (ZBADVERSION on /status race or counter race, ZNODEEXISTS on
            /// lazy-create race, ZNONODE if someone removed the task concurrently) is
            /// non-fatal: the next attempt re-reads /status and either skips (terminal
            /// state won) or retries the bookkeeping. Worst case we delay FAILED by one
            /// poll cycle, which matches the best-effort property of the existing counters.
            LOG_INFO(log, "ExportPartition: Failed to persist commit failure bookkeeping for {}: {}", entry_path, rc);
            return false;
        }

        LOG_INFO(log,
            "ExportPartition: Commit failure recorded for {} (attempt {}/{}){}",
            entry_path, attempts, max_attempts,
            exhausted ? ", task transitioned to FAILED" : "");

        return exhausted;
    }

    void appendExceptionOps(
        Coordination::Requests & ops,
        const zkutil::ZooKeeperPtr & zk,
        const std::filesystem::path & entry_path,
        const std::string & replica_name,
        const std::string & part_name,
        const std::string & exception_message,
        const LoggerPtr & log)
    {
        const auto exceptions_per_replica_path = entry_path / "exceptions_per_replica" / replica_name;
        const auto count_path = exceptions_per_replica_path / "count";
        const auto last_exception_path = exceptions_per_replica_path / "last_exception";

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperExists);
        if (zk->exists(exceptions_per_replica_path))
        {
            LOG_INFO(log, "ExportPartition: Exceptions per replica path exists, no need to create it");
            std::string num_exceptions_string;

            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
            if (zk->tryGet(count_path, num_exceptions_string))
            {
                const auto num_exceptions = parse<size_t>(num_exceptions_string) + 1;
                ops.emplace_back(zkutil::makeSetRequest(count_path, std::to_string(num_exceptions), -1));
            }
            else
            {
                /// TODO maybe we should find a better way to handle this case, not urgent
                LOG_INFO(log, "ExportPartition: Failed to get number of exceptions, will not increment it");
            }

            ops.emplace_back(zkutil::makeSetRequest(last_exception_path / "part", part_name, -1));
            ops.emplace_back(zkutil::makeSetRequest(last_exception_path / "exception", exception_message, -1));
        }
        else
        {
            LOG_INFO(log, "ExportPartition: Exceptions per replica path does not exist, will create it");
            ops.emplace_back(zkutil::makeCreateRequest(exceptions_per_replica_path, "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(count_path, "1", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(last_exception_path, "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(last_exception_path / "part", part_name, zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(last_exception_path / "exception", exception_message, zkutil::CreateMode::Persistent));
        }
    }

#if USE_AVRO
    void verifyIcebergPartitionCompatibility(
        const Poco::JSON::Object::Ptr & metadata_object,
        const ASTPtr & partition_key_ast)
    {
        const auto original_schema_id = metadata_object->getValue<Int64>(Iceberg::f_current_schema_id);
        const auto partition_spec_id  = metadata_object->getValue<Int64>(Iceberg::f_default_spec_id);

        Poco::JSON::Object::Ptr current_schema_json;
        {
            const auto schemas = metadata_object->getArray(Iceberg::f_schemas);
            for (size_t i = 0; i < schemas->size(); ++i)
            {
                auto s = schemas->getObject(static_cast<UInt32>(i));
                if (s->getValue<Int32>(Iceberg::f_schema_id) == static_cast<Int32>(original_schema_id))
                {
                    current_schema_json = s;
                    break;
                }
            }
        }

        Poco::JSON::Object::Ptr partition_spec_json;
        {
            const auto specs = metadata_object->getArray(Iceberg::f_partition_specs);
            for (size_t i = 0; i < specs->size(); ++i)
            {
                auto s = specs->getObject(static_cast<UInt32>(i));
                if (s->getValue<Int64>(Iceberg::f_spec_id) == partition_spec_id)
                {
                    partition_spec_json = s;
                    break;
                }
            }
        }

        if (!current_schema_json || !partition_spec_json)
            return;

        /// Build column_name → Iceberg source-id from the destination schema (and the inverse).
        std::unordered_map<String, Int32> column_name_to_source_id;
        std::unordered_map<Int32, String> source_id_to_column_name;
        {
            const auto schema_fields = current_schema_json->getArray(Iceberg::f_fields);
            for (size_t i = 0; i < schema_fields->size(); ++i)
            {
                auto f = schema_fields->getObject(static_cast<UInt32>(i));
                const auto col_name  = f->getValue<String>(Iceberg::f_name);
                const auto source_id = f->getValue<Int32>(Iceberg::f_id);
                column_name_to_source_id[col_name]  = source_id;
                source_id_to_column_name[source_id] = col_name;
            }
        }

        auto source_id_to_name = [&](Int32 id) -> String
        {
            auto it = source_id_to_column_name.find(id);
            return it != source_id_to_column_name.end() ? it->second : fmt::format("<unknown source_id={}>", id);
        };

        /// Convert the MergeTree PARTITION BY AST into the equivalent Iceberg spec.
        Poco::JSON::Array::Ptr expected_fields;
        try
        {
            const auto expected_spec = Iceberg::getPartitionSpec(
                partition_key_ast, column_name_to_source_id).first;
            expected_fields = expected_spec->getArray(Iceberg::f_fields);
        }
        catch (const Exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot export partition to Iceberg table: the source MergeTree partition "
                "key cannot be represented as an Iceberg partition spec: {}", e.message());
        }

        const auto actual_fields = partition_spec_json->getArray(Iceberg::f_fields);
        const size_t expected_size = expected_fields ? expected_fields->size() : 0;
        const size_t actual_size   = actual_fields   ? actual_fields->size()   : 0;

        if (expected_size != actual_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot export partition to Iceberg table: partition scheme mismatch. "
                "Source MergeTree has {} partition field(s), destination Iceberg table has {}.",
                expected_size, actual_size);

        for (size_t i = 0; i < expected_size; ++i)
        {
            auto ef = expected_fields->getObject(static_cast<UInt32>(i));
            auto af = actual_fields->getObject(static_cast<UInt32>(i));

            const auto expected_source_id = ef->getValue<Int32>(Iceberg::f_source_id);
            const auto actual_source_id   = af->getValue<Int32>(Iceberg::f_source_id);
            const auto expected_transform = ef->getValue<String>(Iceberg::f_transform);
            const auto actual_transform   = af->getValue<String>(Iceberg::f_transform);

            /// Normalize both transform names through parseTransformAndArgument so that
            /// equivalent aliases ("day"/"days", "hour"/"hours", "year"/"years", etc.)
            /// produced by different writers (ClickHouse vs Spark/Trino) compare equal.
            /// Comparison is on {function_name, argument}; time_zone is writer-specific
            /// and not part of the partition spec identity.
            const auto expected_canonical = Iceberg::parseTransformAndArgument(expected_transform, "");
            const auto actual_canonical   = Iceberg::parseTransformAndArgument(actual_transform, "");
            const bool transforms_match =
                (expected_canonical && actual_canonical)
                    ? (expected_canonical->transform_name == actual_canonical->transform_name
                       && expected_canonical->argument    == actual_canonical->argument)
                    : (expected_transform == actual_transform);

            if (expected_source_id != actual_source_id || !transforms_match)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cannot export partition to Iceberg table: partition field {} mismatch. "
                    "Source MergeTree maps to column '{}' (source_id={}) transform='{}', "
                    "but destination Iceberg has column '{}' (source_id={}) transform='{}'.",
                    i,
                    source_id_to_name(expected_source_id), expected_source_id, expected_transform,
                    source_id_to_name(actual_source_id),   actual_source_id,   actual_transform);
        }
    }
#endif
}

}
