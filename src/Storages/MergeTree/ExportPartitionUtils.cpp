#include <Storages/MergeTree/ExportPartitionUtils.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include "Storages/ExportReplicatedMergeTreePartitionManifest.h"
#include "Storages/ExportReplicatedMergeTreePartitionTaskEntry.h"
#include <Storages/MergeTree/MergeTreeData.h>
#include <filesystem>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Utils.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/ColumnsDescription.h>

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
    extern const Event ExportPartitionZooKeeperCreate;
    extern const Event ExportPartitionZooKeeperMulti;
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
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TYPE_MISMATCH;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int FILE_ALREADY_EXISTS;
    extern const int METADATA_MISMATCH;
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_BOOL;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int DECIMAL_OVERFLOW;
}

namespace Setting
{
    extern const SettingsBool export_merge_tree_part_allow_lossy_cast;
    extern const SettingsMergeTreePartExportSchemaMismatchMode export_merge_tree_part_schema_mismatch_mode;
}

namespace FailPoints
{
    extern const char iceberg_export_after_commit_before_zk_completed[];
    extern const char export_partition_commit_always_throw[];
}

namespace fs = std::filesystem;

namespace ExportPartitionUtils
{
    bool isNonRetryableExportError(int code)
    {
        /// Deterministic failures where retrying cannot possibly succeed (schema/type
        /// incompatibilities, unsupported features, programming errors). Everything else
        /// (memory limits, network/object-storage/Keeper transient errors, ...) is retryable.
        /// `QUERY_WAS_CANCELLED` is handled separately by the caller and never reaches here.
        ///
        /// ErrorCodes values are runtime `extern const int`, not constant expressions, so they
        /// cannot be used as `switch` labels; compare against a static set instead.
        static const std::unordered_set<int> non_retryable_codes = {
            ErrorCodes::BAD_ARGUMENTS,
            ErrorCodes::TYPE_MISMATCH,
            ErrorCodes::CANNOT_CONVERT_TYPE,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            ErrorCodes::ILLEGAL_COLUMN,
            ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
            ErrorCodes::INCOMPATIBLE_COLUMNS,
            ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
            ErrorCodes::NOT_IMPLEMENTED,
            ErrorCodes::SUPPORT_IS_DISABLED,
            ErrorCodes::LOGICAL_ERROR,
            ErrorCodes::FILE_ALREADY_EXISTS,
            ErrorCodes::METADATA_MISMATCH,
            ErrorCodes::CANNOT_PARSE_TEXT,
            ErrorCodes::CANNOT_PARSE_NUMBER,
            ErrorCodes::CANNOT_PARSE_DATE,
            ErrorCodes::CANNOT_PARSE_DATETIME,
            ErrorCodes::CANNOT_PARSE_BOOL,
            ErrorCodes::CANNOT_PARSE_UUID,
            ErrorCodes::CANNOT_PARSE_IPV4,
            ErrorCodes::CANNOT_PARSE_IPV6,
            ErrorCodes::CANNOT_PARSE_QUOTED_STRING,
            ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE,
            ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
            ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING,
            ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
            ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF,
            ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT,
            ErrorCodes::DECIMAL_OVERFLOW,
        };
        return non_retryable_codes.contains(code);
    }

    Block getPartitionSourceBlockForIcebergCommit(
        MergeTreeData & storage, const String & partition_id)
    {
        auto lock = storage.readLockParts();
        const auto parts = storage.getDataPartsVectorInPartitionForInternalUsage(
            MergeTreeDataPartState::Active, partition_id, lock);

        if (parts.empty())
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART,
                "Cannot find active part for partition_id '{}' to derive Iceberg partition "
                "values. Edge case: the partition may have been dropped after export started, "
                "or this replica has not yet received any part for this partition. "
                "The commit will be retried.",
                partition_id);
        return parts.front()->getMinMaxIndex()->getBlock(storage);
    }

    ContextPtr getContextCopyWithTaskSettings(const ContextPtr & context, const ExportReplicatedMergeTreePartitionManifest & manifest)
    {
        auto context_copy = Context::createCopy(context);
        context_copy->makeQueryContextForExportPart();
        context_copy->setCurrentQueryId(manifest.query_id);
        context_copy->setSetting("output_format_parallel_formatting", manifest.parallel_formatting);
        context_copy->setSetting("output_format_parquet_parallel_encoding", manifest.parquet_parallel_encoding);

        /// Backwards compatibility
        if (manifest.parquet_compression_method)
            context_copy->setSetting("output_format_parquet_compression_method", *manifest.parquet_compression_method);
        if (manifest.output_format_compression_level)
            context_copy->setSetting("output_format_compression_level", *manifest.output_format_compression_level);
        if (manifest.parquet_row_group_size)
            context_copy->setSetting("output_format_parquet_row_group_size", *manifest.parquet_row_group_size);
        if (manifest.parquet_row_group_size_bytes)
            context_copy->setSetting("output_format_parquet_row_group_size_bytes", *manifest.parquet_row_group_size_bytes);
        /// Manifests written before this setting existed have no value here; such tasks were always
        /// scheduled under the old, strict column-count check, so an absent value must resolve to
        /// `strict` regardless of the ambient context's setting (which may have since been changed).
        context_copy->setSetting(
            "export_merge_tree_part_schema_mismatch_mode",
            String(magic_enum::enum_name(manifest.schema_mismatch_mode.value_or(MergeTreePartExportSchemaMismatchMode::strict))));

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

        /// The request-time call to exportPartitionToTable has already validated allow_insert_into_iceberg
        /// against the initiator's settings. Once the manifest is in ZooKeeper, every replica must be
        /// able to execute the task regardless of its own profile - otherwise an export silently
        /// stalls when the setting is only set at the query level.
        context_copy->setSetting("allow_insert_into_iceberg", true);

        /// Reapply the initiator's lossy-cast decision (persisted in the manifest) so the
        /// worker's schema revalidation honors the user's choice. Without this, a task
        /// scheduled without the opt-in could still apply a lossy cast if the destination
        /// schema drifts to a lossy target between scheduling and execution.
        context_copy->setSetting("export_merge_tree_part_allow_lossy_cast", manifest.allow_lossy_cast);

	    return context_copy;
    }

    /// Collect all the exported paths from the processed parts
    /// If multiRead is supported by the keeper implementation, it is done in a single request
    /// Otherwise, multiple async requests are sent
    std::vector<std::string> getExportedPaths(const LoggerPtr & log, const zkutil::ZooKeeperPtr & zk, const std::string & export_path)
    {
        std::vector<std::string> exported_paths;

        LOG_DEBUG(log, "ExportPartition: Getting exported paths for {}", export_path);

        const auto processed_parts_path = fs::path(export_path) / "processed";

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
        std::vector<std::string> processed_parts;
        if (Coordination::Error::ZOK != zk->tryGetChildren(processed_parts_path, processed_parts))
        {
            /// todo arthur do something here
            LOG_WARNING(log, "ExportPartition: Failed to get parts children, exiting");
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
                LOG_WARNING(log, "ExportPartition: Failed to get exported path, exiting");
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
        MergeTreeData & source_storage,
        const String & replica_name)
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

        /// Per-task ephemeral lock that serializes the commit phase across replicas.
        /// Without it, `handlePartExportSuccess` (post-last-part path) and `tryCleanup`
        /// (poll/recovery path) can drive `commitExportPartitionTransaction` concurrently
        /// for the same task.
        const auto commit_lock_path = fs::path(entry_path) / "commit_lock";
        auto commit_lock = zkutil::EphemeralNodeHolder::tryCreate(commit_lock_path, *zk, replica_name);
        if (!commit_lock)
        {
            LOG_DEBUG(log, "ExportPartition: commit_lock for {} is held by another replica, skipping commit on this replica", entry_path);
            return;
        }
        LOG_INFO(log, "ExportPartition: commit_lock for {} acquired by replica {}", entry_path, replica_name);

        /// Honor a concurrent KILL: commit_lock serializes us against killExportPartition,
        /// so a non-PENDING status here means cancel won the race.
        std::string status_str;
        if (!zk->tryGet(fs::path(entry_path) / "status", status_str))
            return;
        const auto status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(status_str);
        if (!status || *status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
        {
            LOG_DEBUG(log, "ExportPartition: {} not PENDING, skipping commit", entry_path);
            return;
        }

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
            const auto source_metadata = source_storage.getInMemoryMetadataPtr(context, false);
            if (source_metadata->hasPartitionKey())
                iceberg_args.partition_source_block =
                    getPartitionSourceBlockForIcebergCommit(source_storage, manifest.partition_id);
        }

        const auto destination_commit_info = destination_storage->commitExportPartitionTransaction(
            manifest.transaction_id, manifest.partition_id, exported_paths, iceberg_args, context);

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

        const std::string status_path = fs::path(entry_path) / "status";
        const std::string completed_name = String(magic_enum::enum_name(ExportReplicatedMergeTreePartitionTaskEntry::Status::COMPLETED)).data();

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(status_path, completed_name, -1));

        ExportReplicatedMergeTreePartitionCommitInfoEntry commit_info_entry {
            destination_commit_info.iceberg_metadata_file,
            destination_commit_info.iceberg_manifest_list,
            destination_commit_info.iceberg_manifest_file,
            destination_commit_info.commit_marker_file};

        const std::string commit_info_path = fs::path(entry_path) / "commit_info";
        ops.emplace_back(zkutil::makeCreateRequest(commit_info_path, commit_info_entry.toJsonString(), zkutil::CreateMode::Persistent));

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperMulti);

        Coordination::Responses responses;
        const auto rc = zk->tryMulti(ops, responses);

        if (rc == Coordination::Error::ZOK)
        {
            LOG_INFO(log, "ExportPartition: Marked export as completed and persisted commit_info");
            return;
        }

        if (rc == Coordination::Error::ZNODEEXISTS)
        {
            LOG_INFO(log, "ExportPartition: commit_info already present (peer wrote it first); task already COMPLETED");
            return;
        }

        throw Exception(ErrorCodes::NETWORK_ERROR, "ExportPartition: Failed to mark export as completed (rc={}), will not try to fix it", rc);
    }

    bool handleCommitFailure(
        const zkutil::ZooKeeperPtr & zk,
        const std::string & entry_path,
        int exception_code,
        const std::string & replica_name,
        const std::string & exception_message,
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
            LOG_DEBUG(log, "ExportPartition: /status missing for {}, skipping commit-failure bookkeeping", entry_path);
            return false;
        }

        const auto status = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(current_status);
        if (!status)
        {
            LOG_WARNING(log, "ExportPartition: Invalid status {} for task {}, skipping commit-failure bookkeeping", current_status, entry_path);
            return false;
        }

        if (status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
        {
            /// Another replica already reached a terminal state (COMPLETED or FAILED).
            /// Do NOT overwrite — a successful commit by a peer must win.
            LOG_DEBUG(log,
                "ExportPartition: /status for {} is {} (not PENDING), skipping commit-failure bookkeeping",
                entry_path, current_status);
            return false;
        }

        Coordination::Requests ops;

        /// Record the exception in the same multi as the (possible) FAILED transition, so the
        /// user-visible last_exception znode is updated atomically with the state change that
        /// exposes it.
        appendExceptionOps(ops, zk, fs::path(entry_path), replica_name, /*part_name=*/"", exception_message, log);

        /// A non-retryable error (schema/spec mismatch, ...) can never succeed,
        /// so fail the task immediately
        const bool non_retryable = isNonRetryableExportError(exception_code);
        if (non_retryable)
        {
            /// Version-checked Set: if /status has changed since we read it (e.g. a peer's
            /// commit() succeeded and wrote COMPLETED), the whole multi aborts with
            /// ZBADVERSION and we safely do nothing — the winning terminal state stands.
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
            LOG_WARNING(log, "ExportPartition: Failed to persist commit failure bookkeeping for {}: {}", entry_path, rc);
            return false;
        }

        LOG_INFO(log,
            "ExportPartition: Commit failure recorded for {} (code {}){}",
            entry_path, exception_code,
            non_retryable ? ", task transitioned to FAILED (non-retryable)" : ", will retry until task timeout");

        return non_retryable;
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
        /// Per-replica leaf under the `last_exception/` container created at task setup.
        /// Each replica only ever writes its own leaf, so cross-replica updates never
        /// race on the count. Concurrent writers within the same replica still race
        /// on read+1+write (best-effort), matching the documented column semantics.
        const auto last_exception_path
            = entry_path / "last_exception" / escapeForFileName(replica_name);

        LastExceptionEntry entry;
        std::string current_data;

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
        const bool leaf_exists = zk->tryGet(last_exception_path, current_data);
        if (leaf_exists)
        {
            try
            {
                entry = LastExceptionEntry::fromJsonString(current_data);
            }
            catch (...)
            {
                LOG_WARNING(log, "ExportPartition: last_exception JSON at {} is malformed, resetting", last_exception_path.string());
                entry = LastExceptionEntry{};
            }
        }

        entry.message = exception_message;
        entry.part = part_name;
        entry.replica = replica_name;
        entry.time = ::time(nullptr);
        entry.count += 1;

        if (!leaf_exists)
        {
            /// Materialize the leaf out-of-band (idempotently) so the op we hand back to the
            /// caller's atomic multi is always a conflict-free Set. Two failing parts on the
            /// same replica whose first failures race would both pick Create here; one of the
            /// enclosing multis would then abort with ZNODEEXISTS and roll back its own part
            /// lock removal, stranding that part behind its ephemeral lock until session loss
            /// or task timeout. A peer thread winning this create (ZNODEEXISTS) is benign.
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
            ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperCreate);
            const auto create_code = zk->tryCreate(last_exception_path, entry.toJsonString(), zkutil::CreateMode::Persistent);
            if (create_code != Coordination::Error::ZOK && create_code != Coordination::Error::ZNODEEXISTS)
                LOG_INFO(log, "ExportPartition: could not pre-create last_exception leaf {}: {}", last_exception_path.string(), create_code);
        }

        /// Always a version -1 Set: it can neither conflict with a peer's create nor abort the
        /// enclosing multi, so the lock-release / FAILED-set ops it accompanies always commit.
        ops.emplace_back(zkutil::makeSetRequest(last_exception_path, entry.toJsonString(), -1));
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

    namespace
    {
        bool haveSameTupleElementLayout(const DataTypePtr & source_type, const DataTypePtr & destination_type)
        {
            const auto source_type_unwrapped = removeNullable(removeLowCardinality(source_type));
            const auto destination_type_unwrapped = removeNullable(removeLowCardinality(destination_type));

            const auto * source_tuple = checkAndGetDataType<DataTypeTuple>(source_type_unwrapped.get());
            const auto * destination_tuple = checkAndGetDataType<DataTypeTuple>(destination_type_unwrapped.get());
            if (source_tuple || destination_tuple)
            {
                if (!source_tuple || !destination_tuple)
                    return false;

                if (source_tuple->hasExplicitNames() && destination_tuple->hasExplicitNames())
                {
                    if (source_tuple->getElementNames() != destination_tuple->getElementNames())
                        return false;
                }
                else if (source_tuple->getElements().size() != destination_tuple->getElements().size())
                    return false;

                const auto & source_elements = source_tuple->getElements();
                const auto & destination_elements = destination_tuple->getElements();
                for (size_t i = 0; i < source_elements.size(); ++i)
                    if (!haveSameTupleElementLayout(source_elements[i], destination_elements[i]))
                        return false;

                return true;
            }

            const auto * source_array = checkAndGetDataType<DataTypeArray>(source_type_unwrapped.get());
            const auto * destination_array = checkAndGetDataType<DataTypeArray>(destination_type_unwrapped.get());
            if (source_array || destination_array)
            {
                if (!source_array || !destination_array)
                    return false;

                return haveSameTupleElementLayout(source_array->getNestedType(), destination_array->getNestedType());
            }

            const auto * source_map = checkAndGetDataType<DataTypeMap>(source_type_unwrapped.get());
            const auto * destination_map = checkAndGetDataType<DataTypeMap>(destination_type_unwrapped.get());
            if (source_map || destination_map)
            {
                if (!source_map || !destination_map)
                    return false;

                return haveSameTupleElementLayout(source_map->getKeyType(), destination_map->getKeyType())
                    && haveSameTupleElementLayout(source_map->getValueType(), destination_map->getValueType());
            }

            return true;
        }

        void verifyPartitionKeyColumn(
            const ColumnWithTypeAndName & source_column,
            const ColumnWithTypeAndName & destination_column,
            size_t position,
            const StorageID & destination_storage_id)
        {
            if (source_column.name != destination_column.name)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot export to {}: partition key column '{}' is at position {} in the source "
                    "table, but the destination's column at that position is named '{}'. EXPORT "
                    "PART/PARTITION matches columns by position, so partition key columns must be "
                    "declared at the same position in both tables.",
                    destination_storage_id.getFullTableName(),
                    source_column.name,
                    position,
                    destination_column.name);

            if (!haveSameTupleElementLayout(source_column.type, destination_column.type))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot export to {}: partition key column '{}' has a different Tuple element "
                    "layout in the source ({}) and destination ({}). Tuple element names must be "
                    "declared in the same order in both tables.",
                    destination_storage_id.getFullTableName(),
                    source_column.name,
                    source_column.type->getName(),
                    destination_column.type->getName());
        }
    }

    void assertPartitionKeyASTAreEqual(
        const StorageMetadataPtr & source_metadata,
        const StorageMetadataPtr & destination_metadata)
    {
        constexpr auto query_to_string = [] (const ASTPtr & ast)
        {
            return ast ? ast->formatWithSecretsOneLine() : "";
        };

        if (query_to_string(source_metadata->getPartitionKeyAST()) != query_to_string(destination_metadata->getPartitionKeyAST()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different partition key");
    }

    void verifyExportSchemaCastable(
        const StorageMetadataPtr & source_metadata,
        const StorageMetadataPtr & destination_metadata,
        const StorageID & destination_storage_id,
        const ContextPtr & context)
    {
        /// Build (and discard) the same converting DAG the export worker will build
        /// later, to surface structural mismatches (column count, untyped casts) early.
        Block source_sample_block;
        for (const auto & column : source_metadata->getColumns().getReadable())
            source_sample_block.insert({column.type->createColumn(), column.type, column.name});

        const auto destination_sample_block = destination_metadata->getSampleBlockNonMaterialized();

        auto source_columns = source_sample_block.getColumnsWithTypeAndName();
        const auto & destination_columns = destination_sample_block.getColumnsWithTypeAndName();

        /// In `ignore_extra_source_columns_by_position` mode a source with more columns than the destination
        /// is allowed: the extra trailing source columns (by position) are dropped, mirroring
        /// the trimming `ExportPartTask::addExportConvertingActions` applies to the real data.
        /// The reverse (destination has more columns than source) is always rejected below by
        /// `makeConvertingActions`, in both modes.
        const bool ignore_extra_source_columns_by_position =
            context->getSettingsRef()[Setting::export_merge_tree_part_schema_mismatch_mode]
                == MergeTreePartExportSchemaMismatchMode::ignore_extra_source_columns_by_position;

        if (ignore_extra_source_columns_by_position && source_columns.size() > destination_columns.size())
        {
            LOG_DEBUG(getLogger("ExportPartitionUtils"),
                "Source has {} columns while destination has {} columns, "
                "the {} extra trailing source column(s) will be ignored",
                source_columns.size(), destination_columns.size(),
                source_columns.size() - destination_columns.size());

            source_columns.resize(destination_columns.size());
        }

        (void) ActionsDAG::makeConvertingActions(
            source_columns,
            destination_columns,
            ActionsDAG::MatchColumnsMode::Position,
            context);

        const auto & source_columns_description = source_metadata->getColumns();
        /// Collect the top-level columns that own columns or subcolumns required by `PARTITION BY`.
        /// For example, both `PARTITION BY t.a` and `PARTITION BY (t.a, t.b)` add `t`.
        std::unordered_set<String> partition_key_owner_columns;
        for (const auto & column_or_subcolumn_name : source_metadata->getColumnsRequiredForPartitionKey())
        {
            auto resolved = source_columns_description.tryGetColumnOrSubcolumn(
                GetColumnsOptions::All, column_or_subcolumn_name);
            const auto & column_name = resolved ? resolved->getNameInStorage() : column_or_subcolumn_name;
            partition_key_owner_columns.insert(column_name);
        }

        const bool allow_lossy_cast = context->getSettingsRef()[Setting::export_merge_tree_part_allow_lossy_cast];

        const size_t num_columns = std::min(source_columns.size(), destination_columns.size());
        for (size_t i = 0; i < num_columns; ++i)
        {
            const auto & source_column = source_columns[i];
            const auto & destination_column = destination_columns[i];

            if (partition_key_owner_columns.contains(source_column.name))
                verifyPartitionKeyColumn(source_column, destination_column, i, destination_storage_id);

            /// Lossy casts may silently change values, so reject them unless the user opts in.
            if (allow_lossy_cast)
                continue;

            if (!canBeSafelyCast(source_column.type, destination_column.type))
                throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS,
                    "Cannot export to {}: column '{}' requires a lossy cast from {} to {}, "
                    "which may change values. Set `export_merge_tree_part_allow_lossy_cast = 1` "
                    "to allow lossy casts during export.",
                    destination_storage_id.getFullTableName(),
                    destination_column.name,
                    source_column.type->getName(),
                    destination_column.type->getName());
        }
    }
}

}
