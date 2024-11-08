#include <Storages/MergeTree/MergeTreeData.h>

#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Utils.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromSmallFile.h>
#include <Backups/BackupEntryWrappedWith.h>
#include <Backups/IBackup.h>
#include <Backups/RestorerFromBackup.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/CurrentMetrics.h>
#include <Common/Increment.h>
#include <Common/ProfileEventsScope.h>
#include <Common/SimpleIncrement.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils.h>
#include <Common/ThreadFuzzer.h>
#include <Common/escapeForFileName.h>
#include <Common/noexcept_scope.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Compression/CompressedReadBuffer.h>
#include <Core/QueryProcessingStage.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/hasNullable.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Disks/createVolume.h>
#include <IO/Operators.h>
#include <IO/S3Common.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTAlterQuery.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPlan/QueryIdHolder.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/Freeze.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>

#include <boost/range/algorithm_ext/erase.hpp>
#include <boost/algorithm/string/join.hpp>

#include <base/insertAtEnd.h>
#include <base/interpolate.h>
#include <base/isSharedPtrUnique.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <optional>
#include <ranges>
#include <set>
#include <thread>
#include <unordered_set>
#include <filesystem>

#include <fmt/format.h>
#include <Poco/Logger.h>
#include <Poco/Net/NetException.h>

#if USE_AZURE_BLOB_STORAGE
#include <azure/core/http/http.hpp>
#endif

template <>
struct fmt::formatter<DB::DataPartPtr> : fmt::formatter<std::string>
{
    template <typename FormatCtx>
    auto format(const DB::DataPartPtr & part, FormatCtx & ctx) const
    {
        return fmt::formatter<std::string>::format(part->name, ctx);
    }
};


namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event RejectedInserts;
    extern const Event DelayedInserts;
    extern const Event DelayedInsertsMilliseconds;
    extern const Event InsertedWideParts;
    extern const Event InsertedCompactParts;
    extern const Event MergedIntoWideParts;
    extern const Event MergedIntoCompactParts;
    extern const Event RejectedMutations;
    extern const Event DelayedMutations;
    extern const Event DelayedMutationsMilliseconds;
    extern const Event PartsLockWaitMicroseconds;
    extern const Event PartsLockHoldMicroseconds;
    extern const Event LoadedDataParts;
    extern const Event LoadedDataPartsMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric DelayedInserts;
}


namespace
{
    constexpr UInt64 RESERVATION_MIN_ESTIMATION_SIZE = 1u * 1024u * 1024u; /// 1MB
}


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_drop_detached;
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_experimental_full_text_index;
    extern const SettingsBool allow_experimental_inverted_index;
    extern const SettingsBool allow_experimental_vector_similarity_index;
    extern const SettingsBool allow_non_metadata_alters;
    extern const SettingsBool allow_statistics_optimize;
    extern const SettingsBool allow_suspicious_indices;
    extern const SettingsBool alter_move_to_space_execute_async;
    extern const SettingsBool alter_partition_verbose_result;
    extern const SettingsBool apply_mutations_on_fly;
    extern const SettingsBool fsync_metadata;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool materialize_ttl_after_modify;
    extern const SettingsUInt64 max_partition_size_to_drop;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 number_of_mutations_to_delay;
    extern const SettingsUInt64 number_of_mutations_to_throw;
    extern const SettingsBool parallel_replicas_for_non_replicated_merge_tree;
    extern const SettingsUInt64 parts_to_delay_insert;
    extern const SettingsUInt64 parts_to_throw_insert;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_nullable_key;
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
    extern const MergeTreeSettingsBool allow_suspicious_indices;
    extern const MergeTreeSettingsBool assign_part_uuids;
    extern const MergeTreeSettingsBool async_insert;
    extern const MergeTreeSettingsBool check_sample_column_is_correct;
    extern const MergeTreeSettingsBool compatibility_allow_sampling_expression_not_in_primary_key;
    extern const MergeTreeSettingsUInt64 concurrent_part_removal_threshold;
    extern const MergeTreeSettingsDeduplicateMergeProjectionMode deduplicate_merge_projection_mode;
    extern const MergeTreeSettingsBool disable_freeze_partition_for_zero_copy_replication;
    extern const MergeTreeSettingsString disk;
    extern const MergeTreeSettingsBool enable_mixed_granularity_parts;
    extern const MergeTreeSettingsBool fsync_after_insert;
    extern const MergeTreeSettingsBool fsync_part_directory;
    extern const MergeTreeSettingsUInt64 inactive_parts_to_delay_insert;
    extern const MergeTreeSettingsUInt64 inactive_parts_to_throw_insert;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsUInt64 max_avg_part_size_for_too_many_parts;
    extern const MergeTreeSettingsUInt64 max_delay_to_insert;
    extern const MergeTreeSettingsUInt64 max_delay_to_mutate_ms;
    extern const MergeTreeSettingsUInt64 max_file_name_length;
    extern const MergeTreeSettingsUInt64 max_parts_in_total;
    extern const MergeTreeSettingsUInt64 max_projections;
    extern const MergeTreeSettingsUInt64 max_suspicious_broken_parts_bytes;
    extern const MergeTreeSettingsUInt64 max_suspicious_broken_parts;
    extern const MergeTreeSettingsUInt64 min_bytes_for_compact_part;
    extern const MergeTreeSettingsUInt64 min_bytes_for_wide_part;
    extern const MergeTreeSettingsUInt64 min_bytes_to_rebalance_partition_over_jbod;
    extern const MergeTreeSettingsUInt64 min_delay_to_insert_ms;
    extern const MergeTreeSettingsUInt64 min_delay_to_mutate_ms;
    extern const MergeTreeSettingsUInt64 min_rows_for_compact_part;
    extern const MergeTreeSettingsUInt64 min_rows_for_wide_part;
    extern const MergeTreeSettingsUInt64 number_of_mutations_to_delay;
    extern const MergeTreeSettingsUInt64 number_of_mutations_to_throw;
    extern const MergeTreeSettingsSeconds old_parts_lifetime;
    extern const MergeTreeSettingsUInt64 part_moves_between_shards_enable;
    extern const MergeTreeSettingsUInt64 parts_to_delay_insert;
    extern const MergeTreeSettingsUInt64 parts_to_throw_insert;
    extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
    extern const MergeTreeSettingsBool remove_empty_parts;
    extern const MergeTreeSettingsBool remove_rolled_back_parts_immediately;
    extern const MergeTreeSettingsBool replace_long_file_name_to_hash;
    extern const MergeTreeSettingsUInt64 simultaneous_parts_removal_limit;
    extern const MergeTreeSettingsUInt64 sleep_before_loading_outdated_parts_ms;
    extern const MergeTreeSettingsString storage_policy;
    extern const MergeTreeSettingsFloat zero_copy_concurrent_part_removal_max_postpone_ratio;
    extern const MergeTreeSettingsUInt64 zero_copy_concurrent_part_removal_max_split_times;
    extern const MergeTreeSettingsBool prewarm_mark_cache;
}

namespace ServerSetting
{
    extern const ServerSettingsDouble mark_cache_prewarm_ratio;
}

namespace ErrorCodes
{
    extern const int NO_SUCH_DATA_PART;
    extern const int NOT_IMPLEMENTED;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int TOO_MANY_UNEXPECTED_DATA_PARTS;
    extern const int DUPLICATE_DATA_PART;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int CORRUPTED_DATA;
    extern const int BAD_TYPE_OF_FIELD;
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_PARTITION_VALUE;
    extern const int METADATA_MISMATCH;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int TOO_MANY_PARTS;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int BAD_TTL_EXPRESSION;
    extern const int INCORRECT_FILE_NAME;
    extern const int BAD_DATA_PART_NAME;
    extern const int READONLY_SETTING;
    extern const int ABORTED;
    extern const int UNKNOWN_DISK;
    extern const int NOT_ENOUGH_SPACE;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int ZERO_COPY_REPLICATION_ERROR;
    extern const int NOT_INITIALIZED;
    extern const int SERIALIZATION_ERROR;
    extern const int TOO_MANY_MUTATIONS;
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int LIMIT_EXCEEDED;
    extern const int CANNOT_FORGET_PARTITION;
    extern const int DATA_TYPE_CANNOT_BE_USED_IN_KEY;
}

static void checkSuspiciousIndices(const ASTFunction * index_function)
{
    std::unordered_set<UInt64> unique_index_expression_hashes;
    for (const auto & child : index_function->arguments->children)
    {
        const IAST::Hash hash = child->getTreeHash(/*ignore_aliases=*/ true);
        const auto & first_half_of_hash = hash.low64;

        if (!unique_index_expression_hashes.emplace(first_half_of_hash).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Primary key or secondary index contains a duplicate expression. To suppress this exception, rerun the command with setting 'allow_suspicious_indices = 1'");
    }
}

static void checkSampleExpression(const StorageInMemoryMetadata & metadata, bool allow_sampling_expression_not_in_primary_key, bool check_sample_column_is_correct)
{
    if (metadata.sampling_key.column_names.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "There are no columns in sampling expression");

    const auto & pk_sample_block = metadata.getPrimaryKey().sample_block;
    if (!pk_sample_block.has(metadata.sampling_key.column_names[0]) && !allow_sampling_expression_not_in_primary_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sampling expression must be present in the primary key");

    if (!check_sample_column_is_correct)
        return;

    const auto & sampling_key = metadata.getSamplingKey();
    DataTypePtr sampling_column_type = sampling_key.data_types[0];

    bool is_correct_sample_condition = false;
    if (sampling_key.data_types.size() == 1)
    {
        if (typeid_cast<const DataTypeUInt64 *>(sampling_column_type.get()))
            is_correct_sample_condition = true;
        else if (typeid_cast<const DataTypeUInt32 *>(sampling_column_type.get()))
            is_correct_sample_condition = true;
        else if (typeid_cast<const DataTypeUInt16 *>(sampling_column_type.get()))
            is_correct_sample_condition = true;
        else if (typeid_cast<const DataTypeUInt8 *>(sampling_column_type.get()))
            is_correct_sample_condition = true;
    }

    if (!is_correct_sample_condition)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Invalid sampling column type in storage parameters: {}. Must be one unsigned integer type",
            sampling_column_type->getName());
}


void MergeTreeData::initializeDirectoriesAndFormatVersion(const std::string & relative_data_path_, bool attach, const std::string & date_column_name, bool need_create_directories)
{
    relative_data_path = relative_data_path_;

    MergeTreeDataFormatVersion min_format_version(0);
    if (date_column_name.empty())
        min_format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;

    if (relative_data_path.empty())
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "MergeTree storages require data path");

    const auto format_version_path = fs::path(relative_data_path) / MergeTreeData::FORMAT_VERSION_FILE_NAME;
    std::optional<UInt32> read_format_version;

    for (const auto & disk : getDisks())
    {
        if (disk->isBroken())
            continue;

        if (need_create_directories)
        {
            disk->createDirectories(relative_data_path);
            disk->createDirectories(fs::path(relative_data_path) / DETACHED_DIR_NAME);
        }

        if (auto buf = disk->readFileIfExists(format_version_path, getReadSettings()))
        {
            UInt32 current_format_version{0};
            readIntText(current_format_version, *buf);
            if (!buf->eof())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Bad version file: {}", fullPath(disk, format_version_path));

            if (!read_format_version.has_value())
                read_format_version = current_format_version;
            else if (*read_format_version != current_format_version)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                                "Version file on {} contains version {} expected version is {}.",
                                fullPath(disk, format_version_path), current_format_version, *read_format_version);
        }
    }

    /// When data path or file not exists, ignore the format_version check
    if (!attach || !read_format_version)
    {
        format_version = min_format_version;

        /// Try to write to first non-readonly disk
        for (const auto & disk : getStoragePolicy()->getDisks())
        {
            if (disk->isBroken())
               continue;

            /// Write once disk is almost the same as read-only for MergeTree,
            /// since it does not support move, that is required for any
            /// operation over MergeTree, so avoid writing format_version.txt
            /// into it as well, to avoid leaving it after DROP.
            if (!disk->isReadOnly() && !disk->isWriteOnce())
            {
                auto buf = disk->writeFile(format_version_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, getContext()->getWriteSettings());
                writeIntText(format_version.toUnderType(), *buf);
                buf->finalize();
                if (getContext()->getSettingsRef()[Setting::fsync_metadata])
                    buf->sync();
            }

            break;
        }
    }
    else
    {
        format_version = *read_format_version;
    }

    if (format_version < min_format_version)
    {
        if (min_format_version == MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING.toUnderType())
            throw Exception(ErrorCodes::METADATA_MISMATCH, "MergeTree data format version on disk doesn't support custom partitioning");
    }
}


DataPartsLock::DataPartsLock(std::mutex & data_parts_mutex_)
    : wait_watch(Stopwatch(CLOCK_MONOTONIC))
    , lock(data_parts_mutex_)
    , lock_watch(Stopwatch(CLOCK_MONOTONIC))
{
    ProfileEvents::increment(ProfileEvents::PartsLockWaitMicroseconds, wait_watch->elapsedMicroseconds());
}


DataPartsLock::~DataPartsLock()
{
    if (lock_watch.has_value())
        ProfileEvents::increment(ProfileEvents::PartsLockHoldMicroseconds, lock_watch->elapsedMicroseconds());
}

MergeTreeData::MergeTreeData(
    const StorageID & table_id_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool require_part_metadata_,
    LoadingStrictnessLevel mode,
    BrokenPartCallback broken_part_callback_)
    : IStorage(table_id_)
    , WithMutableContext(context_->getGlobalContext())
    , format_version(date_column_name.empty() ? MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING : MERGE_TREE_DATA_OLD_FORMAT_VERSION)
    , merging_params(merging_params_)
    , require_part_metadata(require_part_metadata_)
    , broken_part_callback(broken_part_callback_)
    , log(table_id_.getNameForLogs())
    , storage_settings(std::move(storage_settings_))
    , pinned_part_uuids(std::make_shared<PinnedPartUUIDs>())
    , data_parts_by_info(data_parts_indexes.get<TagByInfo>())
    , data_parts_by_state_and_info(data_parts_indexes.get<TagByStateAndInfo>())
    , parts_mover(this)
    , background_operations_assignee(*this, BackgroundJobsAssignee::Type::DataProcessing, getContext())
    , background_moves_assignee(*this, BackgroundJobsAssignee::Type::Moving, getContext())
{
    context_->getGlobalContext()->initializeBackgroundExecutorsIfNeeded();

    const auto settings = getSettings();

    bool sanity_checks = mode <= LoadingStrictnessLevel::CREATE;

    allow_nullable_key = !sanity_checks || (*settings)[MergeTreeSetting::allow_nullable_key];

    /// Check sanity of MergeTreeSettings. Only when table is created.
    if (sanity_checks)
        settings->sanityCheck(getContext()->getMergeMutateExecutor()->getMaxTasksCount());

    if (!date_column_name.empty())
    {
        try
        {
            checkPartitionKeyAndInitMinMax(metadata_.partition_key);
            setProperties(metadata_, metadata_, !sanity_checks);
            if (minmax_idx_date_column_pos == -1)
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Could not find Date column");
        }
        catch (Exception & e)
        {
            /// Better error message.
            e.addMessage("(while initializing MergeTree partition key from date column " + backQuote(date_column_name) + ")");
            throw;
        }
    }
    else
    {
        is_custom_partitioned = true;
        checkPartitionKeyAndInitMinMax(metadata_.partition_key);
    }
    setProperties(metadata_, metadata_, !sanity_checks);

    /// NOTE: using the same columns list as is read when performing actual merges.
    merging_params.check(metadata_);

    if (metadata_.sampling_key.definition_ast != nullptr)
    {
        /// This is for backward compatibility.
        checkSampleExpression(metadata_, !sanity_checks || (*settings)[MergeTreeSetting::compatibility_allow_sampling_expression_not_in_primary_key],
                              (*settings)[MergeTreeSetting::check_sample_column_is_correct] && sanity_checks);
    }

    checkColumnFilenamesForCollision(metadata_.getColumns(), *settings, sanity_checks);
    checkTTLExpressions(metadata_, metadata_);

    String reason;
    if (!canUsePolymorphicParts(*settings, reason) && !reason.empty())
        LOG_WARNING(log, "{} Settings 'min_rows_for_wide_part'and 'min_bytes_for_wide_part' will be ignored.", reason);

    common_assignee_trigger = [this] (bool delay) noexcept
    {
        if (delay)
            background_operations_assignee.postpone();
        else
            background_operations_assignee.trigger();
    };

    moves_assignee_trigger = [this] (bool delay) noexcept
    {
        if (delay)
            background_moves_assignee.postpone();
        else
            background_moves_assignee.trigger();
    };
}

VirtualColumnsDescription MergeTreeData::createVirtuals(const StorageInMemoryMetadata & metadata)
{
    VirtualColumnsDescription desc;

    desc.addEphemeral("_part", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Name of part");
    desc.addEphemeral("_part_index", std::make_shared<DataTypeUInt64>(), "Sequential index of the part in the query result");
    desc.addEphemeral("_part_uuid", std::make_shared<DataTypeUUID>(), "Unique part identifier (if enabled MergeTree setting assign_part_uuids)");
    desc.addEphemeral("_partition_id", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Name of partition");
    desc.addEphemeral("_sample_factor", std::make_shared<DataTypeFloat64>(), "Sample factor (from the query)");
    desc.addEphemeral("_part_offset", std::make_shared<DataTypeUInt64>(), "Number of row in the part");
    desc.addEphemeral("_part_data_version", std::make_shared<DataTypeUInt64>(), "Data version of part (either min block number or mutation version)");

    if (metadata.hasPartitionKey())
    {
        auto partition_types = metadata.partition_key.sample_block.getDataTypes();
        desc.addEphemeral("_partition_value", std::make_shared<DataTypeTuple>(std::move(partition_types)), "Value (a tuple) of a PARTITION BY expression");
    }

    desc.addPersistent(RowExistsColumn::name, RowExistsColumn::type, nullptr, "Persisted mask created by lightweight delete that show whether row exists or is deleted");
    desc.addPersistent(BlockNumberColumn::name, BlockNumberColumn::type, BlockNumberColumn::codec, "Persisted original number of block that was assigned at insert");
    desc.addPersistent(BlockOffsetColumn::name, BlockOffsetColumn::type, BlockOffsetColumn::codec, "Persisted original number of row in block that was assigned at insert");

    return desc;
}

StoragePolicyPtr MergeTreeData::getStoragePolicy() const
{
    auto settings = getSettings();
    const auto & context = getContext();

    StoragePolicyPtr storage_policy;

    if ((*settings)[MergeTreeSetting::disk].changed)
        storage_policy = context->getStoragePolicyFromDisk((*settings)[MergeTreeSetting::disk]);
    else
        storage_policy = context->getStoragePolicy((*settings)[MergeTreeSetting::storage_policy]);

    return storage_policy;
}

ConditionSelectivityEstimator MergeTreeData::getConditionSelectivityEstimatorByPredicate(
    const StorageSnapshotPtr & storage_snapshot, const ActionsDAG * filter_dag, ContextPtr local_context) const
{
    if (!local_context->getSettingsRef()[Setting::allow_statistics_optimize])
        return {};

    const auto & parts = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data).parts;

    if (parts.empty())
        return {};

    ASTPtr expression_ast;

    ConditionSelectivityEstimator estimator;
    PartitionPruner partition_pruner(storage_snapshot->metadata, filter_dag, local_context);

    if (partition_pruner.isUseless())
    {
        /// Read all partitions.
        for (const auto & part : parts)
        try
        {
            auto stats = part->loadStatistics();
            /// TODO: We only have one stats file for every part.
            estimator.incrementRowCount(part->rows_count);
            for (const auto & stat : stats)
                estimator.addStatistics(part->info.getPartNameV1(), stat);
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("while loading statistics on part {}", part->info.getPartNameV1()));
        }
    }
    else
    {
        for (const auto & part : parts)
        try
        {
            if (!partition_pruner.canBePruned(*part))
            {
                auto stats = part->loadStatistics();
                estimator.incrementRowCount(part->rows_count);
                for (const auto & stat : stats)
                    estimator.addStatistics(part->info.getPartNameV1(), stat);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("while loading statistics on part {}", part->info.getPartNameV1()));
        }
    }

    return estimator;
}

bool MergeTreeData::supportsFinal() const
{
    return merging_params.mode == MergingParams::Collapsing
        || merging_params.mode == MergingParams::Summing
        || merging_params.mode == MergingParams::Aggregating
        || merging_params.mode == MergingParams::Replacing
        || merging_params.mode == MergingParams::Graphite
        || merging_params.mode == MergingParams::VersionedCollapsing;
}

static void checkKeyExpression(const ExpressionActions & expr, const Block & sample_block, const String & key_name, bool allow_nullable_key)
{
    if (expr.hasArrayJoin())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{} key cannot contain array joins", key_name);

    try
    {
        expr.assertDeterministic();
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("for {} key", key_name));
        throw;
    }

    for (const ColumnWithTypeAndName & element : sample_block)
    {
        const ColumnPtr & column = element.column;
        if (column && (isColumnConst(*column) || column->isDummy()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{} key cannot contain constants", key_name);

        if (!allow_nullable_key && hasNullable(element.type))
            throw Exception(
                            ErrorCodes::ILLEGAL_COLUMN,
                            "{} key contains nullable columns, "
                            "but merge tree setting `allow_nullable_key` is disabled", key_name);
    }
}

void MergeTreeData::checkProperties(
    const StorageInMemoryMetadata & new_metadata,
    const StorageInMemoryMetadata & old_metadata,
    bool attach,
    bool allow_empty_sorting_key,
    bool allow_nullable_key_,
    ContextPtr local_context) const
{
    if (!new_metadata.sorting_key.definition_ast && !allow_empty_sorting_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ORDER BY cannot be empty");

    KeyDescription new_sorting_key = new_metadata.sorting_key;
    KeyDescription new_primary_key = new_metadata.primary_key;

    size_t sorting_key_size = new_sorting_key.column_names.size();
    size_t primary_key_size = new_primary_key.column_names.size();
    if (primary_key_size > sorting_key_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary key must be a prefix of the sorting key, but its length: "
            "{} is greater than the sorting key length: {}", primary_key_size, sorting_key_size);

    bool allow_suspicious_indices = (*getSettings())[MergeTreeSetting::allow_suspicious_indices];
    if (local_context)
        allow_suspicious_indices = local_context->getSettingsRef()[Setting::allow_suspicious_indices];

    if (!allow_suspicious_indices && !attach)
        if (const auto * index_function = typeid_cast<ASTFunction *>(new_sorting_key.definition_ast.get()))
            checkSuspiciousIndices(index_function);

    for (size_t i = 0; i < sorting_key_size; ++i)
    {
        const String & sorting_key_column = new_sorting_key.column_names[i];

        if (i < primary_key_size)
        {
            const String & pk_column = new_primary_key.column_names[i];
            if (pk_column != sorting_key_column)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Primary key must be a prefix of the sorting key, "
                                "but the column in the position {} is {}", i, sorting_key_column +", not " + pk_column);

        }
    }

    auto all_columns = new_metadata.columns.getAllPhysical();

    /// This is ALTER, not CREATE/ATTACH TABLE. Let us check that all new columns used in the sorting key
    /// expression have just been added (so that the sorting order is guaranteed to be valid with the new key).

    Names new_primary_key_columns = new_primary_key.column_names;
    Names new_sorting_key_columns = new_sorting_key.column_names;

    ASTPtr added_key_column_expr_list = std::make_shared<ASTExpressionList>();
    const auto & old_sorting_key_columns = old_metadata.getSortingKeyColumns();
    for (size_t new_i = 0, old_i = 0; new_i < sorting_key_size; ++new_i)
    {
        if (old_i < old_sorting_key_columns.size())
        {
            if (new_sorting_key_columns[new_i] != old_sorting_key_columns[old_i])
                added_key_column_expr_list->children.push_back(new_sorting_key.expression_list_ast->children[new_i]);
            else
                ++old_i;
        }
        else
            added_key_column_expr_list->children.push_back(new_sorting_key.expression_list_ast->children[new_i]);
    }

    if (!added_key_column_expr_list->children.empty())
    {
        auto syntax = TreeRewriter(getContext()).analyze(added_key_column_expr_list, all_columns);
        Names used_columns = syntax->requiredSourceColumns();

        NamesAndTypesList deleted_columns;
        NamesAndTypesList added_columns;
        old_metadata.getColumns().getAllPhysical().getDifference(all_columns, deleted_columns, added_columns);

        for (const String & col : used_columns)
        {
            if (!added_columns.contains(col) || deleted_columns.contains(col))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Existing column {} is used in the expression that was added to the sorting key. "
                                "You can add expressions that use only the newly added columns",
                                backQuoteIfNeed(col));

            if (new_metadata.columns.getDefaults().contains(col))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Newly added column {} has a default expression, so adding expressions that use "
                                "it to the sorting key is forbidden", backQuoteIfNeed(col));
        }
    }

    if (!new_metadata.secondary_indices.empty())
    {
        std::unordered_set<String> indices_names;

        for (const auto & index : new_metadata.secondary_indices)
        {
            if (!allow_suspicious_indices && !attach)
            {
                const auto * index_ast = typeid_cast<const ASTIndexDeclaration *>(index.definition_ast.get());
                ASTPtr index_expression = index_ast ? index_ast->getExpression() : nullptr;
                const auto * index_expression_ptr = index_expression ? typeid_cast<const ASTFunction *>(index_expression.get()) : nullptr;
                if (index_expression_ptr)
                    checkSuspiciousIndices(index_expression_ptr);
            }

            MergeTreeIndexFactory::instance().validate(index, attach);

            if (indices_names.find(index.name) != indices_names.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with name {} already exists", backQuote(index.name));

            indices_names.insert(index.name);
        }
    }

    /// If adaptive index granularity is disabled, certain vector search queries with PREWHERE run into LOGICAL_ERRORs.
    ///     SET allow_experimental_vector_similarity_index = 1;
    ///     CREATE TABLE tab (`id` Int32, `vec` Array(Float32), INDEX idx vec TYPE  vector_similarity('hnsw', 'L2Distance') GRANULARITY 100000000) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes = 0;
    ///     INSERT INTO tab SELECT number, [toFloat32(number), 0.] FROM numbers(10000);
    ///     WITH [1., 0.] AS reference_vec SELECT id, L2Distance(vec, reference_vec) FROM tab PREWHERE toLowCardinality(10) ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 100;
    /// As a workaround, force enabled adaptive index granularity for now (it is the default anyways).
    if (new_metadata.secondary_indices.hasType("vector_similarity") && (*getSettings())[MergeTreeSetting::index_granularity_bytes] == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Experimental vector similarity index can only be used with MergeTree setting 'index_granularity_bytes' != 0");

    if (!new_metadata.projections.empty())
    {
        std::unordered_set<String> projections_names;

        for (const auto & projection : new_metadata.projections)
        {
            if (projections_names.find(projection.name) != projections_names.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Projection with name {} already exists", backQuote(projection.name));

            const auto settings = getSettings();
            if (projections_names.size() >= (*settings)[MergeTreeSetting::max_projections])
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Maximum limit of {} projection(s) exceeded", (*settings)[MergeTreeSetting::max_projections]);

            /// We cannot alter a projection so far. So here we do not try to find a projection in old metadata.
            bool is_aggregate = projection.type == ProjectionDescription::Type::Aggregate;
            checkProperties(*projection.metadata, *projection.metadata, attach, is_aggregate, true /* allow_nullable_key */, local_context);
            projections_names.insert(projection.name);
        }
    }

    for (const auto & col : new_metadata.columns)
    {
        if (!col.statistics.empty())
            MergeTreeStatisticsFactory::instance().validate(col.statistics, col.type);
    }

    checkKeyExpression(*new_sorting_key.expression, new_sorting_key.sample_block, "Sorting", allow_nullable_key_);
}

void MergeTreeData::setProperties(
    const StorageInMemoryMetadata & new_metadata,
    const StorageInMemoryMetadata & old_metadata,
    bool attach,
    ContextPtr local_context)
{
    checkProperties(new_metadata, old_metadata, attach, false, allow_nullable_key, local_context);
    setInMemoryMetadata(new_metadata);
    setVirtuals(createVirtuals(new_metadata));
}

namespace
{

ExpressionActionsPtr getCombinedIndicesExpression(
    const KeyDescription & key,
    const MergeTreeIndices & indices,
    const ColumnsDescription & columns,
    ContextPtr context)
{
    ASTPtr combined_expr_list = key.expression_list_ast->clone();

    for (const auto & index : indices)
        for (const auto & index_expr : index->index.expression_list_ast->children)
            combined_expr_list->children.push_back(index_expr->clone());

    auto syntax_result = TreeRewriter(context).analyze(combined_expr_list, columns.getAllPhysical());
    return ExpressionAnalyzer(combined_expr_list, syntax_result, context).getActions(false);
}

}

ExpressionActionsPtr MergeTreeData::getMinMaxExpr(const KeyDescription & partition_key, const ExpressionActionsSettings & settings)
{
    NamesAndTypesList partition_key_columns;
    if (!partition_key.column_names.empty())
        partition_key_columns = partition_key.expression->getRequiredColumnsWithTypes();

    return std::make_shared<ExpressionActions>(ActionsDAG(partition_key_columns), settings);
}

Names MergeTreeData::getMinMaxColumnsNames(const KeyDescription & partition_key)
{
    if (!partition_key.column_names.empty())
        return partition_key.expression->getRequiredColumns();
    return {};
}

DataTypes MergeTreeData::getMinMaxColumnsTypes(const KeyDescription & partition_key)
{
    if (!partition_key.column_names.empty())
        return partition_key.expression->getRequiredColumnsWithTypes().getTypes();
    return {};
}

ExpressionActionsPtr
MergeTreeData::getPrimaryKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot, const MergeTreeIndices & indices) const
{
    return getCombinedIndicesExpression(metadata_snapshot->getPrimaryKey(), indices, metadata_snapshot->getColumns(), getContext());
}

ExpressionActionsPtr
MergeTreeData::getSortingKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot, const MergeTreeIndices & indices) const
{
    return getCombinedIndicesExpression(metadata_snapshot->getSortingKey(), indices, metadata_snapshot->getColumns(), getContext());
}


void MergeTreeData::checkPartitionKeyAndInitMinMax(const KeyDescription & new_partition_key)
{
    if (new_partition_key.expression_list_ast->children.empty())
        return;

    checkKeyExpression(*new_partition_key.expression, new_partition_key.sample_block, "Partition", allow_nullable_key);

    /// Add all columns used in the partition key to the min-max index.
    DataTypes minmax_idx_columns_types = getMinMaxColumnsTypes(new_partition_key);

    /// Try to find the date column in columns used by the partition key (a common case).
    /// If there are no - DateTime or DateTime64 would also suffice.

    bool has_date_column = false;
    bool has_datetime_column = false;

    for (size_t i = 0; i < minmax_idx_columns_types.size(); ++i)
    {
        if (isDate(minmax_idx_columns_types[i]))
        {
            if (!has_date_column)
            {
                minmax_idx_date_column_pos = i;
                has_date_column = true;
            }
            else
            {
                /// There is more than one Date column in partition key and we don't know which one to choose.
                minmax_idx_date_column_pos = -1;
            }
        }
    }
    if (!has_date_column)
    {
        for (size_t i = 0; i < minmax_idx_columns_types.size(); ++i)
        {
            if (isDateTime(minmax_idx_columns_types[i])
                || isDateTime64(minmax_idx_columns_types[i])
            )
            {
                if (!has_datetime_column)
                {
                    minmax_idx_time_column_pos = i;
                    has_datetime_column = true;
                }
                else
                {
                    /// There is more than one DateTime column in partition key and we don't know which one to choose.
                    minmax_idx_time_column_pos = -1;
                }
            }
        }
    }
}


void MergeTreeData::checkTTLExpressions(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata) const
{
    auto new_column_ttls = new_metadata.column_ttls_by_name;

    if (!new_column_ttls.empty())
    {
        NameSet columns_ttl_forbidden;

        if (old_metadata.hasPartitionKey())
            for (const auto & col : old_metadata.getColumnsRequiredForPartitionKey())
                columns_ttl_forbidden.insert(col);

        if (old_metadata.hasSortingKey())
            for (const auto & col : old_metadata.getColumnsRequiredForSortingKey())
                columns_ttl_forbidden.insert(col);

        for (const auto & [name, ttl_description] : new_column_ttls)
        {
            if (columns_ttl_forbidden.contains(name))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Trying to set TTL for key column {}", name);
        }
    }
    auto new_table_ttl = new_metadata.table_ttl;

    if (new_table_ttl.definition_ast)
    {
        for (const auto & move_ttl : new_table_ttl.move_ttl)
        {
            if (!move_ttl.if_exists && !getDestinationForMoveTTL(move_ttl))
            {
                if (move_ttl.destination_type == DataDestinationType::DISK)
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                                    "No such disk {} for given storage policy", backQuote(move_ttl.destination_name));

                throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                                "No such volume {} for given storage policy", backQuote(move_ttl.destination_name));
            }
        }
    }
}

namespace
{
template <typename TMustHaveDataType>
void checkSpecialColumn(const std::string_view column_meta_name, const AlterCommand & command)
{
    if (command.type == AlterCommand::MODIFY_COLUMN)
    {
        if (!command.data_type)
        {
            throw Exception(
                ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                "Trying to modify settings for column {} ({}) ",
                column_meta_name,
                command.column_name);
        }
        else if (!typeid_cast<const TMustHaveDataType *>(command.data_type.get()))
        {
            throw Exception(
                ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                "Cannot alter {} column ({}) to type {}, because it must have type {}",
                column_meta_name,
                command.column_name,
                command.data_type->getName(),
                TypeName<TMustHaveDataType>);
        }
    }
    else if (command.type == AlterCommand::DROP_COLUMN)
    {
        throw Exception(
            ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
            "Trying to ALTER DROP {} ({}) column",
            column_meta_name,
            backQuoteIfNeed(command.column_name));
    }
    else if (command.type == AlterCommand::RENAME_COLUMN)
    {
        throw Exception(
            ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
            "Trying to ALTER RENAME {} ({}) column",
            column_meta_name,
            backQuoteIfNeed(command.column_name));
    }
};
}

void MergeTreeData::checkStoragePolicy(const StoragePolicyPtr & new_storage_policy) const
{
    const auto old_storage_policy = getStoragePolicy();
    old_storage_policy->checkCompatibleWith(new_storage_policy);
}


void MergeTreeData::MergingParams::check(const StorageInMemoryMetadata & metadata) const
{
    const auto columns = metadata.getColumns().getAllPhysical();

    if (!is_deleted_column.empty() && mode != MergingParams::Replacing)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "is_deleted column for MergeTree cannot be specified in modes except Replacing.");

    if (!sign_column.empty() && mode != MergingParams::Collapsing && mode != MergingParams::VersionedCollapsing)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Sign column for MergeTree cannot be specified "
                        "in modes except Collapsing or VersionedCollapsing.");

    if (!version_column.empty() && mode != MergingParams::Replacing && mode != MergingParams::VersionedCollapsing)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Version column for MergeTree cannot be specified "
                        "in modes except Replacing or VersionedCollapsing.");

    if (!columns_to_sum.empty() && mode != MergingParams::Summing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "List of columns to sum for MergeTree cannot be specified in all modes except Summing.");

    /// Check that if the sign column is needed, it exists and is of type Int8.
    auto check_sign_column = [this, & columns](bool is_optional, const std::string & storage)
    {
        if (sign_column.empty())
        {
            if (is_optional)
                return;

            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sign column for storage {} is empty", storage);
        }

        bool miss_column = true;
        for (const auto & column : columns)
        {
            if (column.name == sign_column)
            {
                if (!typeid_cast<const DataTypeInt8 *>(column.type.get()))
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Sign column ({}) for storage {} must have type Int8. "
                            "Provided column of type {}.", sign_column, storage, column.type->getName());
                miss_column = false;
                break;
            }
        }
        if (miss_column)
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Sign column {} does not exist in table declaration.", sign_column);
    };

    /// that if the version_column column is needed, it exists and is of unsigned integer type.
    auto check_version_column = [this, & columns](bool is_optional, const std::string & storage)
    {
        if (version_column.empty())
        {
            if (is_optional)
                return;

            throw Exception(ErrorCodes::LOGICAL_ERROR, "Version column for storage {} is empty", storage);
        }

        bool miss_column = true;
        for (const auto & column : columns)
        {
            if (column.name == version_column)
            {
                if (!column.type->canBeUsedAsVersion())
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                                    "The column {} cannot be used as a version column for storage {} because it is "
                                    "of type {} (must be of an integer type or of type Date/DateTime/DateTime64)",
                                    version_column, storage, column.type->getName());
                miss_column = false;
                break;
            }
        }
        if (miss_column)
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Version column {} does not exist in table declaration.", version_column);
    };

    /// Check that if the is_deleted column is needed, it exists and is of type UInt8. If exist, version column must be defined too but version checks are not done here.
    auto check_is_deleted_column = [this, & columns](bool is_optional, const std::string & storage)
    {
        if (is_deleted_column.empty())
        {
            if (is_optional)
                return;

            throw Exception(ErrorCodes::LOGICAL_ERROR, "`is_deleted` ({}) column for storage {} is empty", is_deleted_column, storage);
        }

        if (version_column.empty() && !is_optional)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Version column ({}) for storage {} is empty while is_deleted ({}) is not.",
                            version_column, storage, is_deleted_column);

        bool miss_is_deleted_column = true;
        for (const auto & column : columns)
        {
            if (column.name == is_deleted_column)
            {
                if (!typeid_cast<const DataTypeUInt8 *>(column.type.get()))
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "is_deleted column ({}) for storage {} must have type UInt8. Provided column of type {}.",
                                    is_deleted_column, storage, column.type->getName());
                miss_is_deleted_column = false;
                break;
            }
        }

        if (miss_is_deleted_column)
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "is_deleted column {} does not exist in table declaration.", is_deleted_column);
    };


    if (mode == MergingParams::Collapsing)
        check_sign_column(false, "CollapsingMergeTree");

    if (mode == MergingParams::Summing)
    {
        auto columns_to_sum_copy = columns_to_sum;
        std::sort(columns_to_sum_copy.begin(), columns_to_sum_copy.end());
        if (const auto it = std::adjacent_find(columns_to_sum_copy.begin(), columns_to_sum_copy.end()); it != columns_to_sum_copy.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} is listed multiple times in the list of columns to sum", *it);

        /// If columns_to_sum are set, then check that such columns exist.
        for (const auto & column_to_sum : columns_to_sum)
        {
            auto check_column_to_sum_exists = [& column_to_sum](const NameAndTypePair & name_and_type)
            {
                return column_to_sum == Nested::extractTableName(name_and_type.name);
            };
            if (columns.end() == std::find_if(columns.begin(), columns.end(), check_column_to_sum_exists))
                throw Exception(
                    ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                    "Column {} listed in columns to sum does not exist in table declaration.",
                    column_to_sum);
        }

        /// Check that summing columns are not in partition key.
        if (metadata.isPartitionKeyDefined())
        {
            auto partition_key_columns = metadata.getPartitionKey().column_names;

            Names names_intersection;
            std::set_intersection(columns_to_sum.begin(), columns_to_sum.end(),
                                  partition_key_columns.begin(), partition_key_columns.end(),
                                  std::back_inserter(names_intersection));

            if (!names_intersection.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Columns: {} listed both in columns to sum and in partition key. "
                "That is not allowed.", boost::algorithm::join(names_intersection, ", "));
        }
    }

    if (mode == MergingParams::Replacing)
    {
        if (!version_column.empty() && version_column == is_deleted_column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The version and is_deleted column cannot be the same column ({})", version_column);

        check_is_deleted_column(true, "ReplacingMergeTree");
        check_version_column(true, "ReplacingMergeTree");
    }

    if (mode == MergingParams::VersionedCollapsing)
    {
        if (!version_column.empty() && version_column == sign_column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The version and sign column cannot be the same column ({})", version_column);

        check_sign_column(false, "VersionedCollapsingMergeTree");
        check_version_column(false, "VersionedCollapsingMergeTree");
    }

    /// TODO Checks for Graphite mode.
}

const Names MergeTreeData::virtuals_useful_for_filter = {"_part", "_partition_id", "_part_uuid", "_partition_value", "_part_data_version"};

Block MergeTreeData::getHeaderWithVirtualsForFilter(const StorageMetadataPtr & metadata) const
{
    const auto columns = metadata->getColumns().getAllPhysical();
    Block header;
    auto virtuals_desc = getVirtualsPtr();
    for (const auto & name : virtuals_useful_for_filter)
    {
        if (columns.contains(name))
            continue;
        if (auto column = virtuals_desc->tryGet(name))
            header.insert({column->type->createColumn(), column->type, name});
    }

    return header;
}

Block MergeTreeData::getBlockWithVirtualsForFilter(
    const StorageMetadataPtr & metadata, const MergeTreeData::DataPartsVector & parts, bool ignore_empty) const
{
    auto block = getHeaderWithVirtualsForFilter(metadata);

    for (const auto & part_or_projection : parts)
    {
        if (ignore_empty && part_or_projection->isEmpty())
            continue;

        const auto * part = part_or_projection->isProjectionPart()
            ? part_or_projection->getParentPart()
            : part_or_projection.get();

        for (auto & column : block)
        {
            auto field = getFieldForConstVirtualColumn(column.name, *part);
            column.column->assumeMutableRef().insert(field);
        }
    }

    return block;
}


std::optional<UInt64> MergeTreeData::totalRowsByPartitionPredicateImpl(
    const ActionsDAG & filter_actions_dag, ContextPtr local_context, const DataPartsVector & parts) const
{
    if (parts.empty())
        return 0;

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto virtual_columns_block = getBlockWithVirtualsForFilter(metadata_snapshot, {parts[0]});

    auto filter_dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag.getOutputs().at(0), nullptr, /*allow_partial_result=*/ false);
    if (!filter_dag)
        return {};

    /// Generate valid expressions for filtering
    bool valid = true;
    for (const auto * input : filter_dag->getInputs())
        if (!virtual_columns_block.has(input->result_name))
            valid = false;

    PartitionPruner partition_pruner(metadata_snapshot, &*filter_dag, local_context, true /* strict */);
    if (partition_pruner.isUseless() && !valid)
        return {};

    std::unordered_set<String> part_values;
    if (valid)
    {
        virtual_columns_block = getBlockWithVirtualsForFilter(metadata_snapshot, parts);
        VirtualColumnUtils::filterBlockWithExpression(VirtualColumnUtils::buildFilterExpression(std::move(*filter_dag), local_context), virtual_columns_block);
        part_values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_part");
        if (part_values.empty())
            return 0;
    }
    // At this point, empty `part_values` means all parts.

    size_t res = 0;
    for (const auto & part : parts)
    {
        if ((part_values.empty() || part_values.find(part->name) != part_values.end()) && !partition_pruner.canBePruned(*part))
            res += part->rows_count;
    }
    return res;
}

String MergeTreeData::MergingParams::getModeName() const
{
    switch (mode)
    {
        case Ordinary:      return "";
        case Collapsing:    return "Collapsing";
        case Summing:       return "Summing";
        case Aggregating:   return "Aggregating";
        case Replacing:     return "Replacing";
        case Graphite:      return "Graphite";
        case VersionedCollapsing: return "VersionedCollapsing";
    }
}

Int64 MergeTreeData::getMaxBlockNumber() const
{
    auto lock = lockParts();

    Int64 max_block_num = 0;
    for (const DataPartPtr & part : data_parts_by_info)
        max_block_num = std::max({max_block_num, part->info.max_block, part->info.mutation});

    return max_block_num;
}

void MergeTreeData::PartLoadingTree::add(const MergeTreePartInfo & info, const String & name, const DiskPtr & disk)
{
    auto & current_ptr = root_by_partition[info.partition_id];
    if (!current_ptr)
        current_ptr = std::make_shared<Node>(MergeTreePartInfo{}, "", disk);

    auto * current = current_ptr.get();
    while (true)
    {
        auto it = current->children.lower_bound(info);
        if (it != current->children.begin())
        {
            auto prev = std::prev(it);
            const auto & prev_info = prev->first;

            if (prev_info.contains(info))
            {
                current = prev->second.get();
                continue;
            }
            if (!prev_info.isDisjoint(info))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Part {} intersects previous part {}. It is a bug or a result of manual intervention in the server or ZooKeeper data",
                    name, prev->second->name);
            }
        }

        if (it != current->children.end())
        {
            const auto & next_info = it->first;

            if (next_info.contains(info))
            {
                current = it->second.get();
                continue;
            }
            if (!next_info.isDisjoint(info))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Part {} intersects next part {}.  It is a bug or a result of manual intervention in the server or ZooKeeper data",
                    name, it->second->name);
            }
        }

        current->children.emplace(info, std::make_shared<Node>(info, name, disk));
        break;
    }
}

template <typename Func>
void MergeTreeData::PartLoadingTree::traverse(bool recursive, Func && func)
{
    std::function<void(const NodePtr &)> traverse_impl = [&](const auto & node)
    {
        func(node);
        if (recursive)
            for (const auto & [_, child] : node->children)
                traverse_impl(child);
    };

    for (const auto & elem : root_by_partition)
        for (const auto & [_, node] : elem.second->children)
            traverse_impl(node);
}

MergeTreeData::PartLoadingTree
MergeTreeData::PartLoadingTree::build(PartLoadingInfos nodes)
{
    std::sort(nodes.begin(), nodes.end(), [](const auto & lhs, const auto & rhs)
    {
        return std::tie(lhs.info.level, lhs.info.mutation) > std::tie(rhs.info.level, rhs.info.mutation);
    });

    PartLoadingTree tree;
    for (const auto & [info, name, disk] : nodes)
        tree.add(info, name, disk);
    return tree;
}

static std::optional<size_t> calculatePartSizeSafe(
    const MergeTreeData::DataPartPtr & part, const LoggerPtr & log)
{
    try
    {
        return part->getDataPartStorage().calculateTotalSizeOnDisk();
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while calculating part size {} on path {}",
            part->name, part->getDataPartStorage().getRelativePath()));
        return {};
    }
}

static void preparePartForRemoval(const MergeTreeMutableDataPartPtr & part)
{
    part->remove_time.store(part->modification_time, std::memory_order_relaxed);
    auto creation_csn = part->version.creation_csn.load(std::memory_order_relaxed);
    if (creation_csn != Tx::RolledBackCSN && creation_csn != Tx::PrehistoricCSN && !part->version.isRemovalTIDLocked())
    {
        /// It's possible that covering part was created without transaction,
        /// but if covered part was created with transaction (i.e. creation_tid is not prehistoric),
        /// then it must have removal tid in metadata file.
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data part {} is Outdated and has creation TID {} and CSN {}, "
                        "but does not have removal tid. It's a bug or a result of manual intervention.",
                        part->name, part->version.creation_tid, creation_csn);
    }

    /// Explicitly set removal_tid_lock for parts w/o transaction (i.e. w/o txn_version.txt)
    /// to avoid keeping part forever (see VersionMetadata::canBeRemoved())
    if (!part->version.isRemovalTIDLocked())
    {
        TransactionInfoContext transaction_context{part->storage.getStorageID(), part->name};
        part->version.lockRemovalTID(Tx::PrehistoricTID, transaction_context);
    }
}

static constexpr size_t loading_parts_initial_backoff_ms = 100;
static constexpr size_t loading_parts_max_backoff_ms = 5000;
static constexpr size_t loading_parts_max_tries = 3;

void MergeTreeData::loadUnexpectedDataPart(UnexpectedPartLoadState & state)
{
    const MergeTreePartInfo & part_info = state.loading_info->info;
    const String & part_name = state.loading_info->name;
    const DiskPtr & part_disk_ptr = state.loading_info->disk;
    LOG_TRACE(log, "Loading unexpected part {} from disk {}", part_name, part_disk_ptr->getName());

    LoadPartResult res;
    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, part_disk_ptr, 0);
    auto data_part_storage = std::make_shared<DataPartStorageOnDiskFull>(single_disk_volume, relative_data_path, part_name);
    String part_path = fs::path(relative_data_path) / part_name;

    try
    {
        state.part = getDataPartBuilder(part_name, single_disk_volume, part_name, getReadSettings())
            .withPartInfo(part_info)
            .withPartFormatFromDisk()
            .build();

        state.part->loadRowsCountFileForUnexpectedPart();
    }
    catch (...)
    {
        LOG_DEBUG(log, "Failed to load unexcepted data part {} with exception: {}", part_name, getExceptionMessage(std::current_exception(), false));
        if (!state.part)
        {
            /// Build a fake part and mark it as broken in case of filesystem error.
            /// If the error impacts part directory instead of single files,
            /// an exception will be thrown during detach and silently ignored.
            state.part = getDataPartBuilder(part_name, single_disk_volume, part_name, getReadSettings())
                .withPartStorageType(MergeTreeDataPartStorageType::Full)
                .withPartType(MergeTreeDataPartType::Wide)
                .build();
        }

        state.is_broken = true;
        tryLogCurrentException(log, fmt::format("while loading unexcepted part {} on path {}", part_name, part_path));
    }
}

MergeTreeData::LoadPartResult MergeTreeData::loadDataPart(
    const MergeTreePartInfo & part_info,
    const String & part_name,
    const DiskPtr & part_disk_ptr,
    MergeTreeDataPartState to_state,
    std::mutex & part_loading_mutex)
{
    LOG_TRACE(log, "Loading {} part {} from disk {}", magic_enum::enum_name(to_state), part_name, part_disk_ptr->getName());

    LoadPartResult res;
    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, part_disk_ptr, 0);
    auto data_part_storage = std::make_shared<DataPartStorageOnDiskFull>(single_disk_volume, relative_data_path, part_name);

    String part_path = fs::path(relative_data_path) / part_name;

    /// Ignore broken parts that can appear as a result of hard server restart.
    auto mark_broken = [&]
    {
        if (!res.part)
        {
            /// Build a fake part and mark it as broken in case of filesystem error.
            /// If the error impacts part directory instead of single files,
            /// an exception will be thrown during detach and silently ignored.
            res.part = getDataPartBuilder(part_name, single_disk_volume, part_name, getReadSettings())
                .withPartStorageType(MergeTreeDataPartStorageType::Full)
                .withPartType(MergeTreeDataPartType::Wide)
                .build();
        }

        res.is_broken = true;
        tryLogCurrentException(log, fmt::format("while loading part {} on path {}", part_name, part_path));

        res.size_of_part = calculatePartSizeSafe(res.part, log.load());
        auto part_size_str = res.size_of_part ? formatReadableSizeWithBinarySuffix(*res.size_of_part) : "failed to calculate size";

        LOG_ERROR(log,
            "Detaching broken part {} (size: {}). "
            "If it happened after update, it is likely because of backward incompatibility. "
            "You need to resolve this manually",
            fs::path(getFullPathOnDisk(part_disk_ptr)) / part_name, part_size_str);
    };

    try
    {
        res.part = getDataPartBuilder(part_name, single_disk_volume, part_name, getReadSettings())
            .withPartInfo(part_info)
            .withPartFormatFromDisk()
            .build();
    }
    catch (...)
    {
        /// Don't count the part as broken if there was a retryalbe error
        /// during loading, such as "not enough memory" or network error.
        if (isRetryableException(std::current_exception()))
            throw;

        LOG_DEBUG(log, "Failed to load data part {} with exception: {}", part_name, getExceptionMessage(std::current_exception(), false));
        mark_broken();
        return res;
    }

    try
    {
        res.part->loadColumnsChecksumsIndexes(require_part_metadata, !part_disk_ptr->isReadOnly());
    }
    catch (...)
    {
        /// Don't count the part as broken if there was a retryalbe error
        /// during loading, such as "not enough memory" or network error.
        if (isRetryableException(std::current_exception()))
            throw;

        mark_broken();
        return res;
    }

    res.part->modification_time = part_disk_ptr->getLastModified(fs::path(relative_data_path) / part_name).epochTime();
    res.part->loadVersionMetadata();

    if (res.part->wasInvolvedInTransaction())
    {
        /// Check if CSNs were written after committing transaction, update and write if needed.
        bool version_updated = false;
        auto & version = res.part->version;
        chassert(!version.creation_tid.isEmpty());

        if (!res.part->version.creation_csn)
        {
            auto min = TransactionLog::getCSNAndAssert(res.part->version.creation_tid, res.part->version.creation_csn);
            if (!min)
            {
                /// Transaction that created this part was not committed. Remove part.
                min = Tx::RolledBackCSN;
            }

            LOG_TRACE(log, "Will fix version metadata of {} after unclean restart: part has creation_tid={}, setting creation_csn={}",
                        res.part->name, res.part->version.creation_tid, min);

            version.creation_csn = min;
            version_updated = true;
        }

        if (!version.removal_tid.isEmpty() && !version.removal_csn)
        {
            auto max = TransactionLog::getCSNAndAssert(version.removal_tid, version.removal_csn);
            if (max)
            {
                LOG_TRACE(log, "Will fix version metadata of {} after unclean restart: part has removal_tid={}, setting removal_csn={}",
                            res.part->name, version.removal_tid, max);
                version.removal_csn = max;
            }
            else
            {
                /// Transaction that tried to remove this part was not committed. Clear removal_tid.
                LOG_TRACE(log, "Will fix version metadata of {} after unclean restart: clearing removal_tid={}",
                            res.part->name, version.removal_tid);
                version.unlockRemovalTID(version.removal_tid, TransactionInfoContext{getStorageID(), res.part->name});
            }

            version_updated = true;
        }

        /// Sanity checks
        bool csn_order = !version.removal_csn || version.creation_csn <= version.removal_csn || version.removal_csn == Tx::PrehistoricCSN;
        bool min_start_csn_order = version.creation_tid.start_csn <= version.creation_csn;
        bool max_start_csn_order = version.removal_tid.start_csn <= version.removal_csn;
        bool creation_csn_known = version.creation_csn;
        if (!csn_order || !min_start_csn_order || !max_start_csn_order || !creation_csn_known)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} has invalid version metadata: {}", res.part->name, version.toString());

        if (version_updated)
            res.part->storeVersionMetadata(/* force */ true);

        /// Deactivate part if creation was not committed or if removal was.
        if (version.creation_csn == Tx::RolledBackCSN || version.removal_csn)
        {
            preparePartForRemoval(res.part);
            to_state = DataPartState::Outdated;
        }
    }

    res.part->setState(to_state);

    DataPartIteratorByInfo it;
    bool inserted;

    {
        std::lock_guard lock(part_loading_mutex);
        LOG_TEST(log, "loadDataPart: inserting {} into data_parts_indexes", res.part->getNameWithState());
        std::tie(it, inserted) = data_parts_indexes.insert(res.part);
    }

    /// Remove duplicate parts with the same checksum.
    if (!inserted)
    {
        if ((*it)->checksums.getTotalChecksumHex() == res.part->checksums.getTotalChecksumHex())
        {
            LOG_ERROR(log, "Remove duplicate part {}", data_part_storage->getFullPath());
            res.part->is_duplicate = true;
            return res;
        }

        throw Exception(ErrorCodes::DUPLICATE_DATA_PART, "Part {} already exists but with different checksums", res.part->name);
    }

    if (to_state == DataPartState::Active)
        addPartContributionToDataVolume(res.part);

    if (res.part->hasLightweightDelete())
        has_lightweight_delete_parts.store(true);

    LOG_TRACE(log, "Finished loading {} part {} on disk {}", magic_enum::enum_name(to_state), part_name, part_disk_ptr->getName());
    return res;
}

MergeTreeData::LoadPartResult MergeTreeData::loadDataPartWithRetries(
    const MergeTreePartInfo & part_info,
    const String & part_name,
    const DiskPtr & part_disk_ptr,
    MergeTreeDataPartState to_state,
    std::mutex & part_loading_mutex,
    size_t initial_backoff_ms,
    size_t max_backoff_ms,
    size_t max_tries)
{
    auto handle_exception = [&, this](std::exception_ptr exception_ptr, size_t try_no)
    {
        if (try_no + 1 == max_tries)
            throw;

        LOG_DEBUG(log,
            "Failed to load data part {} at try {} with retryable error: {}. Will retry in {} ms",
             part_name, try_no, getExceptionMessage(exception_ptr, false), initial_backoff_ms);

        std::this_thread::sleep_for(std::chrono::milliseconds(initial_backoff_ms));
        initial_backoff_ms = std::min(initial_backoff_ms * 2, max_backoff_ms);
    };

    for (size_t try_no = 0; try_no < max_tries; ++try_no)
    {
        try
        {
            return loadDataPart(part_info, part_name, part_disk_ptr, to_state, part_loading_mutex);
        }
        catch (...)
        {
            if (isRetryableException(std::current_exception()))
                handle_exception(std::current_exception(),try_no);
            else
                throw;
        }
    }
    UNREACHABLE();
}


std::vector<MergeTreeData::LoadPartResult> MergeTreeData::loadDataPartsFromDisk(PartLoadingTreeNodes & parts_to_load)
{
    const size_t num_parts = parts_to_load.size();

    LOG_TRACE(log, "Will load {} parts using up to {} threads", num_parts, getActivePartsLoadingThreadPool().get().getMaxThreads());

    /// Shuffle all the parts randomly to possible speed up loading them from JBOD.
    std::shuffle(parts_to_load.begin(), parts_to_load.end(), thread_local_rng);

    std::mutex part_select_mutex;
    std::mutex part_loading_mutex;

    std::vector<LoadPartResult> loaded_parts;

    ThreadPoolCallbackRunnerLocal<void> runner(getActivePartsLoadingThreadPool().get(), "ActiveParts");
    while (true)
    {
        bool are_parts_to_load_empty = false;
        {
            std::lock_guard lock(part_select_mutex);
            are_parts_to_load_empty = parts_to_load.empty();
        }

        if (are_parts_to_load_empty)
        {
            /// Wait for all scheduled tasks.
            runner.waitForAllToFinishAndRethrowFirstError();

            /// At this point it is possible, that some other parts appeared in the queue for processing (parts_to_load),
            /// because we added them from inside the pool.
            /// So we need to recheck it.
        }

        PartLoadingTree::NodePtr current_part;
        {
            std::lock_guard lock(part_select_mutex);
            if (parts_to_load.empty())
                break;

            current_part = parts_to_load.back();
            parts_to_load.pop_back();
        }

        runner(
            [&, part = std::move(current_part)]()
            {
                /// Pass a separate mutex to guard the set of parts, because this lambda
                /// is called concurrently but with already locked @data_parts_mutex.
                auto res = loadDataPartWithRetries(
                    part->info, part->name, part->disk,
                    DataPartState::Active, part_loading_mutex, loading_parts_initial_backoff_ms,
                    loading_parts_max_backoff_ms, loading_parts_max_tries);

                part->is_loaded = true;
                bool is_active_part = res.part->getState() == DataPartState::Active;

                /// If part is broken or duplicate or should be removed according to transaction
                /// and it has any covered parts then try to load them to replace this part.
                if (!is_active_part && !part->children.empty())
                {
                    std::lock_guard lock{part_select_mutex};
                    for (const auto & [_, node] : part->children)
                        parts_to_load.push_back(node);
                }

                {
                    std::lock_guard lock(part_loading_mutex);
                    loaded_parts.push_back(std::move(res));
                }
            }, Priority{0});
    }

    return loaded_parts;
}


void MergeTreeData::loadDataParts(bool skip_sanity_checks, std::optional<std::unordered_set<std::string>> expected_parts)
{
    Stopwatch watch;
    LOG_DEBUG(log, "Loading data parts");

    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto settings = getSettings();
    Strings part_file_names;

    auto disks = getStoragePolicy()->getDisks();

    /// Only check if user did touch storage configuration for this table.
    if (!getStoragePolicy()->isDefaultPolicy() && !skip_sanity_checks)
    {
        /// Check extra parts at different disks, in order to not allow to miss data parts at undefined disks.
        std::unordered_set<String> defined_disk_names;

        for (const auto & disk_ptr : disks)
        {
            defined_disk_names.insert(disk_ptr->getName());
        }

        /// In case of delegate disks it is not enough to traverse `disks`,
        /// because for example cache or encrypted disk which wrap s3 disk and s3 disk itself can be put into different storage policies.
        /// But disk->exists returns the same thing for both disks.
        for (const auto & [disk_name, disk] : getContext()->getDisksMap())
        {
            /// As encrypted disk can use the same path of its nested disk,
            /// we need to take it into account here.
            const auto & delegate = disk->getDelegateDiskIfExists();
            if (delegate && disk->getPath() == delegate->getPath())
                defined_disk_names.insert(delegate->getName());

            if (disk->supportsCache())
            {
                /// As cache is implemented on object storage layer, not on disk level, e.g.
                /// we have such structure:
                /// DiskObjectStorage(CachedObjectStorage(...(CachedObjectStored(ObjectStorage)...)))
                /// and disk_ptr->getName() here is the name of last delegate - ObjectStorage.
                /// So now we need to add cache layers to defined disk names.
                auto caches = disk->getCacheLayersNames();
                defined_disk_names.insert(caches.begin(), caches.end());
            }
        }

        std::unordered_set<String> skip_check_disks;
        for (const auto & [disk_name, disk] : getContext()->getDisksMap())
        {
            if (disk->isBroken() || disk->isCustomDisk())
            {
                skip_check_disks.insert(disk_name);
                continue;
            }

            bool is_disk_defined = defined_disk_names.contains(disk_name);

            if (!is_disk_defined && disk->existsDirectory(relative_data_path))
            {
                /// There still a chance that underlying disk is defined in storage policy
                const auto & delegate = disk->getDelegateDiskIfExists();
                is_disk_defined = delegate && !delegate->isBroken() && !delegate->isCustomDisk() && delegate->getPath() == disk->getPath()
                    && defined_disk_names.contains(delegate->getName());
            }

            if (!is_disk_defined && disk->existsDirectory(relative_data_path))
            {
                for (const auto it = disk->iterateDirectory(relative_data_path); it->isValid(); it->next())
                {
                    if (!MergeTreePartInfo::tryParsePartName(it->name(), format_version))
                        continue; /// Cannot parse part name, some garbage on disk, just ignore it.
                    /// But we can't ignore valid part name on undefined disk.
                    throw Exception(
                        ErrorCodes::UNKNOWN_DISK,
                        "Part '{}' ({}) was found on disk '{}' which is not defined in the storage policy '{}' or broken"
                        " (defined disks: [{}], skipped disks: [{}])",
                        it->name(), it->path(), disk_name, getStoragePolicy()->getName(),
                        fmt::join(defined_disk_names, ", "), fmt::join(skip_check_disks, ", "));
                }
            }
        }
    }

    std::vector<PartLoadingTree::PartLoadingInfos> parts_to_load_by_disk(disks.size());
    std::vector<PartLoadingTree::PartLoadingInfos> unexpected_parts_to_load_by_disk(disks.size());

    ThreadPoolCallbackRunnerLocal<void> runner(getActivePartsLoadingThreadPool().get(), "ActiveParts");

    bool all_disks_are_readonly = true;
    for (size_t i = 0; i < disks.size(); ++i)
    {
        const auto & disk_ptr = disks[i];
        if (disk_ptr->isBroken())
            continue;
        if (!disk_ptr->isReadOnly())
            all_disks_are_readonly = false;

        auto & disk_parts = parts_to_load_by_disk[i];
        auto & unexpected_disk_parts = unexpected_parts_to_load_by_disk[i];

        runner([&expected_parts, &unexpected_disk_parts, &disk_parts, this, disk_ptr]()
        {
            for (auto it = disk_ptr->iterateDirectory(relative_data_path); it->isValid(); it->next())
            {
                /// Skip temporary directories, file 'format_version.txt' and directory 'detached'.
                if (startsWith(it->name(), "tmp")
                    || it->name() == MergeTreeData::FORMAT_VERSION_FILE_NAME
                    || it->name() == DETACHED_DIR_NAME)
                    continue;

                if (auto part_info = MergeTreePartInfo::tryParsePartName(it->name(), format_version))
                {
                    if (expected_parts && !expected_parts->contains(it->name()))
                        unexpected_disk_parts.emplace_back(*part_info, it->name(), disk_ptr);
                    else
                        disk_parts.emplace_back(*part_info, it->name(), disk_ptr);
                }
            }
        }, Priority{0});
    }

    /// For iteration to be completed
    runner.waitForAllToFinishAndRethrowFirstError();

    PartLoadingTree::PartLoadingInfos parts_to_load;
    for (auto & disk_parts : parts_to_load_by_disk)
        std::move(disk_parts.begin(), disk_parts.end(), std::back_inserter(parts_to_load));
    PartLoadingTree::PartLoadingInfos unexpected_parts_to_load;
    for (auto & disk_parts : unexpected_parts_to_load_by_disk)
        std::move(disk_parts.begin(), disk_parts.end(), std::back_inserter(unexpected_parts_to_load));

    auto loading_tree = PartLoadingTree::build(std::move(parts_to_load));

    size_t num_parts = 0;
    PartLoadingTreeNodes active_parts;

    /// Collect only "the most covering" parts from the top level of the tree.
    loading_tree.traverse(/*recursive=*/ false, [&](const auto & node)
    {
        active_parts.emplace_back(node);
    });

    num_parts += active_parts.size();

    auto part_lock = lockParts();
    LOG_TEST(log, "loadDataParts: clearing data_parts_indexes (had {} parts)", data_parts_indexes.size());
    data_parts_indexes.clear();

    MutableDataPartsVector broken_parts_to_detach;
    MutableDataPartsVector duplicate_parts_to_remove;

    size_t suspicious_broken_parts = 0;
    size_t suspicious_broken_parts_bytes = 0;
    size_t suspicious_broken_unexpected_parts = 0;
    size_t suspicious_broken_unexpected_parts_bytes = 0;
    bool have_adaptive_parts = false;
    bool have_non_adaptive_parts = false;
    bool have_lightweight_in_parts = false;
    bool have_parts_with_version_metadata = false;

    bool is_static_storage = isStaticStorage();

    if (num_parts > 0)
    {
        auto loaded_parts = loadDataPartsFromDisk(active_parts);

        for (const auto & res : loaded_parts)
        {
            if (res.is_broken)
            {
                broken_parts_to_detach.push_back(res.part);
                bool unexpected = expected_parts != std::nullopt && !expected_parts->contains(res.part->name);
                if (unexpected)
                {
                    LOG_DEBUG(log, "loadDataParts: Part {} is broken, but it's not expected to be in parts set, "
                              " will not count it as suspicious broken part", res.part->name);
                    ++suspicious_broken_unexpected_parts;
                }
                else
                    ++suspicious_broken_parts;

                if (res.size_of_part)
                {
                    if (unexpected)
                        suspicious_broken_unexpected_parts_bytes += *res.size_of_part;
                    else
                        suspicious_broken_parts_bytes += *res.size_of_part;
                }
            }
            else if (res.part->is_duplicate)
            {
                if (!is_static_storage)
                    res.part->remove();
            }
            else
            {
                bool is_adaptive = res.part->index_granularity_info.mark_type.adaptive;
                have_adaptive_parts |= is_adaptive;
                have_non_adaptive_parts |= !is_adaptive;
                have_lightweight_in_parts |= res.part->hasLightweightDelete();
                have_parts_with_version_metadata |= res.part->wasInvolvedInTransaction();
            }
        }
    }

    if (num_parts == 0 && unexpected_parts_to_load.empty())
    {
        resetObjectColumnsFromActiveParts(part_lock);
        resetSerializationHints(part_lock);
        LOG_DEBUG(log, "There are no data parts");
        return;
    }

    if (have_non_adaptive_parts && have_adaptive_parts && !(*settings)[MergeTreeSetting::enable_mixed_granularity_parts])
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Table contains parts with adaptive and non adaptive marks, "
                        "but `setting enable_mixed_granularity_parts` is disabled");

    has_non_adaptive_index_granularity_parts = have_non_adaptive_parts;
    has_lightweight_delete_parts = have_lightweight_in_parts;
    transactions_enabled = have_parts_with_version_metadata;

    if (!skip_sanity_checks)
    {
        if (suspicious_broken_parts > (*settings)[MergeTreeSetting::max_suspicious_broken_parts])
            throw Exception(
                ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS,
                "Suspiciously many ({} parts, {} in total) broken parts "
                "to remove while maximum allowed broken parts count is {}. You can change the maximum value "
                "with merge tree setting 'max_suspicious_broken_parts' in <merge_tree> configuration section or in table settings in .sql file "
                "(don't forget to return setting back to default value)",
                suspicious_broken_parts,
                formatReadableSizeWithBinarySuffix(suspicious_broken_parts_bytes),
                (*settings)[MergeTreeSetting::max_suspicious_broken_parts]);

        if (suspicious_broken_parts_bytes > (*settings)[MergeTreeSetting::max_suspicious_broken_parts_bytes])
            throw Exception(
                ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS,
                "Suspiciously big size ({} parts, {} in total) of all broken "
                "parts to remove while maximum allowed broken parts size is {}. "
                "You can change the maximum value with merge tree setting 'max_suspicious_broken_parts_bytes' in <merge_tree> configuration "
                "section or in table settings in .sql file (don't forget to return setting back to default value)",
                suspicious_broken_parts,
                formatReadableSizeWithBinarySuffix(suspicious_broken_parts_bytes),
                formatReadableSizeWithBinarySuffix((*settings)[MergeTreeSetting::max_suspicious_broken_parts_bytes]));
    }

    if (suspicious_broken_unexpected_parts != 0)
        LOG_WARNING(log, "Found suspicious broken unexpected parts {} with total rows count {}", suspicious_broken_unexpected_parts, suspicious_broken_unexpected_parts_bytes);

    if (!is_static_storage)
        for (auto & part : broken_parts_to_detach)
            part->renameToDetached("broken-on-start"); /// detached parts must not have '_' in prefixes

    resetObjectColumnsFromActiveParts(part_lock);
    resetSerializationHints(part_lock);
    calculateColumnAndSecondaryIndexSizesImpl();

    PartLoadingTreeNodes unloaded_parts;

    std::vector<UnexpectedPartLoadState> unexpected_unloaded_data_parts;
    for (const auto & [info, name, disk] : unexpected_parts_to_load)
    {
        bool uncovered = true;
        for (const auto & part : unexpected_parts_to_load)
        {
            if (name != part.name && part.info.contains(info))
            {
                uncovered = false;
                break;
            }
        }
        unexpected_unloaded_data_parts.push_back({std::make_shared<PartLoadingTree::Node>(info, name, disk), uncovered, /*is_broken*/ false, /*part*/ nullptr});
    }

    if (!unexpected_unloaded_data_parts.empty())
    {
        LOG_DEBUG(log, "Found {} unexpected data parts. They will be loaded asynchronously", unexpected_unloaded_data_parts.size());
        {
            std::lock_guard lock(unexpected_data_parts_mutex);
            unexpected_data_parts = std::move(unexpected_unloaded_data_parts);
            unexpected_data_parts_loading_finished = false;
        }

        unexpected_data_parts_loading_task = getContext()->getSchedulePool().createTask(
            "MergeTreeData::loadUnexpectedDataParts",
            [this] { loadUnexpectedDataParts(); });
    }

    loading_tree.traverse(/*recursive=*/ true, [&](const auto & node)
    {
        if (!node->is_loaded)
            unloaded_parts.push_back(node);
    });

    /// By the way, if all disks are readonly, it does not make sense to load outdated parts (we will not own them).
    if (!unloaded_parts.empty() && !all_disks_are_readonly)
    {
        LOG_DEBUG(log, "Found {} outdated data parts. They will be loaded asynchronously", unloaded_parts.size());

        {
            std::lock_guard lock(outdated_data_parts_mutex);
            outdated_unloaded_data_parts = std::move(unloaded_parts);
            outdated_data_parts_loading_finished = false;
        }

        outdated_data_parts_loading_task = getContext()->getSchedulePool().createTask(
            "MergeTreeData::loadOutdatedDataParts",
            [this] { loadOutdatedDataParts(/*is_async=*/ true); });
    }

    watch.stop();
    LOG_DEBUG(log, "Loaded data parts ({} items) took {} seconds", data_parts_indexes.size(), watch.elapsedSeconds());
    ProfileEvents::increment(ProfileEvents::LoadedDataParts, data_parts_indexes.size());
    ProfileEvents::increment(ProfileEvents::LoadedDataPartsMicroseconds, watch.elapsedMicroseconds());
    data_parts_loading_finished = true;
}

void MergeTreeData::loadUnexpectedDataParts()
try
{
    {
        std::lock_guard lock(unexpected_data_parts_mutex);
        if (unexpected_data_parts.empty())
        {
            unexpected_data_parts_loading_finished = true;
            unexpected_data_parts_cv.notify_all();
            return;
        }

        LOG_DEBUG(log, "Loading {} unexpected data parts",
            unexpected_data_parts.size());
    }

    ThreadFuzzer::maybeInjectSleep();

    auto blocker = CannotAllocateThreadFaultInjector::blockFaultInjections();

    ThreadPoolCallbackRunnerLocal<void> runner(getUnexpectedPartsLoadingThreadPool().get(), "UnexpectedParts");

    for (auto & load_state : unexpected_data_parts)
    {
        std::lock_guard lock(unexpected_data_parts_mutex);
        chassert(!load_state.part);
        if (unexpected_data_parts_loading_canceled)
        {
            runner.waitForAllToFinishAndRethrowFirstError();
            return;
        }
        runner([&]()
        {
            loadUnexpectedDataPart(load_state);

            chassert(load_state.part);
            if (load_state.is_broken)
            {
                load_state.part->renameToDetached("broken-on-start"); /// detached parts must not have '_' in prefixes
            }
        }, Priority{});
    }
    runner.waitForAllToFinishAndRethrowFirstError();
    LOG_DEBUG(log, "Loaded {} unexpected data parts", unexpected_data_parts.size());

    {
        std::lock_guard lock(unexpected_data_parts_mutex);
        unexpected_data_parts_loading_finished = true;
        unexpected_data_parts_cv.notify_all();
    }
}
catch (...)
{
    LOG_ERROR(log, "Loading of unexpected parts failed. "
        "Will terminate to avoid undefined behaviour due to inconsistent set of parts. "
        "Exception: {}", getCurrentExceptionMessage(true));
    std::terminate();
}

void MergeTreeData::loadOutdatedDataParts(bool is_async)
try
{
    {
        std::lock_guard lock(outdated_data_parts_mutex);
        if (outdated_unloaded_data_parts.empty())
        {
            outdated_data_parts_loading_finished = true;
            outdated_data_parts_cv.notify_all();
            return;
        }

        LOG_DEBUG(log, "Loading {} outdated data parts {}",
            outdated_unloaded_data_parts.size(),
            is_async ? "asynchronously" : "synchronously");
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<size_t>((*getSettings())[MergeTreeSetting::sleep_before_loading_outdated_parts_ms])));
    ThreadFuzzer::maybeInjectSleep();

    /// Acquire shared lock because 'relative_data_path' is used while loading parts.
    TableLockHolder shared_lock;
    if (is_async)
        shared_lock = lockForShare(RWLockImpl::NO_QUERY, (*getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);

    std::atomic_size_t num_loaded_parts = 0;

    auto blocker = CannotAllocateThreadFaultInjector::blockFaultInjections();

    ThreadPoolCallbackRunnerLocal<void> runner(getOutdatedPartsLoadingThreadPool().get(), "OutdatedParts");

    while (true)
    {
        ThreadFuzzer::maybeInjectSleep();
        PartLoadingTree::NodePtr part;

        {
            std::lock_guard lock(outdated_data_parts_mutex);

            if (is_async && outdated_data_parts_loading_canceled)
            {
                /// Wait for every scheduled task
                /// In case of any exception it will be re-thrown and server will be terminated.
                runner.waitForAllToFinishAndRethrowFirstError();

                LOG_DEBUG(log,
                    "Stopped loading outdated data parts because task was canceled. "
                    "Loaded {} parts, {} left unloaded", num_loaded_parts, outdated_unloaded_data_parts.size());
                return;
            }

            if (outdated_unloaded_data_parts.empty())
                break;

            part = outdated_unloaded_data_parts.back();
            outdated_unloaded_data_parts.pop_back();
        }

        runner([&, my_part = part]()
        {
            auto blocker_for_runner_thread = CannotAllocateThreadFaultInjector::blockFaultInjections();

            auto res = loadDataPartWithRetries(
            my_part->info, my_part->name, my_part->disk,
            DataPartState::Outdated, data_parts_mutex, loading_parts_initial_backoff_ms,
            loading_parts_max_backoff_ms, loading_parts_max_tries);

            ++num_loaded_parts;
            if (res.is_broken)
            {
                forcefullyRemoveBrokenOutdatedPartFromZooKeeperBeforeDetaching(res.part->name);
                res.part->renameToDetached("broken-on-start"); /// detached parts must not have '_' in prefixes
            }
            else if (res.part->is_duplicate)
                res.part->remove();
            else
                preparePartForRemoval(res.part);
        }, Priority{});
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    LOG_DEBUG(log, "Loaded {} outdated data parts {}",
        num_loaded_parts, is_async ? "asynchronously" : "synchronously");

    {
        std::lock_guard lock(outdated_data_parts_mutex);
        outdated_data_parts_loading_finished = true;
        outdated_data_parts_cv.notify_all();
    }
}
catch (...)
{
    LOG_ERROR(log, "Loading of outdated parts failed. "
        "Will terminate to avoid undefined behaviour due to inconsistent set of parts. "
        "Exception: {}", getCurrentExceptionMessage(true));
    std::terminate();
}

/// No TSA because of std::unique_lock and std::condition_variable.
void MergeTreeData::waitForOutdatedPartsToBeLoaded() const TSA_NO_THREAD_SAFETY_ANALYSIS
{
    /// Background tasks are not run if storage is static.
    if (isStaticStorage())
        return;

    /// If waiting is not required, do NOT log and do NOT enable/disable turbo mode to make `waitForOutdatedPartsToBeLoaded` a lightweight check
    {
        std::unique_lock lock(outdated_data_parts_mutex);
        if (outdated_data_parts_loading_canceled)
            throw Exception(ErrorCodes::NOT_INITIALIZED, "Loading of outdated data parts was already canceled");
        if (outdated_data_parts_loading_finished)
            return;
    }

    /// We need to load parts as fast as possible
    getOutdatedPartsLoadingThreadPool().enableTurboMode();
    SCOPE_EXIT({
        /// Let's lower the number of threads e.g. for later ATTACH queries to behave as usual
        getOutdatedPartsLoadingThreadPool().disableTurboMode();
    });

    LOG_TRACE(log, "Will wait for outdated data parts to be loaded");

    std::unique_lock lock(outdated_data_parts_mutex);

    outdated_data_parts_cv.wait(lock, [this]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return outdated_data_parts_loading_finished || outdated_data_parts_loading_canceled;
    });

    if (outdated_data_parts_loading_canceled)
        throw Exception(ErrorCodes::NOT_INITIALIZED, "Loading of outdated data parts was canceled");

    LOG_TRACE(log, "Finished waiting for outdated data parts to be loaded");
}

void MergeTreeData::waitForUnexpectedPartsToBeLoaded() const TSA_NO_THREAD_SAFETY_ANALYSIS
{
    /// Background tasks are not run if storage is static.
    if (isStaticStorage())
        return;

    /// If waiting is not required, do NOT log and do NOT enable/disable turbo mode to make `waitForUnexpectedPartsToBeLoaded` a lightweight check
    {
        std::unique_lock lock(unexpected_data_parts_mutex);
        if (unexpected_data_parts_loading_canceled)
            throw Exception(ErrorCodes::NOT_INITIALIZED, "Loading of unexpected data parts was already canceled");
        if (unexpected_data_parts_loading_finished)
            return;
    }

    /// We need to load parts as fast as possible
    getUnexpectedPartsLoadingThreadPool().enableTurboMode();
    SCOPE_EXIT({
        /// Let's lower the number of threads e.g. for later ATTACH queries to behave as usual
        getUnexpectedPartsLoadingThreadPool().disableTurboMode();
    });

    LOG_TRACE(log, "Will wait for unexpected data parts to be loaded");

    std::unique_lock lock(unexpected_data_parts_mutex);

    unexpected_data_parts_cv.wait(lock, [this]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return unexpected_data_parts_loading_finished || unexpected_data_parts_loading_canceled;
    });

    if (unexpected_data_parts_loading_canceled)
        throw Exception(ErrorCodes::NOT_INITIALIZED, "Loading of unexpected data parts was canceled");

    LOG_TRACE(log, "Finished waiting for unexpected data parts to be loaded");
}

void MergeTreeData::startOutdatedAndUnexpectedDataPartsLoadingTask()
{
    if (outdated_data_parts_loading_task)
        outdated_data_parts_loading_task->activateAndSchedule();
    if (unexpected_data_parts_loading_task)
        unexpected_data_parts_loading_task->activateAndSchedule();
}

void MergeTreeData::stopOutdatedAndUnexpectedDataPartsLoadingTask()
{
    if (outdated_data_parts_loading_task)
    {
        {
            std::lock_guard lock(outdated_data_parts_mutex);
            outdated_data_parts_loading_canceled = true;
        }

        outdated_data_parts_loading_task->deactivate();
        outdated_data_parts_cv.notify_all();
    }

    if (unexpected_data_parts_loading_task)
    {
        {
            std::lock_guard lock(unexpected_data_parts_mutex);
            unexpected_data_parts_loading_canceled = true;
        }

        unexpected_data_parts_loading_task->deactivate();
        unexpected_data_parts_cv.notify_all();
    }
}

void MergeTreeData::prewarmMarkCacheIfNeeded(ThreadPool & pool)
{
    if (!(*getSettings())[MergeTreeSetting::prewarm_mark_cache])
        return;

    prewarmMarkCache(pool);
}

void MergeTreeData::prewarmMarkCache(ThreadPool & pool)
{
    auto * mark_cache = getContext()->getMarkCache().get();
    if (!mark_cache)
        return;

    auto metadata_snaphost = getInMemoryMetadataPtr();
    auto column_names = getColumnsToPrewarmMarks(*getSettings(), metadata_snaphost->getColumns().getAllPhysical());

    if (column_names.empty())
        return;

    Stopwatch watch;
    LOG_TRACE(log, "Prewarming mark cache");

    auto data_parts = getDataPartsVectorForInternalUsage();

    /// Prewarm mark cache firstly for the most fresh parts according
    /// to time columns in partition key (if exists) and by modification time.

    auto to_tuple = [](const auto & part)
    {
        return std::make_tuple(part->getMinMaxDate().second, part->getMinMaxTime().second, part->modification_time);
    };

    std::sort(data_parts.begin(), data_parts.end(), [&to_tuple](const auto & lhs, const auto & rhs)
    {
        return to_tuple(lhs) > to_tuple(rhs);
    });

    ThreadPoolCallbackRunnerLocal<void> runner(pool, "PrewarmMarks");
    double ratio_to_prewarm = getContext()->getServerSettings()[ServerSetting::mark_cache_prewarm_ratio];

    for (const auto & part : data_parts)
    {
        if (mark_cache->sizeInBytes() >= mark_cache->maxSizeInBytes() * ratio_to_prewarm)
            break;

        runner([&] { part->loadMarksToCache(column_names, mark_cache); });
    }

    runner.waitForAllToFinishAndRethrowFirstError();
    watch.stop();
    LOG_TRACE(log, "Prewarmed mark cache in {} seconds", watch.elapsedSeconds());
}

/// Is the part directory old.
/// True if its modification time and the modification time of all files inside it is less then threshold.
/// (Only files on the first level of nesting are considered).
static bool isOldPartDirectory(const DiskPtr & disk, const String & directory_path, time_t threshold)
{
    if (!disk->existsDirectory(directory_path) || disk->getLastModified(directory_path).epochTime() > threshold)
        return false;

    for (auto it = disk->iterateDirectory(directory_path); it->isValid(); it->next())
        if (disk->getLastModified(it->path()).epochTime() > threshold)
            return false;

    return true;
}


size_t MergeTreeData::clearOldTemporaryDirectories(size_t custom_directories_lifetime_seconds, const NameSet & valid_prefixes)
{
    size_t cleared_count = 0;

    cleared_count += clearOldTemporaryDirectories(relative_data_path, custom_directories_lifetime_seconds, valid_prefixes);

    if (allowRemoveStaleMovingParts())
    {
        /// Clear _all_ parts from the `moving` directory
        cleared_count += clearOldTemporaryDirectories(fs::path(relative_data_path) / "moving", custom_directories_lifetime_seconds, {""});
    }

    return cleared_count;
}

size_t MergeTreeData::clearOldTemporaryDirectories(const String & root_path, size_t custom_directories_lifetime_seconds, const NameSet & valid_prefixes)
{
    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock lock(clear_old_temporary_directories_mutex, std::defer_lock);
    if (!lock.try_lock())
        return 0;

    const auto settings = getSettings();
    time_t current_time = time(nullptr);
    ssize_t deadline = current_time - custom_directories_lifetime_seconds;

    size_t cleared_count = 0;

    /// Delete temporary directories older than a the specified age.
    for (const auto & disk : getDisks())
    {
        if (disk->isBroken())
            continue;

        if (!disk->existsDirectory(root_path))
            continue;

        for (auto it = disk->iterateDirectory(root_path); it->isValid(); it->next())
        {
            const std::string & basename = it->name();
            bool start_with_valid_prefix = false;
            for (const auto & prefix : valid_prefixes)
            {
                if (startsWith(basename, prefix))
                {
                    start_with_valid_prefix = true;
                    break;
                }
            }

            if (!start_with_valid_prefix)
                continue;

            const std::string & full_path = fullPath(disk, it->path());

            try
            {
                if (isOldPartDirectory(disk, it->path(), deadline))
                {
                    ThreadFuzzer::maybeInjectSleep();

                    if (temporary_parts.contains(basename))
                    {
                        /// Actually we don't rely on temporary_directories_lifetime when removing old temporaries directories,
                        /// it's just an extra level of protection just in case we have a bug.
                        LOG_INFO(LogFrequencyLimiter(log.load(), 10), "{} is in use (by merge/mutation/INSERT) (consider increasing temporary_directories_lifetime setting)", full_path);
                        continue;
                    }
                    if (!disk->existsDirectory(it->path()))
                    {
                        /// We should recheck that the dir exists, otherwise we can get "No such file or directory"
                        /// due to a race condition with "Renaming temporary part" (temporary part holder could be already released, so the check above is not enough)
                        LOG_WARNING(log, "Temporary directory {} suddenly disappeared while iterating, assuming it was concurrently renamed to persistent", it->path());
                        continue;
                    }

                    LOG_WARNING(log, "Removing temporary directory {}", full_path);

                    /// Even if it's a temporary part it could be downloaded with zero copy replication and this function
                    /// is executed as a callback.
                    ///
                    /// We don't control the amount of refs for temporary parts so we cannot decide can we remove blobs
                    /// or not. So we are not doing it
                    bool keep_shared = false;
                    if (disk->supportZeroCopyReplication() && (*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication] && supportsReplication())
                    {
                        LOG_WARNING(log, "Since zero-copy replication is enabled we are not going to remove blobs from shared storage for {}", full_path);
                        keep_shared = true;
                    }

                    disk->removeSharedRecursive(it->path(), keep_shared, {});
                    ++cleared_count;
                }
            }
            catch (const fs::filesystem_error & e)
            {
                if (e.code() == std::errc::no_such_file_or_directory)
                {
                    /// If the file is already deleted, do nothing.
                }
                else
                    throw;
            }
        }
    }

    return cleared_count;
}

scope_guard MergeTreeData::getTemporaryPartDirectoryHolder(const String & part_dir_name) const
{
    temporary_parts.add(part_dir_name);
    return [this, part_dir_name]() { temporary_parts.remove(part_dir_name); };
}

MergeTreeData::MutableDataPartPtr MergeTreeData::asMutableDeletingPart(const DataPartPtr & part)
{
    auto state = part->getState();
    if (state != DataPartState::Deleting && state != DataPartState::DeleteOnDestroy)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot remove part {}, because it has state: {}", part->name, magic_enum::enum_name(state));

    return std::const_pointer_cast<IMergeTreeDataPart>(part);
}

MergeTreeData::DataPartsVector MergeTreeData::grabOldParts(bool force)
{
    DataPartsVector res;

    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock lock(grab_old_parts_mutex, std::defer_lock);
    if (!lock.try_lock())
        return res;

    /// Concurrent parts removal is disabled for "zero-copy replication" (a non-production feature),
    /// because parts removal involves hard links and concurrent hard link operations don't work correctly
    /// in the "zero-copy replication" (because it is a non-production feature).
    /// Please don't use "zero-copy replication" (a non-production feature) in production.
    /// It is not ready for production usage. Don't use it.

    bool need_remove_parts_in_order = supportsReplication() && (*getSettings())[MergeTreeSetting::allow_remote_fs_zero_copy_replication];

    if (need_remove_parts_in_order)
    {
        bool has_zero_copy_disk = false;
        for (const auto & disk : getDisks())
        {
            if (disk->supportZeroCopyReplication())
            {
                has_zero_copy_disk = true;
                break;
            }
        }
        need_remove_parts_in_order = has_zero_copy_disk;
    }

    std::vector<DataPartIteratorByStateAndInfo> parts_to_delete;
    std::vector<MergeTreePartInfo> skipped_parts;

    auto has_skipped_mutation_parent = [&skipped_parts, need_remove_parts_in_order] (const DataPartPtr & part)
    {
        if (!need_remove_parts_in_order)
            return false;

        for (const auto & part_info : skipped_parts)
            if (part->info.isMutationChildOf(part_info))
                return true;

        return false;
    };

    auto time_now = time(nullptr);

    {
        auto removal_limit = (*getSettings())[MergeTreeSetting::simultaneous_parts_removal_limit];
        size_t current_removal_limit = removal_limit == 0 ? std::numeric_limits<size_t>::max() : static_cast<size_t>(removal_limit);

        auto parts_lock = lockParts();

        auto outdated_parts_range = getDataPartsStateRange(DataPartState::Outdated);
        for (auto it = outdated_parts_range.begin(); it != outdated_parts_range.end(); ++it)
        {
            if (parts_to_delete.size() == current_removal_limit)
            {
                LOG_TRACE(log, "Found {} parts to remove and reached the limit for one removal iteration", current_removal_limit);
                break;
            }

            const DataPartPtr & part = *it;

            part->last_removal_attempt_time.store(time_now, std::memory_order_relaxed);

            /// Do not remove outdated part if it may be visible for some transaction
            if (!part->version.canBeRemoved())
            {
                part->removal_state.store(DataPartRemovalState::VISIBLE_TO_TRANSACTIONS, std::memory_order_relaxed);
                skipped_parts.push_back(part->info);
                continue;
            }

            /// Grab only parts that are not used by anyone (SELECTs for example).
            if (!isSharedPtrUnique(part))
            {
                part->removal_state.store(DataPartRemovalState::NON_UNIQUE_OWNERSHIP, std::memory_order_relaxed);
                skipped_parts.push_back(part->info);
                continue;
            }

            /// First remove all covered parts, then remove covering empty part
            /// Avoids resurrection of old parts for MergeTree and issues with unexpected parts for Replicated
            if (part->rows_count == 0 && !getCoveredOutdatedParts(part, parts_lock).empty())
            {
                part->removal_state.store(DataPartRemovalState::EMPTY_PART_COVERS_OTHER_PARTS, std::memory_order_relaxed);
                skipped_parts.push_back(part->info);
                continue;
            }

            auto part_remove_time = part->remove_time.load(std::memory_order_relaxed);
            bool reached_removal_time = part_remove_time <= time_now && time_now - part_remove_time >= (*getSettings())[MergeTreeSetting::old_parts_lifetime].totalSeconds();
            if ((reached_removal_time && !has_skipped_mutation_parent(part))
                || force
                || (part->version.creation_csn == Tx::RolledBackCSN && (*getSettings())[MergeTreeSetting::remove_rolled_back_parts_immediately]))
            {
                part->removal_state.store(DataPartRemovalState::REMOVED, std::memory_order_relaxed);
                parts_to_delete.emplace_back(it);
            }
            else
            {
                if (!reached_removal_time)
                    part->removal_state.store(DataPartRemovalState::NOT_REACHED_REMOVAL_TIME, std::memory_order_relaxed);
                else
                    part->removal_state.store(DataPartRemovalState::HAS_SKIPPED_MUTATION_PARENT, std::memory_order_relaxed);
                skipped_parts.push_back(part->info);
                continue;
            }
        }

        res.reserve(parts_to_delete.size());
        for (const auto & it_to_delete : parts_to_delete)
        {
            res.emplace_back(*it_to_delete);
            modifyPartState(it_to_delete, DataPartState::Deleting);
        }
    }

    if (!res.empty())
        LOG_TRACE(log, "Found {} old parts to remove. Parts: [{}]",
                  res.size(), fmt::join(getPartsNames(res), ", "));

    return res;
}


void MergeTreeData::rollbackDeletingParts(const MergeTreeData::DataPartsVector & parts)
{
    auto lock = lockParts();
    for (const auto & part : parts)
    {
        /// We should modify it under data_parts_mutex
        part->assertState({DataPartState::Deleting});
        modifyPartState(part, DataPartState::Outdated);
    }
}

void MergeTreeData::removePartsFinally(const MergeTreeData::DataPartsVector & parts)
{
    if (parts.empty())
        return;

    {
        auto lock = lockParts();

        /// TODO: use data_parts iterators instead of pointers
        for (const auto & part : parts)
        {
            /// Temporary does not present in data_parts_by_info.
            if (part->getState() == DataPartState::Temporary)
                continue;

            auto it = data_parts_by_info.find(part->info);
            if (it == data_parts_by_info.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Deleting data part {} doesn't exist", part->name);

            (*it)->assertState({DataPartState::Deleting});

            LOG_TEST(log, "removePartsFinally: removing {} from data_parts_indexes", (*it)->getNameWithState());
            data_parts_indexes.erase(it);
        }
    }

    LOG_DEBUG(log, "Removing {} parts from memory: Parts: [{}]", parts.size(), fmt::join(parts, ", "));

    /// Data parts is still alive (since DataPartsVector holds shared_ptrs) and contain useful metainformation for logging
    /// NOTE: There is no need to log parts deletion somewhere else, all deleting parts pass through this function and pass away

    auto table_id = getStorageID();
    if (auto part_log = getContext()->getPartLog(table_id.database_name))
    {
        PartLogElement part_log_elem;

        part_log_elem.event_type = PartLogElement::REMOVE_PART;

        const auto time_now = std::chrono::system_clock::now();
        part_log_elem.event_time = timeInSeconds(time_now);
        part_log_elem.event_time_microseconds = timeInMicroseconds(time_now);

        part_log_elem.duration_ms = 0;

        part_log_elem.database_name = table_id.database_name;
        part_log_elem.table_name = table_id.table_name;
        part_log_elem.table_uuid = table_id.uuid;

        for (const auto & part : parts)
        {
            part_log_elem.partition_id = part->info.partition_id;
            {
                WriteBufferFromString out(part_log_elem.partition);
                part->partition.serializeText(part->storage, out, {});
            }
            part_log_elem.part_name = part->name;
            part_log_elem.bytes_compressed_on_disk = part->getBytesOnDisk();
            part_log_elem.bytes_uncompressed = part->getBytesUncompressedOnDisk();
            part_log_elem.rows = part->rows_count;
            part_log_elem.part_type = part->getType();

            part_log->add(part_log_elem);
        }
    }
}

size_t MergeTreeData::clearOldPartsFromFilesystem(bool force)
{
    DataPartsVector parts_to_remove = grabOldParts(force);
    if (parts_to_remove.empty())
        return 0;

    clearPartsFromFilesystem(parts_to_remove);
    removePartsFinally(parts_to_remove);
    /// This is needed to close files to avoid they reside on disk after being deleted.
    /// NOTE: we can drop files from cache more selectively but this is good enough.
    getContext()->clearMMappedFileCache();

    return parts_to_remove.size();
}


void MergeTreeData::clearPartsFromFilesystem(const DataPartsVector & parts, bool throw_on_error, NameSet * parts_failed_to_delete)
{
    NameSet part_names_succeed;

    auto get_failed_parts = [&part_names_succeed, &parts_failed_to_delete, &parts] ()
    {
        if (part_names_succeed.size() == parts.size())
            return;

        if (parts_failed_to_delete)
        {
            for (const auto & part : parts)
            {
                if (!part_names_succeed.contains(part->name))
                    parts_failed_to_delete->insert(part->name);
            }
        }
    };

    try
    {
        clearPartsFromFilesystemImpl(parts, &part_names_succeed);
        get_failed_parts();
    }
    catch (...)
    {
        get_failed_parts();

        LOG_DEBUG(log, "Failed to remove all parts, all count {}, removed {}", parts.size(), part_names_succeed.size());

        if (throw_on_error)
            throw;
    }
}

void MergeTreeData::clearPartsFromFilesystemImpl(const DataPartsVector & parts_to_remove, NameSet * part_names_succeed)
{
    if (parts_to_remove.empty())
        return;

    const auto settings = getSettings();

    auto remove_single_thread = [this, &parts_to_remove, part_names_succeed]()
    {
        LOG_DEBUG(
            log, "Removing {} parts from filesystem (serially): Parts: [{}]", parts_to_remove.size(), fmt::join(parts_to_remove, ", "));
        for (const DataPartPtr & part : parts_to_remove)
        {
            asMutableDeletingPart(part)->remove();
            if (part_names_succeed)
                part_names_succeed->insert(part->name);
        }
    };

    if (parts_to_remove.size() <= (*settings)[MergeTreeSetting::concurrent_part_removal_threshold])
    {
        remove_single_thread();
        return;
    }

    /// Parallel parts removal.
    std::mutex part_names_mutex;

    /// This flag disallow straightforward concurrent parts removal. It's required only in case
    /// when we have parts on zero-copy disk + at least some of them were mutated.
    bool remove_parts_in_order = false;
    if ((*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication] && dynamic_cast<StorageReplicatedMergeTree *>(this) != nullptr)
    {
        remove_parts_in_order = std::any_of(
            parts_to_remove.begin(), parts_to_remove.end(),
            [] (const auto & data_part) { return data_part->isStoredOnRemoteDiskWithZeroCopySupport() && data_part->info.getMutationVersion() > 0; }
        );
    }


    if (!remove_parts_in_order)
    {
        /// NOTE: Under heavy system load you may get "Cannot schedule a task" from ThreadPool.
        LOG_DEBUG(
            log, "Removing {} parts from filesystem (concurrently): Parts: [{}]", parts_to_remove.size(), fmt::join(parts_to_remove, ", "));

        ThreadPoolCallbackRunnerLocal<void> runner(getPartsCleaningThreadPool().get(), "PartsCleaning");

        for (const DataPartPtr & part : parts_to_remove)
        {
            runner([&part, &part_names_mutex, part_names_succeed, thread_group = CurrentThread::getGroup()]
            {
                asMutableDeletingPart(part)->remove();
                if (part_names_succeed)
                {
                    std::lock_guard lock(part_names_mutex);
                    part_names_succeed->insert(part->name);
                }
            }, Priority{0});
        }

        runner.waitForAllToFinishAndRethrowFirstError();

        return;
    }

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        remove_single_thread();
        return;
    }

    /// NOTE: Under heavy system load you may get "Cannot schedule a task" from ThreadPool.
    LOG_DEBUG(
        log, "Removing {} parts from filesystem (concurrently): Parts: [{}]", parts_to_remove.size(), fmt::join(parts_to_remove, ", "));

    /// We have "zero copy replication" parts and we are going to remove them in parallel.
    /// The problem is that all parts in a mutation chain must be removed sequentially to avoid "key does not exits" issues.
    /// We remove disjoint subsets of parts in parallel.
    /// The problem is that it's not trivial to divide Outdated parts into disjoint subsets,
    /// because Outdated parts legally can be intersecting (but intersecting parts must be separated by a DROP_RANGE).
    /// So we ignore level and version and use block numbers only (they cannot intersect by block numbers unless we have a bug).

    struct RemovalRanges
    {
        std::vector<MergeTreePartInfo> infos;
        std::vector<DataPartsVector> parts;
        std::vector<UInt64> split_times;
    };

    auto split_into_independent_ranges = [this](const DataPartsVector & parts_to_remove_, size_t split_times) -> RemovalRanges
    {
        if (parts_to_remove_.empty())
            return {};

        ActiveDataPartSet independent_ranges_set(format_version);
        for (const auto & part : parts_to_remove_)
        {
            MergeTreePartInfo range_info = part->info;
            range_info.level = static_cast<UInt32>(range_info.max_block - range_info.min_block);
            range_info.mutation = 0;
            independent_ranges_set.add(range_info, range_info.getPartNameV1());
        }

        RemovalRanges independent_ranges;
        independent_ranges.infos = independent_ranges_set.getPartInfos();
        size_t num_ranges = independent_ranges.infos.size();
        independent_ranges.parts.resize(num_ranges);
        independent_ranges.split_times.resize(num_ranges, split_times);
        size_t avg_range_size = parts_to_remove_.size() / num_ranges;

        size_t sum_of_ranges = 0;
        for (size_t i = 0; i < num_ranges; ++i)
        {
            MergeTreePartInfo & range = independent_ranges.infos[i];
            DataPartsVector & parts_in_range = independent_ranges.parts[i];
            range.level = MergeTreePartInfo::MAX_LEVEL;
            range.mutation = MergeTreePartInfo::MAX_BLOCK_NUMBER;

            parts_in_range.reserve(avg_range_size * 2);
            for (const auto & part : parts_to_remove_)
                if (range.contains(part->info))
                    parts_in_range.push_back(part);
            sum_of_ranges += parts_in_range.size();
        }

        if (parts_to_remove_.size() != sum_of_ranges)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of removed parts is not equal to number of parts in independent ranges "
                                                       "({} != {}), it's a bug", parts_to_remove_.size(), sum_of_ranges);

        return independent_ranges;
    };

    ThreadPoolCallbackRunnerLocal<void> runner(getPartsCleaningThreadPool().get(), "PartsCleaning");

    auto schedule_parts_removal = [this, &runner, &part_names_mutex, part_names_succeed](
        const MergeTreePartInfo & range, DataPartsVector && parts_in_range)
    {
        /// Below, range should be captured by copy to avoid use-after-scope on exception from pool
        runner(
            [this, range, &part_names_mutex, part_names_succeed, batch = std::move(parts_in_range)]
        {
            LOG_TRACE(log, "Removing {} parts in blocks range {}", batch.size(), range.getPartNameForLogs());

            for (const auto & part : batch)
            {
                asMutableDeletingPart(part)->remove();
                if (part_names_succeed)
                {
                    std::lock_guard lock(part_names_mutex);
                    part_names_succeed->insert(part->name);
                }
            }
        }, Priority{0});
    };

    RemovalRanges independent_ranges = split_into_independent_ranges(parts_to_remove, /* split_times */ 0);
    DataPartsVector excluded_parts;
    size_t num_ranges = independent_ranges.infos.size();
    size_t sum_of_ranges = 0;
    for (size_t i = 0; i < num_ranges; ++i)
    {
        MergeTreePartInfo & range = independent_ranges.infos[i];
        DataPartsVector & parts_in_range = independent_ranges.parts[i];
        UInt64 split_times = independent_ranges.split_times[i];

        /// It may happen that we have a huge part covering thousands small parts.
        /// In this case, we will get a huge range that will be process by only one thread causing really long tail latency.
        /// Let's try to exclude such parts in order to get smaller tasks for thread pool and more uniform distribution.
        if ((*settings)[MergeTreeSetting::concurrent_part_removal_threshold] < parts_in_range.size() &&
            split_times < (*settings)[MergeTreeSetting::zero_copy_concurrent_part_removal_max_split_times])
        {
            auto smaller_parts_pred = [&range](const DataPartPtr & part)
            {
                return !(part->info.min_block == range.min_block && part->info.max_block == range.max_block);
            };

            size_t covered_parts_count = std::count_if(parts_in_range.begin(), parts_in_range.end(), smaller_parts_pred);
            size_t top_level_count = parts_in_range.size() - covered_parts_count;
            chassert(top_level_count);
            Float32 parts_to_exclude_ratio = static_cast<Float32>(top_level_count) / parts_in_range.size();
            if ((*settings)[MergeTreeSetting::zero_copy_concurrent_part_removal_max_postpone_ratio] < parts_to_exclude_ratio)
            {
                /// Most likely we have a long mutations chain here
                LOG_DEBUG(log, "Block range {} contains {} parts including {} top-level parts, will not try to split it",
                          range.getPartNameForLogs(), parts_in_range.size(), top_level_count);
            }
            else
            {
                auto new_end_it = std::partition(parts_in_range.begin(), parts_in_range.end(), smaller_parts_pred);
                std::move(new_end_it, parts_in_range.end(), std::back_inserter(excluded_parts));
                parts_in_range.erase(new_end_it, parts_in_range.end());

                RemovalRanges subranges = split_into_independent_ranges(parts_in_range, split_times + 1);

                LOG_DEBUG(log, "Block range {} contained {} parts, it was split into {} independent subranges after excluding {} top-level parts",
                          range.getPartNameForLogs(), parts_in_range.size() + top_level_count, subranges.infos.size(), top_level_count);

                std::move(subranges.infos.begin(), subranges.infos.end(), std::back_inserter(independent_ranges.infos));
                std::move(subranges.parts.begin(), subranges.parts.end(), std::back_inserter(independent_ranges.parts));
                std::move(subranges.split_times.begin(), subranges.split_times.end(), std::back_inserter(independent_ranges.split_times));
                num_ranges += subranges.infos.size();
                continue;
            }
        }

        sum_of_ranges += parts_in_range.size();

        schedule_parts_removal(range, std::move(parts_in_range));
    }

    /// Remove excluded parts as well. They were reordered, so sort them again
    std::sort(excluded_parts.begin(), excluded_parts.end(), [](const auto & x, const auto & y) { return x->info < y->info; });
    LOG_TRACE(log, "Will remove {} big parts separately: {}", excluded_parts.size(), fmt::join(excluded_parts, ", "));

    independent_ranges = split_into_independent_ranges(excluded_parts, /* split_times */ 0);

    runner.waitForAllToFinishAndRethrowFirstError();

    for (size_t i = 0; i < independent_ranges.infos.size(); ++i)
    {
        MergeTreePartInfo & range = independent_ranges.infos[i];
        DataPartsVector & parts_in_range = independent_ranges.parts[i];
        schedule_parts_removal(range, std::move(parts_in_range));
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    if (parts_to_remove.size() != sum_of_ranges + excluded_parts.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Number of parts to remove was not equal to number of parts in independent ranges and excluded parts"
                        "({} != {} + {}), it's a bug", parts_to_remove.size(), sum_of_ranges, excluded_parts.size());
}


size_t MergeTreeData::clearEmptyParts()
{
    if (!(*getSettings())[MergeTreeSetting::remove_empty_parts])
        return 0;

    std::vector<std::string> parts_names_to_drop;

    {
        /// Need to destroy parts vector before clearing them from filesystem.
        auto parts = getDataPartsVectorForInternalUsage();
        for (const auto & part : parts)
        {
            if (part->rows_count != 0)
                continue;

            /// Do not try to drop uncommitted parts. If the newest tx doesn't see it then it probably hasn't been committed yet
            if (!part->version.getCreationTID().isPrehistoric() && !part->version.isVisible(TransactionLog::instance().getLatestSnapshot()))
                continue;

            parts_names_to_drop.emplace_back(part->name);
        }
    }

    for (auto & name : parts_names_to_drop)
    {
        LOG_INFO(log, "Will drop empty part {}", name);
        dropPartNoWaitNoThrow(name);
    }

    return parts_names_to_drop.size();
}

void MergeTreeData::rename(const String & new_table_path, const StorageID & new_table_id)
{
    LOG_INFO(log, "Renaming table to path {} with ID {}", new_table_path, new_table_id.getFullTableName());

    auto disks = getStoragePolicy()->getDisks();

    for (const auto & disk : disks)
    {
        if (disk->existsDirectory(new_table_path))
            throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Target path already exists: {}", fullPath(disk, new_table_path));
    }

    for (const auto & disk : disks)
    {
        auto new_table_path_parent = parentPath(new_table_path);
        disk->createDirectories(new_table_path_parent);
        disk->moveDirectory(relative_data_path, new_table_path);
    }

    if (!getStorageID().hasUUID())
        getContext()->clearCaches();

    /// TODO: remove const_cast
    for (const auto & part : data_parts_by_info)
    {
        auto & part_mutable = const_cast<IMergeTreeDataPart &>(*part);
        part_mutable.getDataPartStorage().changeRootPath(relative_data_path, new_table_path);
    }

    relative_data_path = new_table_path;
    renameInMemory(new_table_id);
}

void MergeTreeData::renameInMemory(const StorageID & new_table_id)
{
    IStorage::renameInMemory(new_table_id);
    log.store(new_table_id.getNameForLogs());
}

void MergeTreeData::dropAllData()
{
    /// In case there is read-only/write-once disk we cannot allow to call dropAllData(), but dropping tables is allowed.
    ///
    /// Note, that one may think that drop on write-once disk should be
    /// supported, since it is pretty trivial to implement
    /// MetadataStorageFromPlainObjectStorageTransaction::removeDirectory(),
    /// however removing part requires moveDirectory() as well.
    if (isStaticStorage())
        return;

    LOG_TRACE(log, "dropAllData: waiting for locks.");
    auto settings_ptr = getSettings();

    auto lock = lockParts();

    DataPartsVector all_parts;
    for (auto it = data_parts_by_info.begin(); it != data_parts_by_info.end(); ++it)
    {
        modifyPartState(it, DataPartState::Deleting);
        all_parts.push_back(*it);
    }

    /// Tables in atomic databases have UUID and stored in persistent locations.
    /// No need to clear caches (that are keyed by filesystem path) because collision is not possible.
    if (!getStorageID().hasUUID())
        getContext()->clearCaches();

    /// Removing of each data part before recursive removal of directory is to speed-up removal, because there will be less number of syscalls.
    NameSet part_names_failed;
    try
    {
        LOG_TRACE(log, "dropAllData: removing data parts (count {}) from filesystem.", all_parts.size());
        clearPartsFromFilesystem(all_parts, true, &part_names_failed);

        LOG_TRACE(log, "dropAllData: removing all data parts from memory.");
        data_parts_indexes.clear();
        all_data_dropped = true;
    }
    catch (...)
    {
        /// Removing from memory only successfully removed parts from disk
        /// Parts removal process can be important and on the next try it's better to try to remove
        /// them instead of remove recursive call.
        LOG_WARNING(log, "dropAllData: got exception removing parts from disk, removing successfully removed parts from memory.");
        for (const auto & part : all_parts)
        {
            if (!part_names_failed.contains(part->name))
                data_parts_indexes.erase(part->info);
        }

        throw;
    }

    LOG_INFO(log, "dropAllData: clearing temporary directories");
    clearOldTemporaryDirectories(0, {"tmp_", "delete_tmp_", "tmp-fetch_"});

    column_sizes.clear();

    auto detached_parts = getDetachedParts();
    for (const auto & part : detached_parts)
    {
        bool is_zero_copy = supportsReplication() && part.disk->supportZeroCopyReplication()
            && (*settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication];
        try
        {
            bool keep_shared = removeDetachedPart(part.disk, fs::path(relative_data_path) / DETACHED_DIR_NAME / part.dir_name / "", part.dir_name);
            LOG_DEBUG(log, "Dropped detached part {}, keep shared data: {}", part.dir_name, keep_shared);
        }
        catch (...)
        {
            /// Without zero-copy-replication we will simply remove it recursively, but with zero-copy it will leave garbage on s3
            if (is_zero_copy && isRetryableException(std::current_exception()))
                throw;
            tryLogCurrentException(log);
        }
    }

    for (const auto & disk : getDisks())
    {
        if (disk->isBroken())
            continue;

        /// It can naturally happen if we cannot drop table from the first time
        /// i.e. get exceptions after remove recursive
        if (!disk->existsDirectory(relative_data_path))
        {
            LOG_INFO(log, "dropAllData: path {} is already removed from disk {}", relative_data_path, disk->getName());
            continue;
        }

        LOG_INFO(log, "dropAllData: remove format_version.txt, detached, moving and write ahead logs");
        disk->removeFileIfExists(fs::path(relative_data_path) / FORMAT_VERSION_FILE_NAME);

        if (disk->existsDirectory(fs::path(relative_data_path) / DETACHED_DIR_NAME))
            disk->removeSharedRecursive(fs::path(relative_data_path) / DETACHED_DIR_NAME, /*keep_all_shared_data*/ true, {});

        if (disk->existsDirectory(fs::path(relative_data_path) / MOVING_DIR_NAME))
            disk->removeRecursive(fs::path(relative_data_path) / MOVING_DIR_NAME);

        try
        {
            if (!disk->isDirectoryEmpty(relative_data_path) &&
                supportsReplication() && disk->supportZeroCopyReplication()
                && (*settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
            {
                std::vector<std::string> files_left;
                disk->listFiles(relative_data_path, files_left);

                throw Exception(
                                ErrorCodes::ZERO_COPY_REPLICATION_ERROR,
                                "Directory {} with table {} not empty (files [{}]) after drop. Will not drop.",
                                relative_data_path, getStorageID().getNameForLogs(), fmt::join(files_left, ", "));
            }

            LOG_INFO(log, "dropAllData: removing table directory recursive to cleanup garbage");
            disk->removeRecursive(relative_data_path);
        }
        catch (const fs::filesystem_error & e)
        {
            if (e.code() == std::errc::no_such_file_or_directory)
            {
                /// If the file is already deleted, log the error message and do nothing.
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
            else
                throw;
        }
    }

    setDataVolume(0, 0, 0);

    LOG_TRACE(log, "dropAllData: done.");
}

void MergeTreeData::dropIfEmpty()
{
    auto lock = lockParts();

    if (!data_parts_by_info.empty())
        return;

    try
    {
        for (const auto & disk : getDisks())
        {
            if (disk->isBroken())
                continue;
            /// Non recursive, exception is thrown if there are more files.
            disk->removeFileIfExists(fs::path(relative_data_path) / FORMAT_VERSION_FILE_NAME);
            disk->removeDirectory(fs::path(relative_data_path) / DETACHED_DIR_NAME);
            disk->removeDirectory(relative_data_path);
        }
    }
    catch (...)
    {
        // On unsuccessful creation of ReplicatedMergeTree table with multidisk configuration some files may not exist.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

namespace
{

/// Conversion that is allowed for serializable key (primary key, sorting key).
/// Key should be serialized in the same way after conversion.
/// NOTE: The list is not complete.
bool isSafeForKeyConversion(const IDataType * from, const IDataType * to)
{
    if (from->getName() == to->getName())
        return true;

    /// Enums are serialized in partition key as numbers - so conversion from Enum to number is Ok.
    /// But only for types of identical width because they are serialized as binary in minmax index.
    /// But not from number to Enum because Enum does not necessarily represents all numbers.

    if (const auto * from_enum8 = typeid_cast<const DataTypeEnum8 *>(from))
    {
        if (const auto * to_enum8 = typeid_cast<const DataTypeEnum8 *>(to))
            return to_enum8->contains(*from_enum8);
        if (typeid_cast<const DataTypeInt8 *>(to))
            return true;    // NOLINT
        return false;
    }

    if (const auto * from_enum16 = typeid_cast<const DataTypeEnum16 *>(from))
    {
        if (const auto * to_enum16 = typeid_cast<const DataTypeEnum16 *>(to))
            return to_enum16->contains(*from_enum16);
        if (typeid_cast<const DataTypeInt16 *>(to))
            return true;    // NOLINT
        return false;
    }

    if (const auto * from_lc = typeid_cast<const DataTypeLowCardinality *>(from))
        return from_lc->getDictionaryType()->equals(*to);

    if (const auto * to_lc = typeid_cast<const DataTypeLowCardinality *>(to))
        return to_lc->getDictionaryType()->equals(*from);

    return false;
}

/// Special check for alters of VersionedCollapsingMergeTree version column
void checkVersionColumnTypesConversion(const IDataType * old_type, const IDataType * new_type, const String column_name)
{
    /// Check new type can be used as version
    if (!new_type->canBeUsedAsVersion())
        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Cannot alter version column {} to type {} because version column must be "
                        "of an integer type or of type Date or DateTime" , backQuoteIfNeed(column_name),
                        new_type->getName());

    auto which_new_type = WhichDataType(new_type);
    auto which_old_type = WhichDataType(old_type);

    /// Check alter to different sign or float -> int and so on
    if ((which_old_type.isInt() && !which_new_type.isInt())
        || (which_old_type.isUInt() && !which_new_type.isUInt())
        || (which_old_type.isDate() && !which_new_type.isDate())
        || (which_old_type.isDate32() && !which_new_type.isDate32())
        || (which_old_type.isDateTime() && !which_new_type.isDateTime())
        || (which_old_type.isFloat() && !which_new_type.isFloat()))
    {
        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "Cannot alter version column {} from type {} to type {} "
                        "because new type will change sort order of version column. "
                        "The only possible conversion is expansion of the number of bytes of the current type.",
                        backQuoteIfNeed(column_name), old_type->getName(), new_type->getName());
    }

    /// Check alter to smaller size: UInt64 -> UInt32 and so on
    if (new_type->getSizeOfValueInMemory() < old_type->getSizeOfValueInMemory())
    {
        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "Cannot alter version column {} from type {} to type {} "
                        "because new type is smaller than current in the number of bytes. "
                        "The only possible conversion is expansion of the number of bytes of the current type.",
                        backQuoteIfNeed(column_name), old_type->getName(), new_type->getName());
    }
}

}

void MergeTreeData::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    /// Check that needed transformations can be applied to the list of columns without considering type conversions.
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    const auto & settings = local_context->getSettingsRef();
    const auto & settings_from_storage = getSettings();

    if (!settings[Setting::allow_non_metadata_alters])
    {
        auto mutation_commands = commands.getMutationCommands(new_metadata, settings[Setting::materialize_ttl_after_modify], local_context);

        if (!mutation_commands.empty())
            throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                            "The following alter commands: '{}' will modify data on disk, "
                            "but setting `allow_non_metadata_alters` is disabled",
                            queryToString(mutation_commands.ast()));
    }

    /// Block the case of alter table add projection for special merge trees.
    if (std::any_of(commands.begin(), commands.end(), [](const AlterCommand & c) { return c.type == AlterCommand::ADD_PROJECTION; }))
    {
        if (merging_params.mode != MergingParams::Mode::Ordinary
            && (*settings_from_storage)[MergeTreeSetting::deduplicate_merge_projection_mode] == DeduplicateMergeProjectionMode::THROW)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Projection is fully supported in {} with deduplicate_merge_projection_mode = throw. "
                "Use 'drop' or 'rebuild' option of deduplicate_merge_projection_mode.",
                getName());
    }

    commands.apply(new_metadata, local_context);

    if (AlterCommands::hasFullTextIndex(new_metadata) && !settings[Setting::allow_experimental_full_text_index])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Experimental full-text index feature is not enabled (turn on setting 'allow_experimental_full_text_index')");

    if (AlterCommands::hasLegacyInvertedIndex(new_metadata) && !settings[Setting::allow_experimental_inverted_index])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Experimental inverted index feature is not enabled (turn on setting 'allow_experimental_inverted_index')");

    if (AlterCommands::hasVectorSimilarityIndex(new_metadata) && !settings[Setting::allow_experimental_vector_similarity_index])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Experimental vector similarity index is disabled (turn on setting 'allow_experimental_vector_similarity_index')");

    /// If adaptive index granularity is disabled, certain vector search queries with PREWHERE run into LOGICAL_ERRORs.
    ///     SET allow_experimental_vector_similarity_index = 1;
    ///     CREATE TABLE tab (`id` Int32, `vec` Array(Float32), INDEX idx vec TYPE  vector_similarity('hnsw', 'L2Distance') GRANULARITY 100000000) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes = 0;
    ///     INSERT INTO tab SELECT number, [toFloat32(number), 0.] FROM numbers(10000);
    ///     WITH [1., 0.] AS reference_vec SELECT id, L2Distance(vec, reference_vec) FROM tab PREWHERE toLowCardinality(10) ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 100;
    /// As a workaround, force enabled adaptive index granularity for now (it is the default anyways).
    if (AlterCommands::hasVectorSimilarityIndex(new_metadata) && (*getSettings())[MergeTreeSetting::index_granularity_bytes] == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Experimental vector similarity index can only be used with MergeTree setting 'index_granularity_bytes' != 0");

    for (const auto & disk : getDisks())
        if (!disk->supportsHardLinks() && !commands.isSettingsAlter() && !commands.isCommentAlter())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "ALTER TABLE commands are not supported on immutable disk '{}', except for setting and comment alteration",
                disk->getName());

    /// Set of columns that shouldn't be altered.
    NameSet columns_alter_type_forbidden;

    /// Primary key columns can be ALTERed only if they are used in the key as-is
    /// (and not as a part of some expression) and if the ALTER only affects column metadata.
    NameSet columns_alter_type_metadata_only;

    /// Columns to check that the type change is safe for partition key.
    NameSet columns_alter_type_check_safe_for_partition;

    if (old_metadata.hasPartitionKey())
    {
        /// Forbid altering columns inside partition key expressions because it can change partition ID format.
        auto partition_key_expr = old_metadata.getPartitionKey().expression;
        for (const auto & action : partition_key_expr->getActions())
        {
            for (const auto * child : action.node->children)
                columns_alter_type_forbidden.insert(child->result_name);
        }

        /// But allow to alter columns without expressions under certain condition.
        for (const String & col : partition_key_expr->getRequiredColumns())
            columns_alter_type_check_safe_for_partition.insert(col);
    }

    if (old_metadata.hasSortingKey())
    {
        auto sorting_key_expr = old_metadata.getSortingKey().expression;
        for (const auto & action : sorting_key_expr->getActions())
        {
            for (const auto * child : action.node->children)
                columns_alter_type_forbidden.insert(child->result_name);
        }
        for (const String & col : sorting_key_expr->getRequiredColumns())
            columns_alter_type_metadata_only.insert(col);

        /// We don't process sample_by_ast separately because it must be among the primary key columns
        /// and we don't process primary_key_expr separately because it is a prefix of sorting_key_expr.
    }
    if (!merging_params.sign_column.empty())
        columns_alter_type_forbidden.insert(merging_params.sign_column);

    /// All of the above.
    NameSet columns_in_keys;
    columns_in_keys.insert(columns_alter_type_forbidden.begin(), columns_alter_type_forbidden.end());
    columns_in_keys.insert(columns_alter_type_metadata_only.begin(), columns_alter_type_metadata_only.end());
    columns_in_keys.insert(columns_alter_type_check_safe_for_partition.begin(), columns_alter_type_check_safe_for_partition.end());

    std::unordered_map<String, String> columns_in_indices;
    for (const auto & index : old_metadata.getSecondaryIndices())
    {
        for (const String & col : index.expression->getRequiredColumns())
            columns_in_indices.emplace(col, index.name);
    }

    std::unordered_map<String, String> columns_in_projections;
    for (const auto & projection : old_metadata.getProjections())
    {
        for (const String & col : projection.getRequiredColumns())
            columns_in_projections.emplace(col, projection.name);
    }

    NameSet dropped_columns;

    std::map<String, const IDataType *> old_types;
    for (const auto & column : old_metadata.getColumns().getAllPhysical())
        old_types.emplace(column.name, column.type.get());

    NamesAndTypesList columns_to_check_conversion;

    auto unfinished_mutations = getUnfinishedMutationCommands();
    std::optional<NameDependencies> name_deps{};
    for (const AlterCommand & command : commands)
    {
        checkDropCommandDoesntAffectInProgressMutations(command, unfinished_mutations, local_context);
        /// Just validate partition expression
        if (command.partition)
        {
            getPartitionIDFromQuery(command.partition, local_context);
        }

        if (command.column_name == merging_params.version_column)
        {
            /// Some type changes for version column is allowed despite it's a part of sorting key
            if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                const IDataType * new_type = command.data_type.get();
                const IDataType * old_type = old_types[command.column_name];

                if (new_type)
                    checkVersionColumnTypesConversion(old_type, new_type, command.column_name);

                /// No other checks required
                continue;
            }
            if (command.type == AlterCommand::DROP_COLUMN)
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER DROP version {} column", backQuoteIfNeed(command.column_name));
            }
            if (command.type == AlterCommand::RENAME_COLUMN)
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER RENAME version {} column", backQuoteIfNeed(command.column_name));
            }
        }
        else if (command.column_name == merging_params.is_deleted_column)
        {
            checkSpecialColumn<DataTypeUInt8>("is_deleted", command);
        }
        else if (command.column_name == merging_params.sign_column)
        {
            checkSpecialColumn<DataTypeUInt8>("sign", command);
        }

        if (command.type == AlterCommand::MODIFY_QUERY)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "ALTER MODIFY QUERY is not supported by MergeTree engines family");
        if (command.type == AlterCommand::MODIFY_REFRESH)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "ALTER MODIFY REFRESH is not supported by MergeTree engines family");

        if (command.type == AlterCommand::MODIFY_SQL_SECURITY)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "ALTER MODIFY SQL SECURITY is not supported by MergeTree engines family");

        if (command.type == AlterCommand::MODIFY_ORDER_BY && !is_custom_partitioned)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "ALTER MODIFY ORDER BY is not supported for default-partitioned tables created with the old syntax");
        }
        if (command.type == AlterCommand::MODIFY_TTL && !is_custom_partitioned)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "ALTER MODIFY TTL is not supported for default-partitioned tables created with the old syntax");
        }
        if (command.type == AlterCommand::MODIFY_SAMPLE_BY)
        {
            if (!is_custom_partitioned)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "ALTER MODIFY SAMPLE BY is not supported for default-partitioned tables created with the old syntax");

            checkSampleExpression(new_metadata, (*getSettings())[MergeTreeSetting::compatibility_allow_sampling_expression_not_in_primary_key],
                                  (*getSettings())[MergeTreeSetting::check_sample_column_is_correct]);
        }
        if (command.type == AlterCommand::ADD_INDEX && !is_custom_partitioned)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER ADD INDEX is not supported for tables with the old syntax");
        }
        if (command.type == AlterCommand::ADD_PROJECTION)
        {
            if (!is_custom_partitioned)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER ADD PROJECTION is not supported for tables with the old syntax");
        }
        if (command.type == AlterCommand::RENAME_COLUMN)
        {
            if (columns_in_keys.contains(command.column_name))
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                                "Trying to ALTER RENAME key {} column which is a part of key expression",
                                backQuoteIfNeed(command.column_name));
            }

            /// Don't check columns in indices here. RENAME works fine with index columns.

            if (auto it = columns_in_projections.find(command.column_name); it != columns_in_projections.end())
            {
                throw Exception(
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER RENAME {} column which is a part of projection {}",
                    backQuoteIfNeed(command.column_name),
                    it->second);
            }
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            if (columns_in_keys.contains(command.column_name))
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER DROP key {} column which is a part of key expression", backQuoteIfNeed(command.column_name));
            }

            /// Don't check columns in indices or projections here. If required columns of indices
            /// or projections get dropped, it will be checked later in AlterCommands::apply. This
            /// allows projections with * to drop columns. One example can be found in
            /// 02691_drop_column_with_projections_replicated.sql.

            if (!command.clear)
            {
                if (!name_deps)
                    name_deps = getDependentViewsByColumn(local_context);
                const auto & deps_mv = name_deps.value()[command.column_name];
                if (!deps_mv.empty())
                {
                    throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Trying to ALTER DROP column {} which is referenced by materialized view {}",
                        backQuoteIfNeed(command.column_name), toString(deps_mv));
                }
            }

            if (old_metadata.columns.has(command.column_name))
            {
                dropped_columns.emplace(command.column_name);
            }
            else
            {
                const auto & nested = old_metadata.columns.getNested(command.column_name);
                for (const auto & nested_column : nested)
                    dropped_columns.emplace(nested_column.name);
            }

        }
        else if (command.type == AlterCommand::RESET_SETTING)
        {
            for (const auto & reset_setting : command.settings_resets)
            {
                if (!settings_from_storage->has(reset_setting))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Cannot reset setting '{}' because it doesn't exist for MergeTree engines family",
                                    reset_setting);
            }
        }
        else if (command.isRequireMutationStage(getInMemoryMetadata()))
        {
            /// This alter will override data on disk. Let's check that it doesn't
            /// modify immutable column.
            if (columns_alter_type_forbidden.contains(command.column_name))
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "ALTER of key column {} is forbidden",
                    backQuoteIfNeed(command.column_name));

            if (auto it = columns_in_indices.find(command.column_name); it != columns_in_indices.end())
            {
                throw Exception(
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER {} column which is a part of index {}",
                    backQuoteIfNeed(command.column_name),
                    it->second);
            }

            /// Don't check columns in projections here. If required columns of projections get
            /// modified, it will be checked later in AlterCommands::apply.

            if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                if (columns_alter_type_check_safe_for_partition.contains(command.column_name))
                {
                    auto it = old_types.find(command.column_name);

                    assert(it != old_types.end());
                    if (!isSafeForKeyConversion(it->second, command.data_type.get()))
                        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                                        "ALTER of partition key column {} from type {} "
                                        "to type {} is not safe because it can change the representation "
                                        "of partition key", backQuoteIfNeed(command.column_name),
                                        it->second->getName(), command.data_type->getName());
                }

                if (columns_alter_type_metadata_only.contains(command.column_name))
                {
                    auto it = old_types.find(command.column_name);
                    assert(it != old_types.end());
                    if (!isSafeForKeyConversion(it->second, command.data_type.get()))
                        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                                        "ALTER of key column {} from type {} "
                                        "to type {} is not safe because it can change the representation "
                                        "of primary key", backQuoteIfNeed(command.column_name),
                                        it->second->getName(), command.data_type->getName());
                }

                if (old_metadata.getColumns().has(command.column_name))
                {
                    columns_to_check_conversion.push_back(
                        new_metadata.getColumns().getPhysical(command.column_name));

                    const auto & old_column = old_metadata.getColumns().get(command.column_name);
                    if (!old_column.statistics.empty())
                    {
                        const auto & new_column = new_metadata.getColumns().get(command.column_name);
                        if (!old_column.type->equals(*new_column.type))
                            throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                                            "ALTER types of column {} with statistics is not safe "
                                            "because it can change the representation of statistics",
                                            backQuoteIfNeed(command.column_name));
                    }
                }
            }
        }
    }

    checkColumnFilenamesForCollision(new_metadata, /*throw_on_error=*/ true);
    checkProperties(new_metadata, old_metadata, false, false, allow_nullable_key, local_context);
    checkTTLExpressions(new_metadata, old_metadata);

    if (!columns_to_check_conversion.empty())
    {
        auto old_header = old_metadata.getSampleBlock();
        performRequiredConversions(old_header, columns_to_check_conversion, local_context);
    }

    if (old_metadata.hasSettingsChanges())
    {
        const auto current_changes = old_metadata.getSettingsChanges()->as<const ASTSetQuery &>().changes;
        const auto & new_changes = new_metadata.settings_changes->as<const ASTSetQuery &>().changes;
        local_context->checkMergeTreeSettingsConstraints(*settings_from_storage, new_changes);

        bool found_disk_setting = false;
        bool found_storage_policy_setting = false;

        for (const auto & changed_setting : new_changes)
        {
            const auto & setting_name = changed_setting.name;
            const auto & new_value = changed_setting.value;
            MergeTreeSettings::checkCanSet(setting_name, new_value);
            const Field * current_value = current_changes.tryGet(setting_name);

            if ((!current_value || *current_value != new_value)
                && MergeTreeSettings::isReadonlySetting(setting_name))
            {
                throw Exception(ErrorCodes::READONLY_SETTING, "Setting '{}' is readonly for storage '{}'", setting_name, getName());
            }

            if (!current_value && MergeTreeSettings::isPartFormatSetting(setting_name))
            {
                MergeTreeSettings copy = *getSettings();
                copy.applyChange(changed_setting);
                String reason;
                if (!canUsePolymorphicParts(copy, reason) && !reason.empty())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can't change settings. Reason: {}", reason);
            }

            if (setting_name == "storage_policy")
            {
                checkStoragePolicy(local_context->getStoragePolicy(new_value.safeGet<String>()));
                found_storage_policy_setting = true;
            }
            else if (setting_name == "disk")
            {
                checkStoragePolicy(local_context->getStoragePolicyFromDisk(new_value.safeGet<String>()));
                found_disk_setting = true;
            }
        }

        if (found_storage_policy_setting && found_disk_setting)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "MergeTree settings `storage_policy` and `disk` cannot be specified at the same time");

        /// Check if it is safe to reset the settings
        for (const auto & current_setting : current_changes)
        {
            const auto & setting_name = current_setting.name;
            const Field * new_value = new_changes.tryGet(setting_name);
            /// Prevent unsetting readonly setting
            if (MergeTreeSettings::isReadonlySetting(setting_name) && !new_value)
            {
                throw Exception(ErrorCodes::READONLY_SETTING, "Setting '{}' is readonly for storage '{}'", setting_name, getName());
            }

            if (MergeTreeSettings::isPartFormatSetting(setting_name) && !new_value)
            {
                /// Use default settings + new and check if doesn't affect part format settings
                auto copy = getDefaultSettings();
                copy->applyChanges(new_changes);
                String reason;
                if (!canUsePolymorphicParts(*copy, reason) && !reason.empty())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can't change settings. Reason: {}", reason);
            }

        }
    }

    for (const auto & part : getDataPartsVectorForInternalUsage())
    {
        bool at_least_one_column_rest = false;
        for (const auto & column : part->getColumns())
        {
            if (!dropped_columns.contains(column.name))
            {
                at_least_one_column_rest = true;
                break;
            }
        }
        if (!at_least_one_column_rest)
        {
            std::string postfix;
            if (dropped_columns.size() > 1)
                postfix = "s";
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cannot drop or clear column{} '{}', because all columns "
                            "in part '{}' will be removed from disk. Empty parts are not allowed",
                            postfix, boost::algorithm::join(dropped_columns, ", "), part->name);
        }
    }
}


void MergeTreeData::checkMutationIsPossible(const MutationCommands & /*commands*/, const Settings & /*settings*/) const
{
    for (const auto & disk : getDisks())
        if (!disk->supportsHardLinks())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Mutations are not supported for immutable disk '{}'", disk->getName());
}

MergeTreeDataPartFormat MergeTreeData::choosePartFormat(size_t bytes_uncompressed, size_t rows_count) const
{
    using PartType = MergeTreeDataPartType;
    using PartStorageType = MergeTreeDataPartStorageType;

    String out_reason;
    const auto settings = getSettings();
    if (!canUsePolymorphicParts(*settings, out_reason))
        return {PartType::Wide, PartStorageType::Full};

    auto satisfies = [&](const auto & min_bytes_for, const auto & min_rows_for)
    {
        return bytes_uncompressed < min_bytes_for || rows_count < min_rows_for;
    };

    auto part_type = PartType::Wide;
    if (satisfies((*settings)[MergeTreeSetting::min_bytes_for_wide_part], (*settings)[MergeTreeSetting::min_rows_for_wide_part]))
        part_type = PartType::Compact;

    return {part_type, PartStorageType::Full};
}

MergeTreeDataPartFormat MergeTreeData::choosePartFormatOnDisk(size_t bytes_uncompressed, size_t rows_count) const
{
    return choosePartFormat(bytes_uncompressed, rows_count);
}

MergeTreeDataPartBuilder MergeTreeData::getDataPartBuilder(
    const String & name, const VolumePtr & volume, const String & part_dir, const ReadSettings & read_settings_) const
{
    return MergeTreeDataPartBuilder(*this, name, volume, relative_data_path, part_dir, read_settings_);
}

void MergeTreeData::changeSettings(
        const ASTPtr & new_settings,
        AlterLockHolder & /* table_lock_holder */)
{
    if (new_settings)
    {
        bool has_storage_policy_changed = false;

        const auto & new_changes = new_settings->as<const ASTSetQuery &>().changes;
        StoragePolicyPtr new_storage_policy = nullptr;

        for (const auto & change : new_changes)
        {
            if (change.name == "disk" || change.name == "storage_policy")
            {
                if (change.name == "disk")
                    new_storage_policy = getContext()->getStoragePolicyFromDisk(change.value.safeGet<String>());
                else
                    new_storage_policy = getContext()->getStoragePolicy(change.value.safeGet<String>());
                StoragePolicyPtr old_storage_policy = getStoragePolicy();

                /// StoragePolicy of different version or name is guaranteed to have different pointer
                if (new_storage_policy != old_storage_policy)
                {
                    checkStoragePolicy(new_storage_policy);

                    std::unordered_set<String> all_diff_disk_names;
                    for (const auto & disk : new_storage_policy->getDisks())
                        all_diff_disk_names.insert(disk->getName());
                    for (const auto & disk : old_storage_policy->getDisks())
                        all_diff_disk_names.erase(disk->getName());

                    for (const String & disk_name : all_diff_disk_names)
                    {
                        auto disk = new_storage_policy->getDiskByName(disk_name);
                        if (disk->existsDirectory(relative_data_path))
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "New storage policy contain disks which already contain data of a table with the same name");
                    }

                    for (const String & disk_name : all_diff_disk_names)
                    {
                        auto disk = new_storage_policy->getDiskByName(disk_name);
                        disk->createDirectories(relative_data_path);
                        disk->createDirectories(fs::path(relative_data_path) / DETACHED_DIR_NAME);
                    }
                    /// FIXME how would that be done while reloading configuration???

                    has_storage_policy_changed = true;
                }
            }
        }

        /// Reset to default settings before applying existing.
        auto copy = getDefaultSettings();
        copy->applyChanges(new_changes);
        copy->sanityCheck(getContext()->getMergeMutateExecutor()->getMaxTasksCount());

        storage_settings.set(std::move(copy));
        StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
        new_metadata.setSettingsChanges(new_settings);
        setInMemoryMetadata(new_metadata);

        if (has_storage_policy_changed)
            startBackgroundMovesIfNeeded();
    }
}

void MergeTreeData::PartsTemporaryRename::addPart(const String & old_name, const String & new_name, const DiskPtr & disk)
{
    old_and_new_names.push_back({old_name, new_name, disk});
}

void MergeTreeData::PartsTemporaryRename::tryRenameAll()
{
    renamed = true;
    for (size_t i = 0; i < old_and_new_names.size(); ++i)
    {
        try
        {
            const auto & [old_name, new_name, disk] = old_and_new_names[i];
            if (old_name.empty() || new_name.empty())
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Empty part name. Most likely it's a bug.");
            const auto full_path = fs::path(storage.relative_data_path) / source_dir;
            disk->moveFile(fs::path(full_path) / old_name, fs::path(full_path) / new_name);
        }
        catch (...)
        {
            old_and_new_names.resize(i);
            LOG_WARNING(storage.log, "Cannot rename parts to perform operation on them: {}", getCurrentExceptionMessage(false));
            throw;
        }
    }
}

MergeTreeData::PartsTemporaryRename::~PartsTemporaryRename()
{
    // TODO what if server had crashed before this destructor was called?
    if (!renamed)
        return;
    for (const auto & [old_name, new_name, disk] : old_and_new_names)
    {
        if (old_name.empty())
            continue;

        try
        {
            const String full_path = fs::path(storage.relative_data_path) / source_dir;
            disk->moveFile(fs::path(full_path) / new_name, fs::path(full_path) / old_name);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

MergeTreeData::PartHierarchy MergeTreeData::getPartHierarchy(
    const MergeTreePartInfo & part_info,
    DataPartState state,
    DataPartsLock & /* data_parts_lock */) const
{
    PartHierarchy result;

    /// Parts contained in the part are consecutive in data_parts, intersecting the insertion place for the part itself.
    auto it_middle = data_parts_by_state_and_info.lower_bound(DataPartStateAndInfo{state, part_info});
    auto committed_parts_range = getDataPartsStateRange(state);

    /// Go to the left.
    DataPartIteratorByStateAndInfo begin = it_middle;
    while (begin != committed_parts_range.begin())
    {
        auto prev = std::prev(begin);

        if (!part_info.contains((*prev)->info))
        {
            if ((*prev)->info.contains(part_info))
            {
                result.covering_parts.push_back(*prev);
            }
            else if (!part_info.isDisjoint((*prev)->info))
            {
                result.intersected_parts.push_back(*prev);
            }

            break;
        }

        begin = prev;
    }

    std::reverse(result.covering_parts.begin(), result.covering_parts.end());

    /// Go to the right.
    DataPartIteratorByStateAndInfo end = it_middle;
    while (end != committed_parts_range.end())
    {
        if ((*end)->info == part_info)
        {
            result.duplicate_part = *end;
        }

        if (!part_info.contains((*end)->info))
        {
            if ((*end)->info.contains(part_info))
            {
                result.covering_parts.push_back(*end);
            }
            else if (!part_info.isDisjoint((*end)->info))
            {
                result.intersected_parts.push_back(*end);
            }

            break;
        }

        ++end;
    }

    if (begin != committed_parts_range.end() && (*begin)->info == part_info)
        ++begin;

    result.covered_parts.insert(result.covered_parts.end(), begin, end);

    return result;
}

MergeTreeData::DataPartsVector MergeTreeData::getCoveredOutdatedParts(
    const DataPartPtr & part,
    DataPartsLock & data_parts_lock) const
{
    part->assertState({DataPartState::Active, DataPartState::PreActive, DataPartState::Outdated});
    bool is_outdated_part = part->getState() == DataPartState::Outdated;
    PartHierarchy hierarchy = getPartHierarchy(part->info, DataPartState::Outdated, data_parts_lock);

    if (hierarchy.duplicate_part && !is_outdated_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected duplicate part {}. It is a bug.", hierarchy.duplicate_part->getNameWithState());

    return hierarchy.covered_parts;
}

MergeTreeData::DataPartsVector MergeTreeData::getActivePartsToReplace(
    const MergeTreePartInfo & new_part_info,
    const String & new_part_name,
    DataPartPtr & out_covering_part,
    DataPartsLock & data_parts_lock) const
{
    PartHierarchy hierarchy = getPartHierarchy(new_part_info, DataPartState::Active, data_parts_lock);

    if (!hierarchy.intersected_parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects part {}. It is a bug.",
                        new_part_name, hierarchy.intersected_parts.back()->getNameWithState());

    if (hierarchy.duplicate_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected duplicate part {}. It is a bug.", hierarchy.duplicate_part->getNameWithState());

    if (!hierarchy.covering_parts.empty())
        out_covering_part = std::move(hierarchy.covering_parts.back());

    return std::move(hierarchy.covered_parts);
}

void MergeTreeData::checkPartPartition(MutableDataPartPtr & part, DataPartsLock & lock) const
{
    if (DataPartPtr existing_part_in_partition = getAnyPartInPartition(part->info.partition_id, lock))
    {
        if (part->partition.value != existing_part_in_partition->partition.value)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Partition value mismatch between two parts with the same partition ID. "
                "Existing part: {}, newly added part: {}", existing_part_in_partition->name, part->name);
    }
}

void MergeTreeData::checkPartDuplicate(MutableDataPartPtr & part, Transaction & transaction, DataPartsLock & /*lock*/) const
{
    auto it_duplicate = data_parts_by_info.find(part->info);

    if (it_duplicate != data_parts_by_info.end())
    {
        if ((*it_duplicate)->checkState({DataPartState::Outdated, DataPartState::Deleting}))
            throw Exception(ErrorCodes::PART_IS_TEMPORARILY_LOCKED, "Part {} already exists, but it will be deleted soon",
                            (*it_duplicate)->getNameWithState());

        if (transaction.txn)
            throw Exception(ErrorCodes::SERIALIZATION_ERROR, "Part {} already exists", (*it_duplicate)->getNameWithState());

        throw Exception(ErrorCodes::DUPLICATE_DATA_PART, "Part {} already exists", (*it_duplicate)->getNameWithState());
    }
}

void MergeTreeData::checkPartDynamicColumns(MutableDataPartPtr & part, DataPartsLock & /*lock*/) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & columns = metadata_snapshot->getColumns();
    auto virtuals = getVirtualsPtr();

    if (!hasDynamicSubcolumns(columns))
        return;

    const auto & part_columns = part->getColumns();
    for (const auto & part_column : part_columns)
    {
        if (virtuals->has(part_column.name))
            continue;

        auto storage_column = columns.getPhysical(part_column.name);
        if (!storage_column.type->hasDynamicSubcolumnsDeprecated())
            continue;

        auto concrete_storage_column = object_columns.getPhysical(part_column.name);

        /// It will throw if types are incompatible.
        getLeastCommonTypeForDynamicColumns(storage_column.type, {concrete_storage_column.type, part_column.type}, true);
    }
}

void MergeTreeData::preparePartForCommit(MutableDataPartPtr & part, Transaction & out_transaction, bool need_rename, bool rename_in_transaction)
{
    part->is_temp = false;
    part->setState(DataPartState::PreActive);

    assert([&]()
           {
               String dir_name = fs::path(part->getDataPartStorage().getRelativePath()).filename();
               bool may_be_cleaned_up = dir_name.starts_with("tmp_") || dir_name.starts_with("tmp-fetch_");
               return !may_be_cleaned_up || temporary_parts.contains(dir_name);
           }());
    assert(!(!need_rename && rename_in_transaction));

    if (need_rename && !rename_in_transaction)
        part->renameTo(part->name, true);

    LOG_TEST(log, "preparePartForCommit: inserting {} into data_parts_indexes", part->getNameWithState());
    data_parts_indexes.insert(part);
    if (rename_in_transaction)
        out_transaction.addPart(part, need_rename);
    else
        out_transaction.addPart(part, /* need_rename= */ false);
}

bool MergeTreeData::addTempPart(
    MutableDataPartPtr & part,
    Transaction & out_transaction,
    DataPartsLock & lock,
    DataPartsVector * out_covered_parts)
{
    LOG_TRACE(log, "Adding temporary part from directory {} with name {}.", part->getDataPartStorage().getPartDirectory(), part->name);
    if (&out_transaction.data != this)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeData::Transaction for one table cannot be used with another. It is a bug.");

    if (part->hasLightweightDelete())
        has_lightweight_delete_parts.store(true);

    checkPartPartition(part, lock);
    checkPartDuplicate(part, out_transaction, lock);
    checkPartDynamicColumns(part, lock);

    DataPartPtr covering_part;
    DataPartsVector covered_parts = getActivePartsToReplace(part->info, part->name, covering_part, lock);

    if (covering_part)
    {
        LOG_WARNING(log, "Tried to add obsolete part {} covered by {}", part->name, covering_part->getNameWithState());
        return false;
    }

    /// All checks are passed. Now we can rename the part on disk.
    /// So, we maintain invariant: if a non-temporary part in filesystem then it is in data_parts
    preparePartForCommit(part, out_transaction, /* need_rename = */false);

    if (out_covered_parts)
    {
        out_covered_parts->reserve(covered_parts.size());

        for (DataPartPtr & covered_part : covered_parts)
            out_covered_parts->emplace_back(std::move(covered_part));
    }

    return true;
}


bool MergeTreeData::renameTempPartAndReplaceImpl(
    MutableDataPartPtr & part,
    Transaction & out_transaction,
    DataPartsLock & lock,
    DataPartsVector * out_covered_parts,
    bool rename_in_transaction)
{
    LOG_TRACE(log, "Renaming temporary part {} to {} with tid {}.", part->getDataPartStorage().getPartDirectory(), part->name, out_transaction.getTID());

    if (&out_transaction.data != this)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeData::Transaction for one table cannot be used with another. It is a bug.");

    part->assertState({DataPartState::Temporary});
    checkPartPartition(part, lock);
    checkPartDuplicate(part, out_transaction, lock);
    checkPartDynamicColumns(part, lock);

    PartHierarchy hierarchy = getPartHierarchy(part->info, DataPartState::Active, lock);

    if (!hierarchy.intersected_parts.empty())
    {
        // Drop part|partition operation inside some transactions sees some stale snapshot from the time when transactions has been started.
        // So such operation may attempt to delete already outdated part. In this case, this outdated part is most likely covered by the other part and intersection may occur.
        // Part mayght be outdated due to merge|mutation|update|optimization operations.
        if (part->isEmpty() || (hierarchy.intersected_parts.size() == 1 && hierarchy.intersected_parts.back()->isEmpty()))
        {
            throw Exception(ErrorCodes::SERIALIZATION_ERROR, "Part {} intersects part {}. One of them is empty part. "
                            "That is a race between drop operation under transaction and a merge/mutation.",
                            part->name, hierarchy.intersected_parts.back()->getNameWithState());
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects part {}. There are {} intersected parts. It is a bug.",
                        part->name, hierarchy.intersected_parts.back()->getNameWithState(), hierarchy.intersected_parts.size());
    }

    if (hierarchy.duplicate_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected duplicate part {}. It is a bug.", hierarchy.duplicate_part->getNameWithState());


    if (part->hasLightweightDelete())
        has_lightweight_delete_parts.store(true);

    /// All checks are passed. Now we can rename the part on disk.
    /// So, we maintain invariant: if a non-temporary part in filesystem then it is in data_parts
    preparePartForCommit(part, out_transaction, /* need_rename= */ true, rename_in_transaction);

    if (out_covered_parts)
    {
        out_covered_parts->reserve(out_covered_parts->size() + hierarchy.covered_parts.size());
        std::move(hierarchy.covered_parts.begin(), hierarchy.covered_parts.end(), std::back_inserter(*out_covered_parts));
    }

    return true;
}

bool MergeTreeData::renameTempPartAndReplaceUnlocked(
    MutableDataPartPtr & part,
    Transaction & out_transaction,
    DataPartsLock & lock,
    bool rename_in_transaction)
{
    return renameTempPartAndReplaceImpl(part, out_transaction, lock, /*out_covered_parts=*/ nullptr, rename_in_transaction);
}

MergeTreeData::DataPartsVector MergeTreeData::renameTempPartAndReplace(
    MutableDataPartPtr & part,
    Transaction & out_transaction,
    bool rename_in_transaction)
{
    auto part_lock = lockParts();
    DataPartsVector covered_parts;
    renameTempPartAndReplaceImpl(part, out_transaction, part_lock, &covered_parts, rename_in_transaction);
    return covered_parts;
}

bool MergeTreeData::renameTempPartAndAdd(
    MutableDataPartPtr & part,
    Transaction & out_transaction,
    DataPartsLock & lock,
    bool rename_in_transaction)
{
    DataPartsVector covered_parts;

    if (!renameTempPartAndReplaceImpl(part, out_transaction, lock, &covered_parts, rename_in_transaction))
        return false;

    if (!covered_parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Added part {} covers {} existing part(s) (including {})",
            part->name, covered_parts.size(), covered_parts[0]->name);

    return true;
}

void MergeTreeData::removePartsFromWorkingSet(MergeTreeTransaction * txn, const MergeTreeData::DataPartsVector & remove, bool clear_without_timeout, DataPartsLock & acquired_lock)
{
    if (txn)
        transactions_enabled.store(true);

    auto remove_time = clear_without_timeout ? 0 : time(nullptr);
    bool removed_active_part = false;

    for (const DataPartPtr & part : remove)
    {
        if (part->version.creation_csn != Tx::RolledBackCSN)
            MergeTreeTransaction::removeOldPart(shared_from_this(), part, txn);

        if (part->getState() == MergeTreeDataPartState::Active)
        {
            removePartContributionToColumnAndSecondaryIndexSizes(part);
            removePartContributionToDataVolume(part);
            removed_active_part = true;
        }

        if (part->getState() == MergeTreeDataPartState::Active || clear_without_timeout)
            part->remove_time.store(remove_time, std::memory_order_relaxed);

        if (part->getState() != MergeTreeDataPartState::Outdated)
            modifyPartState(part, MergeTreeDataPartState::Outdated);
    }

    if (removed_active_part)
        resetObjectColumnsFromActiveParts(acquired_lock);
}

void MergeTreeData::removePartsFromWorkingSetImmediatelyAndSetTemporaryState(const DataPartsVector & remove, DataPartsLock * acquired_lock)
{
    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();

    for (const auto & part : remove)
    {
        auto it_part = data_parts_by_info.find(part->info);
        if (it_part == data_parts_by_info.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} not found in data_parts", part->getNameWithState());

        assert(part->getState() == MergeTreeDataPartState::PreActive);

        modifyPartState(part, MergeTreeDataPartState::Temporary);
        /// Erase immediately
        LOG_TEST(log, "removePartsFromWorkingSetImmediatelyAndSetTemporaryState: removing {} from data_parts_indexes", part->getNameWithState());
        data_parts_indexes.erase(it_part);
    }
}

void MergeTreeData::removePartsFromWorkingSet(
        MergeTreeTransaction * txn, const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock * acquired_lock)
{
    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();

    for (const auto & part : remove)
    {
        if (!data_parts_by_info.count(part->info))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} not found in data_parts", part->getNameWithState());

        part->assertState({DataPartState::PreActive, DataPartState::Active, DataPartState::Outdated});
    }

    removePartsFromWorkingSet(txn, remove, clear_without_timeout, lock);
}


void MergeTreeData::removePartsInRangeFromWorkingSet(MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock)
{
    removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper(txn, drop_range, lock, /*create_empty_part*/ false);
}

DataPartsVector MergeTreeData::grabActivePartsToRemoveForDropRange(
    MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock)
{
    DataPartsVector parts_to_remove;

    if (drop_range.min_block > drop_range.max_block)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid drop range: {}", drop_range.getPartNameForLogs());

    auto partition_range = getVisibleDataPartsVectorInPartition(txn, drop_range.partition_id, &lock);

    for (const DataPartPtr & part : partition_range)
    {
        if (part->info.partition_id != drop_range.partition_id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected partition_id of part {}. This is a bug.", part->name);

        /// It's a DROP PART and it's already executed by fetching some covering part
        bool is_drop_part = !drop_range.isFakeDropRangePart() && drop_range.min_block;

        if (is_drop_part && (part->info.min_block != drop_range.min_block || part->info.max_block != drop_range.max_block || part->info.getMutationVersion() != drop_range.getMutationVersion()))
        {
            /// Why we check only min and max blocks here without checking merge
            /// level? It's a tricky situation which can happen on a stale
            /// replica. For example, we have parts all_1_1_0, all_2_2_0 and
            /// all_3_3_0. Fast replica assign some merges (OPTIMIZE FINAL or
            /// TTL) all_2_2_0 -> all_2_2_1 -> all_2_2_2. So it has set of parts
            /// all_1_1_0, all_2_2_2 and all_3_3_0. After that it decides to
            /// drop part all_2_2_2. Now set of parts is all_1_1_0 and
            /// all_3_3_0. Now fast replica assign merge all_1_1_0 + all_3_3_0
            /// to all_1_3_1 and finishes it. Slow replica pulls the queue and
            /// have two contradictory tasks -- drop all_2_2_2 and merge/fetch
            /// all_1_3_1. If this replica will fetch all_1_3_1 first and then tries
            /// to drop all_2_2_2 after that it will receive the LOGICAL ERROR.
            /// So here we just check that all_1_3_1 covers blocks from drop
            /// all_2_2_2.
            ///
            bool is_covered_by_min_max_block = part->info.min_block <= drop_range.min_block && part->info.max_block >= drop_range.max_block && part->info.getMutationVersion() >= drop_range.getMutationVersion();
            if (is_covered_by_min_max_block)
            {
                LOG_TRACE(LogFrequencyLimiter(log.load(), 1), "Skipping drop range for part {} because covering part {} already exists", drop_range.getPartNameForLogs(), part->name);
                return {};
            }
        }

        if (part->info.min_block < drop_range.min_block)
        {
            if (drop_range.min_block <= part->info.max_block)
            {
                /// Intersect left border
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected merged part {} intersecting drop range {}",
                                part->name, drop_range.getPartNameForLogs());
            }

            continue;
        }

        /// Stop on new parts
        if (part->info.min_block > drop_range.max_block)
            break;

        if (part->info.min_block <= drop_range.max_block && drop_range.max_block < part->info.max_block)
        {
            /// Intersect right border
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected merged part {} intersecting drop range {}",
                            part->name, drop_range.getPartNameForLogs());
        }

        parts_to_remove.emplace_back(part);
    }
    return parts_to_remove;
}

MergeTreeData::PartsToRemoveFromZooKeeper MergeTreeData::removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper(
        MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock, bool create_empty_part)
{
#ifndef NDEBUG
    {
        /// All parts (including outdated) must be loaded at this moment.
        std::lock_guard outdated_parts_lock(outdated_data_parts_mutex);
        assert(outdated_unloaded_data_parts.empty());
    }
#endif

    auto parts_to_remove = grabActivePartsToRemoveForDropRange(txn, drop_range, lock);

    bool clear_without_timeout = true;
    /// We a going to remove active parts covered by drop_range without timeout.
    /// Let's also reset timeout for inactive parts
    /// and add these parts to list of parts to remove from ZooKeeper
    auto inactive_parts_to_remove_immediately = getDataPartsVectorInPartitionForInternalUsage({DataPartState::Outdated, DataPartState::Deleting}, drop_range.partition_id, &lock);

    /// FIXME refactor removePartsFromWorkingSet(...), do not remove parts twice
    removePartsFromWorkingSet(txn, parts_to_remove, clear_without_timeout, lock);

    /// We can only create a covering part for a blocks range that starts with 0 (otherwise we may get "intersecting parts"
    /// if we remove a range from the middle when dropping a part).
    /// Maybe we could do it by incrementing mutation version to get a name for the empty covering part,
    /// but it's okay to simply avoid creating it for DROP PART (for a part in the middle).
    /// NOTE: Block numbers in ReplicatedMergeTree start from 0. For MergeTree, is_new_syntax is always false.
    assert(!create_empty_part || supportsReplication());
    bool range_in_the_middle = drop_range.min_block;
    bool is_new_syntax = format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    if (create_empty_part && !parts_to_remove.empty() && is_new_syntax && !range_in_the_middle)
    {
        /// We are going to remove a lot of parts from zookeeper just after returning from this function.
        /// And we will remove parts from disk later (because some queries may use them).
        /// But if the server restarts in-between, then it will notice a lot of unexpected parts,
        /// so it may refuse to start. Let's create an empty part that covers them.
        /// We don't need to commit it to zk, and don't even need to activate it.

        MergeTreePartInfo empty_info = drop_range;
        empty_info.level = empty_info.mutation = 0;
        empty_info.min_block = MergeTreePartInfo::MAX_BLOCK_NUMBER;
        for (const auto & part : parts_to_remove)
        {
            /// We still have to take min_block into account to avoid creating multiple covering ranges
            /// that intersect each other
            empty_info.min_block = std::min(empty_info.min_block, part->info.min_block);
            empty_info.level = std::max(empty_info.level, part->info.level);
            empty_info.mutation = std::max(empty_info.mutation, part->info.mutation);
        }
        empty_info.level += 1;

        const auto & partition = parts_to_remove.front()->partition;
        String empty_part_name = empty_info.getPartNameAndCheckFormat(format_version);
        auto [new_data_part, tmp_dir_holder] = createEmptyPart(empty_info, partition, empty_part_name, NO_TRANSACTION_PTR);

        MergeTreeData::Transaction transaction(*this, NO_TRANSACTION_RAW);
        renameTempPartAndAdd(new_data_part, transaction, lock, /*rename_in_transaction=*/ false);     /// All covered parts must be already removed

        /// It will add the empty part to the set of Outdated parts without making it Active (exactly what we need)
        transaction.rollback(&lock);
        new_data_part->remove_time.store(0, std::memory_order_relaxed);
        /// Such parts are always local, they don't participate in replication, they don't have shared blobs.
        /// So we don't have locks for shared data in zk for them, and can just remove blobs (this avoids leaving garbage in S3)
        new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS_OF_NOT_TEMPORARY;
    }

    /// Since we can return parts in Deleting state, we have to use a wrapper that restricts access to such parts.
    PartsToRemoveFromZooKeeper parts_to_remove_from_zookeeper;
    for (auto & part : parts_to_remove)
        parts_to_remove_from_zookeeper.emplace_back(std::move(part));

    for (auto & part : inactive_parts_to_remove_immediately)
    {
        if (!drop_range.contains(part->info))
            continue;
        part->remove_time.store(0, std::memory_order_relaxed);
        parts_to_remove_from_zookeeper.emplace_back(std::move(part), /* was_active */ false);
    }

    return parts_to_remove_from_zookeeper;
}

void MergeTreeData::restoreAndActivatePart(const DataPartPtr & part, DataPartsLock * acquired_lock)
{
    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();
    if (part->getState() == DataPartState::Active)
        return;
    addPartContributionToColumnAndSecondaryIndexSizes(part);
    addPartContributionToDataVolume(part);
    modifyPartState(part, DataPartState::Active);
}


void MergeTreeData::outdateUnexpectedPartAndCloneToDetached(const DataPartPtr & part_to_detach)
{
    LOG_INFO(log, "Cloning part {} to unexpected_{} and making it obsolete.", part_to_detach->getDataPartStorage().getPartDirectory(), part_to_detach->name);
    part_to_detach->makeCloneInDetached("unexpected", getInMemoryMetadataPtr(), /*disk_transaction*/ {});

    DataPartsLock lock = lockParts();
    part_to_detach->is_unexpected_local_part = true;
    if (part_to_detach->getState() == DataPartState::Active)
        removePartsFromWorkingSet(NO_TRANSACTION_RAW, {part_to_detach}, true, &lock);
}

void MergeTreeData::forcefullyMovePartToDetachedAndRemoveFromMemory(const MergeTreeData::DataPartPtr & part_to_detach, const String & prefix)
{
    if (prefix.empty())
        LOG_INFO(log, "Renaming {} to {} and forgetting it.", part_to_detach->getDataPartStorage().getPartDirectory(), part_to_detach->name);
    else
        LOG_INFO(log, "Renaming {} to {}_{} and forgetting it.", part_to_detach->getDataPartStorage().getPartDirectory(), prefix, part_to_detach->name);

    auto lock = lockParts();
    bool removed_active_part = false;
    bool restored_active_part = false;

    auto it_part = data_parts_by_info.find(part_to_detach->info);
    if (it_part == data_parts_by_info.end())
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No such data part {}", part_to_detach->getNameWithState());

    /// What if part_to_detach is a reference to *it_part? Make a new owner just in case.
    /// Important to own part pointer here (not const reference), because it will be removed from data_parts_indexes
    /// few lines below.
    DataPartPtr part = *it_part; // NOLINT

    if (part->getState() == DataPartState::Active)
    {
        removePartContributionToDataVolume(part);
        removePartContributionToColumnAndSecondaryIndexSizes(part);
        removed_active_part = true;
    }

    modifyPartState(it_part, DataPartState::Deleting);
    asMutableDeletingPart(part)->renameToDetached(prefix);
    LOG_TEST(log, "forcefullyMovePartToDetachedAndRemoveFromMemory: removing {} from data_parts_indexes", part->getNameWithState());
    data_parts_indexes.erase(it_part);

    if (removed_active_part || restored_active_part)
        resetObjectColumnsFromActiveParts(lock);
}


bool MergeTreeData::tryRemovePartImmediately(DataPartPtr && part)
{
    DataPartPtr part_to_delete;
    {
        auto lock = lockParts();

        auto part_name_with_state = part->getNameWithState();
        LOG_TRACE(log, "Trying to immediately remove part {}", part_name_with_state);

        if (part->getState() != DataPartState::Temporary)
        {
            auto it = data_parts_by_info.find(part->info);
            if (it == data_parts_by_info.end() || (*it).get() != part.get())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} doesn't exist", part->name);

            part.reset();

            if (!((*it)->getState() == DataPartState::Outdated && isSharedPtrUnique(*it)))
            {
                if ((*it)->getState() != DataPartState::Outdated)
                    LOG_WARNING(log, "Cannot immediately remove part {} because it's not in Outdated state "
                             "usage counter {}", part_name_with_state, it->use_count());

                if (!isSharedPtrUnique(*it))
                    LOG_WARNING(log, "Cannot immediately remove part {} because someone using it right now "
                             "usage counter {}", part_name_with_state, it->use_count());
                return false;
            }

            modifyPartState(it, DataPartState::Deleting);

            part_to_delete = *it;
        }
        else
        {
            part_to_delete = std::move(part);
        }
    }

    try
    {
        asMutableDeletingPart(part_to_delete)->remove();
    }
    catch (...)
    {
        rollbackDeletingParts({part_to_delete});
        throw;
    }

    removePartsFinally({part_to_delete});
    LOG_TRACE(log, "Removed part {}", part_to_delete->name);
    return true;
}


size_t MergeTreeData::getTotalActiveSizeInBytes() const
{
    return total_active_size_bytes.load();
}


size_t MergeTreeData::getTotalActiveSizeInRows() const
{
    return total_active_size_rows.load();
}


size_t MergeTreeData::getActivePartsCount() const
{
    return total_active_size_parts.load();
}


size_t MergeTreeData::getOutdatedPartsCount() const
{
    return total_outdated_parts_count.load();
}

size_t MergeTreeData::getNumberOfOutdatedPartsWithExpiredRemovalTime() const
{
    size_t res = 0;

    auto time_now = time(nullptr);

    auto parts_lock = lockParts();
    auto outdated_parts_range = getDataPartsStateRange(DataPartState::Outdated);
    for (const auto & part : outdated_parts_range)
    {
        auto part_remove_time = part->remove_time.load(std::memory_order_relaxed);
        if (part_remove_time <= time_now && time_now - part_remove_time >= (*getSettings())[MergeTreeSetting::old_parts_lifetime].totalSeconds() && isSharedPtrUnique(part))
            ++res;
    }

    return res;
}

std::pair<size_t, size_t> MergeTreeData::getMaxPartsCountAndSizeForPartitionWithState(DataPartState state) const
{
    auto lock = lockParts();

    size_t cur_parts_count = 0;
    size_t cur_parts_size = 0;
    size_t max_parts_count = 0;
    size_t argmax_parts_size = 0;

    const String * cur_partition_id = nullptr;

    for (const auto & part : getDataPartsStateRange(state))
    {
        if (!cur_partition_id || part->info.partition_id != *cur_partition_id)
        {
            cur_partition_id = &part->info.partition_id;
            cur_parts_count = 0;
            cur_parts_size = 0;
        }

        ++cur_parts_count;
        cur_parts_size += part->getBytesOnDisk();

        if (cur_parts_count > max_parts_count)
        {
            max_parts_count = cur_parts_count;
            argmax_parts_size = cur_parts_size;
        }
    }

    return {max_parts_count, argmax_parts_size};
}


std::pair<size_t, size_t> MergeTreeData::getMaxPartsCountAndSizeForPartition() const
{
    return getMaxPartsCountAndSizeForPartitionWithState(DataPartState::Active);
}


size_t MergeTreeData::getMaxOutdatedPartsCountForPartition() const
{
    return getMaxPartsCountAndSizeForPartitionWithState(DataPartState::Outdated).first;
}


std::optional<Int64> MergeTreeData::getMinPartDataVersion() const
{
    auto lock = lockParts();

    std::optional<Int64> result;
    for (const auto & part : getDataPartsStateRange(DataPartState::Active))
    {
        if (!result || *result > part->info.getDataVersion())
            result = part->info.getDataVersion();
    }

    return result;
}


void MergeTreeData::delayInsertOrThrowIfNeeded(Poco::Event * until, const ContextPtr & query_context, bool allow_throw) const
{
    const auto settings = getSettings();
    const auto & query_settings = query_context->getSettingsRef();
    const size_t parts_count_in_total = getActivePartsCount();

    /// Check if we have too many parts in total
    if (allow_throw && parts_count_in_total >= (*settings)[MergeTreeSetting::max_parts_in_total])
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception(
            ErrorCodes::TOO_MANY_PARTS,
            "Too many parts ({}) in all partitions in total in table '{}'. This indicates wrong choice of partition key. The threshold can be modified "
            "with 'max_parts_in_total' setting in <merge_tree> element in config.xml or with per-table setting.",
            parts_count_in_total, getLogName());
    }

    size_t outdated_parts_over_threshold = 0;
    {
        size_t outdated_parts_count_in_partition = 0;
        if ((*settings)[MergeTreeSetting::inactive_parts_to_throw_insert] > 0 || (*settings)[MergeTreeSetting::inactive_parts_to_delay_insert] > 0)
            outdated_parts_count_in_partition = getMaxOutdatedPartsCountForPartition();

        if (allow_throw && (*settings)[MergeTreeSetting::inactive_parts_to_throw_insert] > 0 && outdated_parts_count_in_partition >= (*settings)[MergeTreeSetting::inactive_parts_to_throw_insert])
        {
            ProfileEvents::increment(ProfileEvents::RejectedInserts);
            throw Exception(
                ErrorCodes::TOO_MANY_PARTS,
                "Too many inactive parts ({}) in table '{}'. Parts cleaning are processing significantly slower than inserts",
                outdated_parts_count_in_partition, getLogName());
        }
        if ((*settings)[MergeTreeSetting::inactive_parts_to_delay_insert] > 0 && outdated_parts_count_in_partition >= (*settings)[MergeTreeSetting::inactive_parts_to_delay_insert])
            outdated_parts_over_threshold = outdated_parts_count_in_partition - (*settings)[MergeTreeSetting::inactive_parts_to_delay_insert] + 1;
    }

    auto [parts_count_in_partition, size_of_partition] = getMaxPartsCountAndSizeForPartition();
    size_t average_part_size = parts_count_in_partition ? size_of_partition / parts_count_in_partition : 0;
    const auto active_parts_to_delay_insert
        = query_settings[Setting::parts_to_delay_insert] ? query_settings[Setting::parts_to_delay_insert] : (*settings)[MergeTreeSetting::parts_to_delay_insert];
    const auto active_parts_to_throw_insert
        = query_settings[Setting::parts_to_throw_insert] ? query_settings[Setting::parts_to_throw_insert] : (*settings)[MergeTreeSetting::parts_to_throw_insert];
    size_t active_parts_over_threshold = 0;

    {
        bool parts_are_large_enough_in_average
            = (*settings)[MergeTreeSetting::max_avg_part_size_for_too_many_parts] && average_part_size > (*settings)[MergeTreeSetting::max_avg_part_size_for_too_many_parts];

        if (allow_throw && parts_count_in_partition >= active_parts_to_throw_insert && !parts_are_large_enough_in_average)
        {
            ProfileEvents::increment(ProfileEvents::RejectedInserts);
            throw Exception(
                ErrorCodes::TOO_MANY_PARTS,
                "Too many parts ({} with average size of {}) in table '{}'. Merges are processing significantly slower than inserts",
                parts_count_in_partition,
                ReadableSize(average_part_size),
                getLogName());
        }
        if (active_parts_to_delay_insert > 0 && parts_count_in_partition >= active_parts_to_delay_insert
            && !parts_are_large_enough_in_average)
            /// if parts_count == parts_to_delay_insert -> we're 1 part over threshold
            active_parts_over_threshold = parts_count_in_partition - active_parts_to_delay_insert + 1;
    }

    /// no need for delay
    if (!active_parts_over_threshold && !outdated_parts_over_threshold)
        return;

    UInt64 delay_milliseconds = 0;
    {
        size_t parts_over_threshold = 0;
        size_t allowed_parts_over_threshold = 1;
        const bool use_active_parts_threshold = (active_parts_over_threshold >= outdated_parts_over_threshold);
        if (use_active_parts_threshold)
        {
            parts_over_threshold = active_parts_over_threshold;
            allowed_parts_over_threshold = active_parts_to_throw_insert - active_parts_to_delay_insert;
        }
        else
        {
            parts_over_threshold = outdated_parts_over_threshold;
            allowed_parts_over_threshold = outdated_parts_over_threshold; /// if throw threshold is not set, will use max delay
            if ((*settings)[MergeTreeSetting::inactive_parts_to_throw_insert] > 0)
                allowed_parts_over_threshold = (*settings)[MergeTreeSetting::inactive_parts_to_throw_insert] - (*settings)[MergeTreeSetting::inactive_parts_to_delay_insert];
        }

        const UInt64 max_delay_milliseconds = ((*settings)[MergeTreeSetting::max_delay_to_insert] > 0 ? (*settings)[MergeTreeSetting::max_delay_to_insert] * 1000 : 1000);
        if (allowed_parts_over_threshold == 0 || parts_over_threshold > allowed_parts_over_threshold)
        {
            delay_milliseconds = max_delay_milliseconds;
        }
        else
        {
            double delay_factor = static_cast<double>(parts_over_threshold) / allowed_parts_over_threshold;
            const UInt64 min_delay_milliseconds = (*settings)[MergeTreeSetting::min_delay_to_insert_ms];
            delay_milliseconds = std::max(min_delay_milliseconds, static_cast<UInt64>(max_delay_milliseconds * delay_factor));
        }
    }

    ProfileEvents::increment(ProfileEvents::DelayedInserts);
    ProfileEvents::increment(ProfileEvents::DelayedInsertsMilliseconds, delay_milliseconds);

    CurrentMetrics::Increment metric_increment(CurrentMetrics::DelayedInserts);

    LOG_INFO(log, "Delaying inserting block by {} ms. because there are {} parts and their average size is {}",
        delay_milliseconds, parts_count_in_partition, ReadableSize(average_part_size));

    if (until)
        until->tryWait(delay_milliseconds);
    else
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<size_t>(delay_milliseconds)));
}

void MergeTreeData::delayMutationOrThrowIfNeeded(Poco::Event * until, const ContextPtr & query_context) const
{
    const auto settings = getSettings();
    const auto & query_settings = query_context->getSettingsRef();

    size_t num_mutations_to_delay = query_settings[Setting::number_of_mutations_to_delay] ? query_settings[Setting::number_of_mutations_to_delay]
                                                                                 : (*settings)[MergeTreeSetting::number_of_mutations_to_delay];

    size_t num_mutations_to_throw = query_settings[Setting::number_of_mutations_to_throw] ? query_settings[Setting::number_of_mutations_to_throw]
                                                                                 : (*settings)[MergeTreeSetting::number_of_mutations_to_throw];

    if (!num_mutations_to_delay && !num_mutations_to_throw)
        return;

    size_t num_unfinished_mutations = getUnfinishedMutationCommands().size();
    if (num_mutations_to_throw && num_unfinished_mutations >= num_mutations_to_throw)
    {
        ProfileEvents::increment(ProfileEvents::RejectedMutations);
        throw Exception(ErrorCodes::TOO_MANY_MUTATIONS,
            "Too many unfinished mutations ({}) in table {}",
            num_unfinished_mutations, getLogName());
    }

    if (num_mutations_to_delay && num_unfinished_mutations >= num_mutations_to_delay)
    {
        if (!num_mutations_to_throw)
            num_mutations_to_throw = num_mutations_to_delay * 2;

        size_t mutations_over_threshold = num_unfinished_mutations - num_mutations_to_delay;
        size_t allowed_mutations_over_threshold = num_mutations_to_throw - num_mutations_to_delay;

        double delay_factor = std::min(static_cast<double>(mutations_over_threshold) / allowed_mutations_over_threshold, 1.0);
        size_t delay_milliseconds = static_cast<size_t>(interpolateLinear((*settings)[MergeTreeSetting::min_delay_to_mutate_ms], (*settings)[MergeTreeSetting::max_delay_to_mutate_ms], delay_factor));

        ProfileEvents::increment(ProfileEvents::DelayedMutations);
        ProfileEvents::increment(ProfileEvents::DelayedMutationsMilliseconds, delay_milliseconds);

        if (until)
            until->tryWait(delay_milliseconds);
        else
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_milliseconds));
    }
}

MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(
    const MergeTreePartInfo & part_info, MergeTreeData::DataPartState state, DataPartsLock & /*lock*/) const
{
    auto current_state_parts_range = getDataPartsStateRange(state);

    /// The part can be covered only by the previous or the next one in data_parts.
    auto it = data_parts_by_state_and_info.lower_bound(DataPartStateAndInfo{state, part_info});

    if (it != current_state_parts_range.end())
    {
        if ((*it)->info == part_info)
            return *it;
        if ((*it)->info.contains(part_info))
            return *it;
    }

    if (it != current_state_parts_range.begin())
    {
        --it;
        if ((*it)->info.contains(part_info))
            return *it;
    }

    return nullptr;
}


void MergeTreeData::swapActivePart(MergeTreeData::DataPartPtr part_copy, DataPartsLock &)
{
    for (auto original_active_part : getDataPartsStateRange(DataPartState::Active)) // NOLINT (copy is intended)
    {
        if (part_copy->name == original_active_part->name)
        {
            auto active_part_it = data_parts_by_info.find(original_active_part->info);
            if (active_part_it == data_parts_by_info.end())
                throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Cannot swap part '{}', no such active part.", part_copy->name);

            /// We do not check allow_remote_fs_zero_copy_replication here because data may be shared
            /// when allow_remote_fs_zero_copy_replication turned on and off again
            original_active_part->force_keep_shared_data = false;

            if (original_active_part->getDataPartStorage().supportZeroCopyReplication() &&
                part_copy->getDataPartStorage().supportZeroCopyReplication() &&
                original_active_part->getDataPartStorage().getUniqueId() == part_copy->getDataPartStorage().getUniqueId())
            {
                /// May be when several volumes use the same S3/HDFS storage
                original_active_part->force_keep_shared_data = true;
            }

            modifyPartState(original_active_part, DataPartState::DeleteOnDestroy);
            LOG_TEST(log, "swapActivePart: removing {} from data_parts_indexes", (*active_part_it)->getNameWithState());
            data_parts_indexes.erase(active_part_it);

            LOG_TEST(log, "swapActivePart: inserting {} into data_parts_indexes", part_copy->getNameWithState());
            auto part_it = data_parts_indexes.insert(part_copy).first;
            modifyPartState(part_it, DataPartState::Active);

            ssize_t diff_bytes = part_copy->getBytesOnDisk() - original_active_part->getBytesOnDisk();
            ssize_t diff_rows = part_copy->rows_count - original_active_part->rows_count;
            increaseDataVolume(diff_bytes, diff_rows, /* parts= */ 0);

            /// Move parts are non replicated operations, so we take lock here.
            /// All other locks are taken in StorageReplicatedMergeTree
            lockSharedData(*part_copy, /* replace_existing_lock */ true);

            return;
        }
    }
    throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Cannot swap part '{}', no such active part.", part_copy->name);
}


MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(const MergeTreePartInfo & part_info) const
{
    auto lock = lockParts();
    return getActiveContainingPart(part_info, DataPartState::Active, lock);
}

MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(const String & part_name) const
{
    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
    return getActiveContainingPart(part_info);
}

MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(const String & part_name, DataPartsLock & lock) const
{
    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
    return getActiveContainingPart(part_info, DataPartState::Active, lock);
}

MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVectorInPartition(ContextPtr local_context, const String & partition_id) const
{
    return getVisibleDataPartsVectorInPartition(local_context->getCurrentTransaction().get(), partition_id);
}


MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVectorInPartition(
    ContextPtr local_context, const String & partition_id, DataPartsLock & lock) const
{
    return getVisibleDataPartsVectorInPartition(local_context->getCurrentTransaction().get(), partition_id, &lock);
}

MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVectorInPartition(
    MergeTreeTransaction * txn, const String & partition_id, DataPartsLock * acquired_lock) const
{
    if (txn)
    {
        DataPartStateAndPartitionID active_parts{MergeTreeDataPartState::Active, partition_id};
        DataPartStateAndPartitionID outdated_parts{MergeTreeDataPartState::Outdated, partition_id};
        DataPartsVector res;
        {
            auto lock = (acquired_lock) ? DataPartsLock() : lockParts();
            res.insert(res.end(), data_parts_by_state_and_info.lower_bound(active_parts), data_parts_by_state_and_info.upper_bound(active_parts));
            res.insert(res.end(), data_parts_by_state_and_info.lower_bound(outdated_parts), data_parts_by_state_and_info.upper_bound(outdated_parts));
        }
        filterVisibleDataParts(res, txn->getSnapshot(), txn->tid);
        return res;
    }

    return getDataPartsVectorInPartitionForInternalUsage(MergeTreeDataPartState::Active, partition_id, acquired_lock);
}


MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorInPartitionForInternalUsage(const DataPartStates & affordable_states, const String & partition_id, DataPartsLock * acquired_lock) const
{
    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();
    DataPartsVector res;
    for (const auto & state : affordable_states)
    {
        DataPartStateAndPartitionID state_with_partition{state, partition_id};
        res.insert(res.end(), data_parts_by_state_and_info.lower_bound(state_with_partition), data_parts_by_state_and_info.upper_bound(state_with_partition));
    }
    return res;
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorInPartitionForInternalUsage(
    const MergeTreeData::DataPartState & state, const String & partition_id, DataPartsLock * acquired_lock) const
{
    DataPartStateAndPartitionID state_with_partition{state, partition_id};

    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();
    return DataPartsVector(
        data_parts_by_state_and_info.lower_bound(state_with_partition),
        data_parts_by_state_and_info.upper_bound(state_with_partition));
}

MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVectorInPartitions(ContextPtr local_context, const std::unordered_set<String> & partition_ids) const
{
    auto txn = local_context->getCurrentTransaction();
    DataPartsVector res;
    {
        auto lock = lockParts();
        for (const auto & partition_id : partition_ids)
        {
            DataPartStateAndPartitionID active_parts{MergeTreeDataPartState::Active, partition_id};
            insertAtEnd(
                res,
                DataPartsVector(
                    data_parts_by_state_and_info.lower_bound(active_parts),
                    data_parts_by_state_and_info.upper_bound(active_parts)));

            if (txn)
            {
                DataPartStateAndPartitionID outdated_parts{MergeTreeDataPartState::Active, partition_id};

                insertAtEnd(
                    res,
                    DataPartsVector(
                        data_parts_by_state_and_info.lower_bound(outdated_parts),
                        data_parts_by_state_and_info.upper_bound(outdated_parts)));
            }
        }
    }

    if (txn)
        filterVisibleDataParts(res, txn->getSnapshot(), txn->tid);

    return res;
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExists(const MergeTreePartInfo & part_info, const MergeTreeData::DataPartStates & valid_states) const
{
    auto lock = lockParts();
    return getPartIfExistsUnlocked(part_info, valid_states, lock);
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExists(const String & part_name, const MergeTreeData::DataPartStates & valid_states) const
{
    auto lock = lockParts();
    return getPartIfExistsUnlocked(part_name, valid_states, lock);
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExistsUnlocked(const String & part_name, const DataPartStates & valid_states, DataPartsLock & acquired_lock) const
{
    return getPartIfExistsUnlocked(MergeTreePartInfo::fromPartName(part_name, format_version), valid_states, acquired_lock);
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExistsUnlocked(const MergeTreePartInfo & part_info, const DataPartStates & valid_states, DataPartsLock & /* acquired_lock */) const
{
    auto it = data_parts_by_info.find(part_info);
    if (it == data_parts_by_info.end())
        return nullptr;

    for (auto state : valid_states)
        if ((*it)->getState() == state)
            return *it;

    return nullptr;
}

static void loadPartAndFixMetadataImpl(MergeTreeData::MutableDataPartPtr part, ContextPtr local_context, int32_t metadata_version, bool sync)
{
    /// Remove metadata version file and take it from table.
    /// Currently we cannot attach parts with different schema, so
    /// we can assume that it's equal to table's current schema.
    part->removeMetadataVersion();
    {
        auto out_metadata = part->getDataPartStorage().writeFile(IMergeTreeDataPart::METADATA_VERSION_FILE_NAME, 4096, local_context->getWriteSettings());
        writeText(metadata_version, *out_metadata);
        out_metadata->finalize();
        if (sync)
            out_metadata->sync();
    }

    part->loadColumnsChecksumsIndexes(false, true);
    part->modification_time = part->getDataPartStorage().getLastModified().epochTime();
    part->removeDeleteOnDestroyMarker();
    part->removeVersionMetadata();
}

void MergeTreeData::calculateColumnAndSecondaryIndexSizesImpl()
{
    column_sizes.clear();

    /// Take into account only committed parts
    auto committed_parts_range = getDataPartsStateRange(DataPartState::Active);
    for (const auto & part : committed_parts_range)
        addPartContributionToColumnAndSecondaryIndexSizes(part);
}

void MergeTreeData::addPartContributionToColumnAndSecondaryIndexSizes(const DataPartPtr & part)
{
    for (const auto & column : part->getColumns())
    {
        ColumnSize & total_column_size = column_sizes[column.name];
        ColumnSize part_column_size = part->getColumnSize(column.name);
        total_column_size.add(part_column_size);
    }

    const auto metadata_snapshot = getInMemoryMetadataPtr();
    auto indexes_descriptions = metadata_snapshot->secondary_indices;
    for (const auto & index : indexes_descriptions)
    {
        IndexSize & total_secondary_index_size = secondary_index_sizes[index.name];
        IndexSize part_index_size = part->getSecondaryIndexSize(index.name);
        total_secondary_index_size.add(part_index_size);
    }
}

void MergeTreeData::removePartContributionToColumnAndSecondaryIndexSizes(const DataPartPtr & part)
{
    for (const auto & column : part->getColumns())
    {
        ColumnSize & total_column_size = column_sizes[column.name];
        ColumnSize part_column_size = part->getColumnSize(column.name);

        auto log_subtract = [&](size_t & from, size_t value, const char * field)
        {
            if (value > from)
                LOG_ERROR(log, "Possibly incorrect column size subtraction: {} - {} = {}, column: {}, field: {}",
                    from, value, from - value, column.name, field);

            from -= value;
        };

        log_subtract(total_column_size.data_compressed, part_column_size.data_compressed, ".data_compressed");
        log_subtract(total_column_size.data_uncompressed, part_column_size.data_uncompressed, ".data_uncompressed");
        log_subtract(total_column_size.marks, part_column_size.marks, ".marks");
    }

    for (auto & [secondary_index_name, total_secondary_index_size] : secondary_index_sizes)
    {
        if (!part->hasSecondaryIndex(secondary_index_name))
            continue;

        IndexSize part_secondary_index_size = part->getSecondaryIndexSize(secondary_index_name);

        auto log_subtract = [&](size_t & from, size_t value, const char * field)
        {
            if (value > from)
                LOG_ERROR(log, "Possibly incorrect index size subtraction: {} - {} = {}, index: {}, field: {}",
                    from, value, from - value, secondary_index_name, field);

            from -= value;
        };

        log_subtract(total_secondary_index_size.data_compressed, part_secondary_index_size.data_compressed, ".data_compressed");
        log_subtract(total_secondary_index_size.data_uncompressed, part_secondary_index_size.data_uncompressed, ".data_uncompressed");
        log_subtract(total_secondary_index_size.marks, part_secondary_index_size.marks, ".marks");
    }
}

void MergeTreeData::checkAlterPartitionIsPossible(
    const PartitionCommands & commands, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & settings, ContextPtr local_context) const
{
    for (const auto & disk : getDisks())
        if (!disk->supportsHardLinks())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED, "ALTER TABLE PARTITION is not supported for immutable disk '{}'", disk->getName());

    for (const auto & command : commands)
    {
        if (command.type == PartitionCommand::DROP_DETACHED_PARTITION && !settings[Setting::allow_drop_detached])
            throw DB::Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                "Cannot execute query: DROP DETACHED PART "
                                "is disabled (see allow_drop_detached setting)");

        if (command.partition && command.type != PartitionCommand::DROP_DETACHED_PARTITION)
        {
            if (command.part)
            {
                auto part_name = command.partition->as<ASTLiteral &>().value.safeGet<String>();
                /// We are able to parse it
                MergeTreePartInfo::fromPartName(part_name, format_version);
            }
            else
            {
                /// We are able to parse it
                const auto * partition_ast = command.partition->as<ASTPartition>();
                if (partition_ast && partition_ast->all)
                {
                    if (command.type != PartitionCommand::DROP_PARTITION && command.type != PartitionCommand::ATTACH_PARTITION && !(command.type == PartitionCommand::REPLACE_PARTITION && !command.replace))
                        throw DB::Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Only support DROP/DETACH/ATTACH PARTITION ALL currently");
                }
                else
                {
                    String partition_id = getPartitionIDFromQuery(command.partition, local_context);
                    if (command.type == PartitionCommand::FORGET_PARTITION)
                    {
                        DataPartsLock lock = lockParts();
                        auto parts_in_partition = getDataPartsPartitionRange(partition_id);
                        if (!parts_in_partition.empty())
                            throw Exception(ErrorCodes::CANNOT_FORGET_PARTITION, "Partition {} is not empty", partition_id);
                    }
                }
            }
        }
    }
}

void MergeTreeData::checkPartitionCanBeDropped(const ASTPtr & partition, ContextPtr local_context)
{
    if (!supportsReplication() && isStaticStorage())
        return;

    DataPartsVector parts_to_remove;
    const auto * partition_ast = partition->as<ASTPartition>();
    if (partition_ast && partition_ast->all)
        parts_to_remove = getVisibleDataPartsVector(local_context);
    else
    {
        const String partition_id = getPartitionIDFromQuery(partition, local_context);
        parts_to_remove = getVisibleDataPartsVectorInPartition(local_context, partition_id);
    }
    UInt64 partition_size = 0;

    for (const auto & part : parts_to_remove)
        partition_size += part->getBytesOnDisk();

    auto table_id = getStorageID();

    const auto & query_settings = local_context->getSettingsRef();
    if (query_settings[Setting::max_partition_size_to_drop].changed)
    {
        getContext()->checkPartitionCanBeDropped(
            table_id.database_name, table_id.table_name, partition_size, query_settings[Setting::max_partition_size_to_drop]);
        return;
    }

    getContext()->checkPartitionCanBeDropped(table_id.database_name, table_id.table_name, partition_size);
}

void MergeTreeData::checkPartCanBeDropped(const String & part_name, ContextPtr local_context)
{
    if (!supportsReplication() && isStaticStorage())
        return;

    auto part = getPartIfExists(part_name, {MergeTreeDataPartState::Active});
    if (!part)
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No part {} in committed state", part_name);

    auto table_id = getStorageID();

    const auto & query_settings = local_context->getSettingsRef();
    if (query_settings[Setting::max_partition_size_to_drop].changed)
    {
        getContext()->checkPartitionCanBeDropped(
            table_id.database_name, table_id.table_name, part->getBytesOnDisk(), query_settings[Setting::max_partition_size_to_drop]);
        return;
    }

    getContext()->checkPartitionCanBeDropped(table_id.database_name, table_id.table_name, part->getBytesOnDisk());
}

void MergeTreeData::movePartitionToDisk(const ASTPtr & partition, const String & name, bool moving_part, ContextPtr local_context)
{
    String partition_id;

    if (moving_part)
        partition_id = partition->as<ASTLiteral &>().value.safeGet<String>();
    else
        partition_id = getPartitionIDFromQuery(partition, local_context);

    DataPartsVector parts;
    if (moving_part)
    {
        auto part_info = MergeTreePartInfo::fromPartName(partition_id, format_version);
        parts.push_back(getActiveContainingPart(part_info));
        if (!parts.back() || parts.back()->name != part_info.getPartNameAndCheckFormat(format_version))
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Part {} is not exists or not active", partition_id);
    }
    else
        parts = getVisibleDataPartsVectorInPartition(local_context, partition_id);

    auto disk = getStoragePolicy()->getDiskByName(name);
    std::erase_if(parts, [&](auto part_ptr)
        {
            return part_ptr->getDataPartStorage().getDiskName() == disk->getName();
        });

    if (parts.empty())
    {
        if (moving_part)
            throw Exception(ErrorCodes::UNKNOWN_DISK, "Part '{}' is already on disk '{}'", partition_id, disk->getName());

        throw Exception(ErrorCodes::UNKNOWN_DISK, "All parts of partition '{}' are already on disk '{}'", partition_id, disk->getName());
    }

    if (parts_mover.moves_blocker.isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Cannot move parts because moves are manually disabled");

    auto moving_tagger = checkPartsForMove(parts, std::static_pointer_cast<Space>(disk));
    if (moving_tagger->parts_to_move.empty())
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No parts to move are found in partition {}", partition_id);

    const auto & query_settings = local_context->getSettingsRef();
    std::future<MovePartsOutcome> moves_future = movePartsToSpace(
        moving_tagger,
        local_context->getReadSettings(),
        local_context->getWriteSettings(),
        query_settings[Setting::alter_move_to_space_execute_async]);

    if (query_settings[Setting::alter_move_to_space_execute_async] && moves_future.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
    {
        return;
    }

    auto moves_outcome = moves_future.get();
    switch (moves_outcome)
    {
        case MovePartsOutcome::MovesAreCancelled:
            throw Exception(ErrorCodes::ABORTED, "Cannot move parts because moves are manually disabled");
        case MovePartsOutcome::NothingToMove:
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No parts to move are found in partition {}", partition_id);
        case MovePartsOutcome::MoveWasPostponedBecauseOfZeroCopy:
            throw Exception(ErrorCodes::PART_IS_TEMPORARILY_LOCKED, "Move was not finished, because zero copy mode is enabled and someone other is moving the same parts right now");
        case MovePartsOutcome::CannotScheduleMove:
            throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot schedule move, no free threads, try to wait until all in-progress move finish or increase <background_move_pool_size>");
        case MovePartsOutcome::PartsMoved:
            break;
    }
}


void MergeTreeData::movePartitionToVolume(const ASTPtr & partition, const String & name, bool moving_part, ContextPtr local_context)
{
    String partition_id;

    if (moving_part)
        partition_id = partition->as<ASTLiteral &>().value.safeGet<String>();
    else
        partition_id = getPartitionIDFromQuery(partition, local_context);

    DataPartsVector parts;
    if (moving_part)
    {
        auto part_info = MergeTreePartInfo::fromPartName(partition_id, format_version);
        parts.emplace_back(getActiveContainingPart(part_info));
        if (!parts.back() || parts.back()->name != part_info.getPartNameAndCheckFormat(format_version))
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Part {} is not exists or not active", partition_id);
    }
    else
        parts = getVisibleDataPartsVectorInPartition(local_context, partition_id);

    auto volume = getStoragePolicy()->getVolumeByName(name);
    if (!volume)
        throw Exception(ErrorCodes::UNKNOWN_DISK, "Volume {} does not exist on policy {}", name, getStoragePolicy()->getName());

    if (parts.empty())
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Nothing to move (check that the partition exists).");

    std::erase_if(parts, [&](auto part_ptr)
        {
            for (const auto & disk : volume->getDisks())
            {
                if (part_ptr->getDataPartStorage().getDiskName() == disk->getName())
                {
                    return true;
                }
            }
            return false;
        });

    if (parts.empty())
    {
        if (moving_part)
            throw Exception(ErrorCodes::UNKNOWN_DISK, "Part '{}' is already on volume '{}'", partition_id, volume->getName());

        throw Exception(ErrorCodes::UNKNOWN_DISK, "All parts of partition '{}' are already on volume '{}'", partition_id, volume->getName());
    }

    if (parts_mover.moves_blocker.isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Cannot move parts because moves are manually disabled");

    auto moving_tagger = checkPartsForMove(parts, std::static_pointer_cast<Space>(volume));
    if (moving_tagger->parts_to_move.empty())
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No parts to move are found in partition {}", partition_id);

    const auto & query_settings = local_context->getSettingsRef();
    std::future<MovePartsOutcome> moves_future = movePartsToSpace(
        moving_tagger,
        local_context->getReadSettings(),
        local_context->getWriteSettings(),
        query_settings[Setting::alter_move_to_space_execute_async]);

    if (query_settings[Setting::alter_move_to_space_execute_async] && moves_future.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
    {
        return;
    }

    auto moves_outcome = moves_future.get();
    switch (moves_outcome)
    {
        case MovePartsOutcome::MovesAreCancelled:
            throw Exception(ErrorCodes::ABORTED, "Cannot move parts because moves are manually disabled");
        case MovePartsOutcome::NothingToMove:
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No parts to move are found in partition {}", partition_id);
        case MovePartsOutcome::MoveWasPostponedBecauseOfZeroCopy:
            throw Exception(ErrorCodes::PART_IS_TEMPORARILY_LOCKED, "Move was not finished, because zero copy mode is enabled and someone other is moving the same parts right now");
        case MovePartsOutcome::CannotScheduleMove:
            throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot schedule move, no free threads, try to wait until all in-progress move finish or increase <background_move_pool_size>");
        case MovePartsOutcome::PartsMoved:
            break;
    }
}

void MergeTreeData::movePartitionToTable(const PartitionCommand & command, ContextPtr query_context)
{
    String dest_database = query_context->resolveDatabase(command.to_database);
    auto dest_storage = DatabaseCatalog::instance().getTable({dest_database, command.to_table}, query_context);

    /// The target table and the source table are the same.
    if (dest_storage->getStorageID() == this->getStorageID())
        return;

    auto * dest_storage_merge_tree = dynamic_cast<MergeTreeData *>(dest_storage.get());
    if (!dest_storage_merge_tree)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot move partition from table {} to table {} with storage {}",
            getStorageID().getNameForLogs(), dest_storage->getStorageID().getNameForLogs(), dest_storage->getName());

    dest_storage_merge_tree->waitForOutdatedPartsToBeLoaded();
    movePartitionToTable(dest_storage, command.partition, query_context);
}

void MergeTreeData::movePartitionToShard(const ASTPtr & /*partition*/, bool /*move_part*/, const String & /*to*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MOVE PARTITION TO SHARD is not supported by storage {}", getName());
}

void MergeTreeData::fetchPartition(
    const ASTPtr & /*partition*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const String & /*from*/,
    bool /*fetch_part*/,
    ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "FETCH PARTITION is not supported by storage {}", getName());
}

void MergeTreeData::forgetPartition(const ASTPtr & /*partition*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "FORGET PARTITION is not supported by storage {}", getName());
}

Pipe MergeTreeData::alterPartition(
    const StorageMetadataPtr & metadata_snapshot,
    const PartitionCommands & commands,
    ContextPtr query_context)
{
    /// Wait for loading of outdated parts
    /// because partition commands (DROP, MOVE, etc.)
    /// must be applied to all parts on disk.
    waitForOutdatedPartsToBeLoaded();

    PartitionCommandsResultInfo result;
    for (const PartitionCommand & command : commands)
    {
        PartitionCommandsResultInfo current_command_results;
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
            {
                if (command.part)
                {
                    auto part_name = command.partition->as<ASTLiteral &>().value.safeGet<String>();
                    checkPartCanBeDropped(part_name, query_context);
                    dropPart(part_name, command.detach, query_context);
                }
                else
                {
                    checkPartitionCanBeDropped(command.partition, query_context);
                    dropPartition(command.partition, command.detach, query_context);
                }
            }
            break;

            case PartitionCommand::DROP_DETACHED_PARTITION:
                dropDetached(command.partition, command.part, query_context);
                break;

            case PartitionCommand::FORGET_PARTITION:
                forgetPartition(command.partition, query_context);
                break;

            case PartitionCommand::ATTACH_PARTITION:
                current_command_results = attachPartition(command.partition, metadata_snapshot, command.part, query_context);
                break;
            case PartitionCommand::MOVE_PARTITION:
            {
                switch (*command.move_destination_type)
                {
                    case PartitionCommand::MoveDestinationType::DISK:
                        movePartitionToDisk(command.partition, command.move_destination_name, command.part, query_context);
                        break;

                    case PartitionCommand::MoveDestinationType::VOLUME:
                        movePartitionToVolume(command.partition, command.move_destination_name, command.part, query_context);
                        break;

                    case PartitionCommand::MoveDestinationType::TABLE:
                        movePartitionToTable(command, query_context);
                        break;

                    case PartitionCommand::MoveDestinationType::SHARD:
                    {
                        if (!(*getSettings())[MergeTreeSetting::part_moves_between_shards_enable])
                            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                            "Moving parts between shards is experimental and work in progress"
                                            ", see part_moves_between_shards_enable setting");
                        movePartitionToShard(command.partition, command.part, command.move_destination_name, query_context);
                    }
                    break;
                }
            }
            break;

            case PartitionCommand::REPLACE_PARTITION:
            {
                if (command.replace)
                    checkPartitionCanBeDropped(command.partition, query_context);

                auto resolved = query_context->resolveStorageID({command.from_database, command.from_table});
                auto from_storage = DatabaseCatalog::instance().getTable(resolved, query_context);

                auto * from_storage_merge_tree = dynamic_cast<MergeTreeData *>(from_storage.get());
                if (!from_storage_merge_tree)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot replace partition from table {} with storage {} to table {}",
                        from_storage->getStorageID().getNameForLogs(), from_storage->getName(), getStorageID().getNameForLogs());

                from_storage_merge_tree->waitForOutdatedPartsToBeLoaded();
                replacePartitionFrom(from_storage, command.partition, command.replace, query_context);
            }
            break;

            case PartitionCommand::FETCH_PARTITION:
                fetchPartition(command.partition, metadata_snapshot, command.from_zookeeper_path, command.part, query_context);
                break;

            case PartitionCommand::FREEZE_PARTITION:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef()[Setting::lock_acquire_timeout]);
                current_command_results = freezePartition(command.partition, command.with_name, query_context, lock);
            }
            break;

            case PartitionCommand::FREEZE_ALL_PARTITIONS:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef()[Setting::lock_acquire_timeout]);
                current_command_results = freezeAll(command.with_name, query_context, lock);
            }
            break;

            case PartitionCommand::UNFREEZE_PARTITION:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef()[Setting::lock_acquire_timeout]);
                current_command_results = unfreezePartition(command.partition, command.with_name, query_context, lock);
            }
            break;

            case PartitionCommand::UNFREEZE_ALL_PARTITIONS:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef()[Setting::lock_acquire_timeout]);
                current_command_results = unfreezeAll(command.with_name, query_context, lock);
            }

            break;

            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Uninitialized partition command");
        }
        for (auto & command_result : current_command_results)
            command_result.command_type = command.typeToString();
        result.insert(result.end(), current_command_results.begin(), current_command_results.end());
    }

    if (query_context->getSettingsRef()[Setting::alter_partition_verbose_result])
        return convertCommandsResultToSource(result);

    return {};
}

MergeTreeData::PartsBackupEntries MergeTreeData::backupParts(
    const DataPartsVector & data_parts,
    const String & data_path_in_backup,
    const BackupSettings & backup_settings,
    const ReadSettings & read_settings,
    const ContextPtr & local_context)
{
    MergeTreeData::PartsBackupEntries res;
    std::map<DiskPtr, std::shared_ptr<TemporaryFileOnDisk>> temp_dirs;
    TableLockHolder table_lock;

    for (const auto & part : data_parts)
    {
        /// Hard links is the default way to ensure that we'll be keeping access to the files of parts.
        bool make_temporary_hard_links = true;
        bool hold_storage_and_part_ptrs = false;
        bool hold_table_lock = false;

        if (getStorageID().hasUUID())
        {
            /// Tables in atomic databases have UUIDs. When using atomic database we don't have to create hard links to make a backup,
            /// we can just hold smart pointers to a storage and to data parts instead. That's enough to protect those files from deleting
            /// until the backup is done (see the calls `part.unique()` in grabOldParts() and table.unique() in DatabaseCatalog).
            make_temporary_hard_links = false;
            hold_storage_and_part_ptrs = true;
        }
        else if (supportsReplication() && part->getDataPartStorage().supportZeroCopyReplication() && (*getSettings())[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
        {
            /// Hard links don't work correctly with zero copy replication.
            make_temporary_hard_links = false;
            hold_storage_and_part_ptrs = true;
            hold_table_lock = true;
        }

        if (hold_table_lock && !table_lock)
            table_lock = lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef()[Setting::lock_acquire_timeout]);

        if (backup_settings.check_projection_parts)
            part->checkConsistencyWithProjections(/* require_part_metadata= */ true);

        BackupEntries backup_entries_from_part;
        part->getDataPartStorage().backup(
            part->checksums,
            part->getFileNamesWithoutChecksums(),
            data_path_in_backup,
            backup_settings,
            read_settings,
            make_temporary_hard_links,
            backup_entries_from_part,
            &temp_dirs,
            false, false);

        auto backup_projection = [&](IDataPartStorage & storage, IMergeTreeDataPart & projection_part)
        {
            storage.backup(
                projection_part.checksums,
                projection_part.getFileNamesWithoutChecksums(),
                fs::path{data_path_in_backup} / part->name,
                backup_settings,
                read_settings,
                make_temporary_hard_links,
                backup_entries_from_part,
                &temp_dirs,
                projection_part.is_broken,
                backup_settings.allow_backup_broken_projections);
        };

        auto projection_parts = part->getProjectionParts();
        std::string proj_suffix = ".proj";
        std::unordered_set<String> defined_projections;

        for (const auto & [projection_name, projection_part] : projection_parts)
        {
            defined_projections.emplace(projection_name);
            backup_projection(projection_part->getDataPartStorage(), *projection_part);
        }

        /// It is possible that the part has a written but not loaded projection,
        /// e.g. it is written to parent part's checksums.txt and exists on disk,
        /// but does not exist in table's projections definition.
        /// Such a part can appear server was restarted after DROP PROJECTION but before old part was removed.
        /// In this case, the old part will load only projections from metadata.
        /// See 031145_non_loaded_projection_backup.sh.
        for (const auto & [name, _] : part->checksums.files)
        {
            auto projection_name = fs::path(name).stem().string();
            if (endsWith(name, proj_suffix) && !defined_projections.contains(projection_name))
            {
                auto projection_storage = part->getDataPartStorage().getProjection(projection_name + proj_suffix);
                if (projection_storage->existsFile("checksums.txt"))
                {
                    auto projection_part = const_cast<IMergeTreeDataPart &>(*part).getProjectionPartBuilder(
                        projection_name, /* is_temp_projection */false).withPartFormatFromDisk().build();
                    backup_projection(projection_part->getDataPartStorage(), *projection_part);
                }
            }
        }

        if (hold_storage_and_part_ptrs)
        {
            /// Wrap backup entries with smart pointers to data parts and to the storage itself
            /// (we'll be holding those smart pointers for as long as we'll be using the backup entries).
            auto storage_and_part = std::make_pair(shared_from_this(), part);
            if (hold_table_lock)
                wrapBackupEntriesWith(backup_entries_from_part, std::make_pair(storage_and_part, table_lock));
            else
                wrapBackupEntriesWith(backup_entries_from_part, storage_and_part);
        }

        auto & part_backup_entries = res.emplace_back();
        part_backup_entries.part_name = part->name;
        part_backup_entries.part_checksum = part->checksums.getTotalChecksumUInt128();
        part_backup_entries.backup_entries = std::move(backup_entries_from_part);
    }

    return res;
}

void MergeTreeData::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions)
{
    auto backup = restorer.getBackup();
    if (!backup->hasFiles(data_path_in_backup))
        return;

    if (!restorer.isNonEmptyTableAllowed() && getTotalActiveSizeInBytes() && backup->hasFiles(data_path_in_backup))
        RestorerFromBackup::throwTableIsNotEmpty(getStorageID());

    restorePartsFromBackup(restorer, data_path_in_backup, partitions);
}

class MergeTreeData::RestoredPartsHolder
{
public:
    RestoredPartsHolder(const std::shared_ptr<MergeTreeData> & storage_, const BackupPtr & backup_)
        : storage(storage_), backup(backup_)
    {
    }

    BackupPtr getBackup() const { return backup; }

    void setNumParts(size_t num_parts_)
    {
        std::lock_guard lock{mutex};
        num_parts = num_parts_;
        attachIfAllPartsRestored();
    }

    void increaseNumBrokenParts()
    {
        std::lock_guard lock{mutex};
        ++num_broken_parts;
        attachIfAllPartsRestored();
    }

    void addPart(MutableDataPartPtr part)
    {
        std::lock_guard lock{mutex};
        parts.emplace_back(part);
        attachIfAllPartsRestored();
    }

    String getTemporaryDirectory(const DiskPtr & disk, const String & part_name)
    {
        std::lock_guard lock{mutex};
        auto it = temp_part_dirs.find(part_name);
        if (it == temp_part_dirs.end())
        {
            auto temp_dir_deleter = std::make_unique<TemporaryFileOnDisk>(disk, fs::path{storage->getRelativeDataPath()} / ("tmp_restore_" + part_name + "-"));
            auto temp_part_dir = fs::path{temp_dir_deleter->getRelativePath()}.filename();
            /// Attaching parts will rename them so it's expected for a temporary part directory not to exist anymore in the end.
            temp_dir_deleter->setShowWarningIfRemoved(false);
            /// The following holder is needed to prevent clearOldTemporaryDirectories() from clearing `temp_part_dir` before we attach the part.
            auto temp_dir_holder = storage->getTemporaryPartDirectoryHolder(temp_part_dir);
            it = temp_part_dirs.emplace(part_name,
                                        std::make_pair(std::move(temp_dir_deleter), std::move(temp_dir_holder))).first;
        }
        return it->second.first->getRelativePath();
    }

private:
    void attachIfAllPartsRestored()
    {
        if (!num_parts || (parts.size() + num_broken_parts < num_parts))
            return;

        /// Sort parts by min_block (because we need to preserve the order of parts).
        std::sort(
            parts.begin(),
            parts.end(),
            [](const MutableDataPartPtr & lhs, const MutableDataPartPtr & rhs) { return lhs->info.min_block < rhs->info.min_block; });

        storage->attachRestoredParts(std::move(parts));
        parts.clear();
        temp_part_dirs.clear();
        num_parts = 0;
    }

    const std::shared_ptr<MergeTreeData> storage;
    const BackupPtr backup;
    size_t num_parts = 0;
    size_t num_broken_parts = 0;
    MutableDataPartsVector parts;
    std::map<String /* part_name*/, std::pair<std::unique_ptr<TemporaryFileOnDisk>, scope_guard>> temp_part_dirs;
    mutable std::mutex mutex;
};

void MergeTreeData::restorePartsFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions)
{
    std::optional<std::unordered_set<String>> partition_ids;
    if (partitions)
        partition_ids = getPartitionIDsFromQuery(*partitions, restorer.getContext());

    auto backup = restorer.getBackup();
    Strings part_names = backup->listFiles(data_path_in_backup, /*recursive*/ false);
    std::erase(part_names, "mutations");

    bool restore_broken_parts_as_detached = restorer.getRestoreSettings().restore_broken_parts_as_detached;

    auto restored_parts_holder = std::make_shared<RestoredPartsHolder>(std::static_pointer_cast<MergeTreeData>(shared_from_this()), backup);

    fs::path data_path_in_backup_fs = data_path_in_backup;
    size_t num_parts = 0;

    for (const String & part_name : part_names)
    {
        const auto part_info = MergeTreePartInfo::tryParsePartName(part_name, format_version);
        if (!part_info)
        {
            throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "File name {} is not a part's name",
                            String{data_path_in_backup_fs / part_name});
        }

        if (partition_ids && !partition_ids->contains(part_info->partition_id))
            continue;

        restorer.addDataRestoreTask(
            [storage = std::static_pointer_cast<MergeTreeData>(shared_from_this()),
             backup,
             part_path_in_backup = data_path_in_backup_fs / part_name,
             my_part_info = *part_info,
             restore_broken_parts_as_detached,
             restored_parts_holder]
            { storage->restorePartFromBackup(restored_parts_holder, my_part_info, part_path_in_backup, restore_broken_parts_as_detached); });

        ++num_parts;
    }

    restored_parts_holder->setNumParts(num_parts);
}

void MergeTreeData::restorePartFromBackup(std::shared_ptr<RestoredPartsHolder> restored_parts_holder, const MergeTreePartInfo & part_info, const String & part_path_in_backup, bool detach_if_broken) const
{
    String part_name = part_info.getPartNameAndCheckFormat(format_version);
    auto backup = restored_parts_holder->getBackup();

    /// Find all files of this part in the backup.
    Strings filenames = backup->listFiles(part_path_in_backup, /* recursive= */ true);

    /// Calculate the total size of the part.
    UInt64 total_size_of_part = 0;
    fs::path part_path_in_backup_fs = part_path_in_backup;
    for (const String & filename : filenames)
        total_size_of_part += backup->getFileSize(part_path_in_backup_fs / filename);

    std::shared_ptr<IReservation> reservation = getStoragePolicy()->reserveAndCheck(total_size_of_part);

    /// Calculate paths, for example:
    /// part_name = 0_1_1_0
    /// part_path_in_backup = /data/test/table/0_1_1_0
    /// temp_part_dir = /var/lib/clickhouse/data/test/table/tmp_restore_all_0_1_1_0-XXXXXXXX
    auto disk = reservation->getDisk();
    fs::path temp_part_dir = restored_parts_holder->getTemporaryDirectory(disk, part_name);

    /// Subdirectories in the part's directory. It's used to restore projections.
    std::unordered_set<String> subdirs;

    /// Copy files from the backup to the directory `tmp_part_dir`.
    disk->createDirectories(temp_part_dir);

    for (const String & filename : filenames)
    {
        /// Needs to create subdirectories before copying the files. Subdirectories are used to represent projections.
        auto separator_pos = filename.rfind('/');
        if (separator_pos != String::npos)
        {
            String subdir = filename.substr(0, separator_pos);
            if (subdirs.emplace(subdir).second)
                disk->createDirectories(temp_part_dir / subdir);
        }

        /// TODO Transactions: Decide what to do with version metadata (if any). Let's just skip it for now.
        if (filename.ends_with(IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME))
            continue;

        size_t file_size = backup->copyFileToDisk(part_path_in_backup_fs / filename, disk, temp_part_dir / filename, WriteMode::Rewrite);
        reservation->update(reservation->getSize() - file_size);
    }

    if (auto part = loadPartRestoredFromBackup(part_name, disk, temp_part_dir, detach_if_broken))
        restored_parts_holder->addPart(part);
    else
        restored_parts_holder->increaseNumBrokenParts();
}

MergeTreeData::MutableDataPartPtr MergeTreeData::loadPartRestoredFromBackup(const String & part_name, const DiskPtr & disk, const String & temp_part_dir, bool detach_if_broken) const
{
    MutableDataPartPtr part;

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);
    fs::path full_part_dir{temp_part_dir};
    String parent_part_dir = full_part_dir.parent_path();
    String part_dir_name = full_part_dir.filename();

    /// Load this part from the directory `temp_part_dir`.
    auto load_part = [&]
    {
        MergeTreeDataPartBuilder builder(*this, part_name, single_disk_volume, parent_part_dir, part_dir_name, getReadSettings());
        builder.withPartFormatFromDisk();
        part = std::move(builder).build();
        part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
        part->loadColumnsChecksumsIndexes(/* require_columns_checksums= */ false, /* check_consistency= */ true);
    };

    /// Broken parts can appear in a backup sometimes.
    auto mark_broken = [&](const std::exception_ptr error)
    {
        tryLogException(error, log,
                        fmt::format("Part {} will be restored as detached because it's broken. You need to resolve this manually", part_name));
        if (!part)
        {
            /// Make a fake data part only to copy its files to /detached/.
            part = MergeTreeDataPartBuilder{*this, part_name, single_disk_volume, parent_part_dir, part_dir_name, getReadSettings()}
                       .withPartStorageType(MergeTreeDataPartStorageType::Full)
                       .withPartType(MergeTreeDataPartType::Wide)
                       .build();
        }
        part->renameToDetached("broken-from-backup");
    };

    /// Try to load this part multiple times.
    auto backoff_ms = loading_parts_initial_backoff_ms;
    for (size_t try_no = 0; try_no < loading_parts_max_tries; ++try_no)
    {
        std::exception_ptr error;
        bool retryable = false;
        try
        {
            load_part();
        }
        catch (const Poco::Net::NetException &)
        {
            error = std::current_exception();
            retryable = true;
        }
        catch (const Poco::TimeoutException &)
        {
            error = std::current_exception();
            retryable = true;
        }
        catch (...)
        {
            error = std::current_exception();
            retryable = isRetryableException(std::current_exception());
        }

        if (!error)
            return part;

        if (!retryable && detach_if_broken)
        {
            mark_broken(error);
            return nullptr;
        }

        if (!retryable)
        {
            LOG_ERROR(log,
                      "Failed to restore part {} because it's broken. You can skip broken parts while restoring by setting "
                      "'restore_broken_parts_as_detached = true'",
                      part_name);
        }

        if (!retryable || (try_no + 1 == loading_parts_max_tries))
        {
            if (Exception * e = exception_cast<Exception *>(error))
                e->addMessage("while restoring part {} of table {}", part->name, getStorageID());
            std::rethrow_exception(error);
        }

        tryLogException(error, log,
                        fmt::format("Failed to load part {} at try {} with a retryable error. Will retry in {} ms", part_name, try_no, backoff_ms));

        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        backoff_ms = std::min(backoff_ms * 2, loading_parts_max_backoff_ms);
    }

    UNREACHABLE();
}


String MergeTreeData::getPartitionIDFromQuery(const ASTPtr & ast, ContextPtr local_context, DataPartsLock * acquired_lock) const
{
    const auto & partition_ast = ast->as<ASTPartition &>();

    if (partition_ast.all)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Only Support DROP/DETACH/ATTACH PARTITION ALL currently");

    if (!partition_ast.value)
    {
        MergeTreePartInfo::validatePartitionID(partition_ast.id->clone(), format_version);
        return partition_ast.id->as<ASTLiteral>()->value.safeGet<String>();
    }
    size_t partition_ast_fields_count = 0;
    ASTPtr partition_value_ast = partition_ast.value->clone();
    if (!partition_ast.fields_count.has_value())
    {
        if (partition_value_ast->as<ASTLiteral>())
        {
            partition_ast_fields_count = 1;
        }
        else if (const auto * tuple_ast = partition_value_ast->as<ASTFunction>())
        {
            if (tuple_ast->name != "tuple")
            {
                if (isFunctionCast(tuple_ast))
                {
                    if (tuple_ast->arguments->as<ASTExpressionList>()->children.empty())
                    {
                        throw Exception(
                            ErrorCodes::INVALID_PARTITION_VALUE, "Expected tuple for complex partition key, got {}", tuple_ast->name);
                    }
                    auto first_arg = tuple_ast->arguments->as<ASTExpressionList>()->children.at(0);
                    if (const auto * inner_tuple = first_arg->as<ASTFunction>(); inner_tuple && inner_tuple->name == "tuple")
                    {
                        const auto * arguments_ast = tuple_ast->arguments->as<ASTExpressionList>();
                        if (arguments_ast)
                            partition_ast_fields_count = arguments_ast->children.size();
                        else
                            partition_ast_fields_count = 0;
                    }
                    else if (const auto * inner_literal_tuple = first_arg->as<ASTLiteral>(); inner_literal_tuple)
                    {
                        if (inner_literal_tuple->value.getType() == Field::Types::Tuple)
                            partition_ast_fields_count = inner_literal_tuple->value.safeGet<Tuple>().size();
                        else
                            partition_ast_fields_count = 1;
                    }
                    else
                    {
                        throw Exception(
                            ErrorCodes::INVALID_PARTITION_VALUE, "Expected tuple for complex partition key, got {}", tuple_ast->name);
                    }
                }
                else
                    throw Exception(ErrorCodes::INVALID_PARTITION_VALUE, "Expected tuple for complex partition key, got {}", tuple_ast->name);
            }
            else
            {
                const auto * arguments_ast = tuple_ast->arguments->as<ASTExpressionList>();
                if (arguments_ast)
                    partition_ast_fields_count = arguments_ast->children.size();
                else
                    partition_ast_fields_count = 0;
            }
        }
        else
        {
            throw Exception(
                ErrorCodes::INVALID_PARTITION_VALUE, "Expected literal or tuple for partition key, got {}", partition_value_ast->getID());
        }
    }
    else
    {
        partition_ast_fields_count = *partition_ast.fields_count;
    }

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// Month-partitioning specific - partition ID can be passed in the partition value.
        const auto * partition_lit = partition_ast.value->as<ASTLiteral>();
        if (partition_lit && partition_lit->value.getType() == Field::Types::String)
        {
            MergeTreePartInfo::validatePartitionID(partition_ast.value->clone(), format_version);
            return partition_lit->value.safeGet<String>();
        }
    }

    /// Re-parse partition key fields using the information about expected field types.
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const Block & key_sample_block = metadata_snapshot->getPartitionKey().sample_block;
    size_t fields_count = key_sample_block.columns();
    if (partition_ast_fields_count != fields_count)
        throw Exception(ErrorCodes::INVALID_PARTITION_VALUE,
                        "Wrong number of fields in the partition expression: {}, must be: {}",
                        partition_ast_fields_count, fields_count);

    Row partition_row(fields_count);
    if (fields_count == 0)
    {
        /// Function tuple(...) requires at least one argument, so empty key is a special case
        assert(!partition_ast_fields_count);
        assert(typeid_cast<ASTFunction *>(partition_value_ast.get()));
        assert(partition_value_ast->as<ASTFunction>()->name == "tuple");
        assert(partition_value_ast->as<ASTFunction>()->arguments);
        auto args = partition_value_ast->as<ASTFunction>()->arguments;
        if (!args)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected at least one argument in partition AST");
        bool empty_tuple = partition_value_ast->as<ASTFunction>()->arguments->children.empty();
        if (!empty_tuple)
            throw Exception(ErrorCodes::INVALID_PARTITION_VALUE, "Partition key is empty, expected 'tuple()' as partition key");
    }
    else if (fields_count == 1)
    {
        if (auto * tuple = partition_value_ast->as<ASTFunction>(); tuple)
        {
            if (tuple->name == "tuple")
            {
                assert(tuple->arguments);
                assert(tuple->arguments->children.size() == 1);
                partition_value_ast = tuple->arguments->children[0];
            }
            else if (isFunctionCast(tuple))
            {
                assert(tuple->arguments);
                assert(tuple->arguments->children.size() == 2);
            }
            else
            {
                throw Exception(
                    ErrorCodes::INVALID_PARTITION_VALUE,
                    "Expected literal or tuple for partition key, got {}",
                    partition_value_ast->getID());
            }
        }
        /// Simple partition key, need to evaluate and cast
        Field partition_key_value = evaluateConstantExpression(partition_value_ast, local_context).first;
        partition_row[0] = convertFieldToTypeOrThrow(partition_key_value, *key_sample_block.getByPosition(0).type);
    }
    else
    {
        /// Complex key, need to evaluate, untuple and cast
        Field partition_key_value = evaluateConstantExpression(partition_value_ast, local_context).first;
        if (partition_key_value.getType() != Field::Types::Tuple)
            throw Exception(ErrorCodes::INVALID_PARTITION_VALUE,
                            "Expected tuple for complex partition key, got {}", partition_key_value.getTypeName());

        const Tuple & tuple = partition_key_value.safeGet<Tuple>();
        if (tuple.size() != fields_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Wrong number of fields in the partition expression: {}, must be: {}", tuple.size(), fields_count);

        for (size_t i = 0; i < fields_count; ++i)
            partition_row[i] = convertFieldToTypeOrThrow(tuple[i], *key_sample_block.getByPosition(i).type);
    }

    MergeTreePartition partition(std::move(partition_row));
    String partition_id = partition.getID(*this);

    {
        auto data_parts_lock = (acquired_lock) ? DataPartsLock() : lockParts();
        DataPartPtr existing_part_in_partition = getAnyPartInPartition(partition_id, data_parts_lock);
        if (existing_part_in_partition && existing_part_in_partition->partition.value != partition.value)
        {
            WriteBufferFromOwnString buf;
            partition.serializeText(*this, buf, FormatSettings{});
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Parsed partition value: {} "
                            "doesn't match partition value for an existing part with the same partition ID: {}",
                            buf.str(), existing_part_in_partition->name);
        }
    }

    return partition_id;
}


DataPartsVector MergeTreeData::getVisibleDataPartsVector(ContextPtr local_context) const
{
    return getVisibleDataPartsVector(local_context->getCurrentTransaction());
}

DataPartsVector MergeTreeData::getVisibleDataPartsVectorUnlocked(ContextPtr local_context, const DataPartsLock & lock) const
{
    DataPartsVector res;
    if (const auto * txn = local_context->getCurrentTransaction().get())
    {
        res = getDataPartsVectorForInternalUsage({DataPartState::Active, DataPartState::Outdated}, lock);
        filterVisibleDataParts(res, txn->getSnapshot(), txn->tid);
    }
    else
    {
        res = getDataPartsVectorForInternalUsage({DataPartState::Active}, lock);
    }
    return res;
}

MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVector(const MergeTreeTransactionPtr & txn) const
{
    DataPartsVector res;
    if (txn)
    {
        res = getDataPartsVectorForInternalUsage({DataPartState::Active, DataPartState::Outdated});
        filterVisibleDataParts(res, txn->getSnapshot(), txn->tid);
    }
    else
    {
        res = getDataPartsVectorForInternalUsage();
    }
    return res;
}

MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVector(CSN snapshot_version, TransactionID current_tid) const
{
    auto res = getDataPartsVectorForInternalUsage({DataPartState::Active, DataPartState::Outdated});
    filterVisibleDataParts(res, snapshot_version, current_tid);
    return res;
}

void MergeTreeData::filterVisibleDataParts(DataPartsVector & maybe_visible_parts, CSN snapshot_version, TransactionID current_tid) const
{
    [[maybe_unused]] size_t total_size = maybe_visible_parts.size();

    auto need_remove_pred = [snapshot_version, &current_tid] (const DataPartPtr & part) -> bool
    {
        return !part->version.isVisible(snapshot_version, current_tid);
    };

    std::erase_if(maybe_visible_parts, need_remove_pred);
    [[maybe_unused]] size_t visible_size = maybe_visible_parts.size();

    LOG_TEST(log, "Got {} parts (of {}) visible in snapshot {} (TID {}): {}",
             visible_size, total_size, snapshot_version, current_tid, fmt::join(getPartsNames(maybe_visible_parts), ", "));
}


std::unordered_set<String> MergeTreeData::getPartitionIDsFromQuery(const ASTs & asts, ContextPtr local_context) const
{
    std::unordered_set<String> partition_ids;
    for (const auto & ast : asts)
        partition_ids.emplace(getPartitionIDFromQuery(ast, local_context));
    return partition_ids;
}

std::set<String> MergeTreeData::getPartitionIdsAffectedByCommands(
    const MutationCommands & commands, ContextPtr query_context) const
{
    std::set<String> affected_partition_ids;

    for (const auto & command : commands)
    {
        if (!command.partition)
        {
            affected_partition_ids.clear();
            break;
        }

        affected_partition_ids.insert(
            getPartitionIDFromQuery(command.partition, query_context)
        );
    }

    return affected_partition_ids;
}

std::unordered_set<String> MergeTreeData::getAllPartitionIds() const
{
    auto lock = lockParts();
    std::unordered_set<String> res;
    std::string_view prev_id;
    for (const auto & part : getDataPartsStateRange(DataPartState::Active))
    {
        if (prev_id == part->info.partition_id)
            continue;

        res.insert(part->info.partition_id);
        prev_id = part->info.partition_id;
    }
    return res;
}


MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorForInternalUsage(
    const DataPartStates & affordable_states, const DataPartsLock & /*lock*/, DataPartStateVector * out_states) const
{
    DataPartsVector res;
    DataPartsVector buf;

    for (auto state : affordable_states)
    {
        auto range = getDataPartsStateRange(state);
        std::swap(buf, res);
        res.clear();
        std::merge(range.begin(), range.end(), buf.begin(), buf.end(), std::back_inserter(res), LessDataPart());
    }

    if (out_states != nullptr)
    {
        out_states->resize(res.size());
        for (size_t i = 0; i < res.size(); ++i)
            (*out_states)[i] = res[i]->getState();
    }

    return res;
}

MergeTreeData::DataPartsVector
MergeTreeData::getDataPartsVectorForInternalUsage(const DataPartStates & affordable_states, DataPartStateVector * out_states) const
{
    auto lock = lockParts();
    return getDataPartsVectorForInternalUsage(affordable_states, lock, out_states);
}

MergeTreeData::ProjectionPartsVector
MergeTreeData::getProjectionPartsVectorForInternalUsage(const DataPartStates & affordable_states, DataPartStateVector * out_states) const
{
    auto lock = lockParts();
    ProjectionPartsVector res;
    for (auto state : affordable_states)
    {
        auto range = getDataPartsStateRange(state);
        for (const auto & part : range)
        {
            res.data_parts.push_back(part);
            for (const auto & [_, projection_part] : part->getProjectionParts())
                res.projection_parts.push_back(projection_part);
        }
    }

    if (out_states != nullptr)
    {
        out_states->resize(res.projection_parts.size());
        for (size_t i = 0; i < res.projection_parts.size(); ++i)
            (*out_states)[i] = res.projection_parts[i]->getParentPart()->getState();
    }

    return res;
}

MergeTreeData::DataPartsVector MergeTreeData::getAllDataPartsVector(MergeTreeData::DataPartStateVector * out_states) const
{
    DataPartsVector res;
    auto lock = lockParts();
    res.assign(data_parts_by_info.begin(), data_parts_by_info.end());
    if (out_states != nullptr)
    {
        out_states->resize(res.size());
        for (size_t i = 0; i < res.size(); ++i)
            (*out_states)[i] = res[i]->getState();
    }

    return res;
}

size_t MergeTreeData::getAllPartsCount() const
{
    auto lock = lockParts();
    return data_parts_by_info.size();
}

size_t MergeTreeData::getTotalMarksCount() const
{
    size_t total_marks = 0;
    auto lock = lockParts();
    for (const auto & part : data_parts_by_info)
    {
        total_marks += part->getMarksCount();
    }
    return total_marks;
}

bool MergeTreeData::supportsLightweightDelete() const
{
    auto lock = lockParts();
    for (const auto & part : data_parts_by_info)
    {
        if (part->getState() == MergeTreeDataPartState::Outdated
            || part->getState() == MergeTreeDataPartState::Deleting)
            continue;

        if (!part->supportLightweightDeleteMutate())
            return false;
    }
    return true;
}

bool MergeTreeData::hasProjection() const
{
    auto lock = lockParts();
    for (const auto & part : data_parts_by_info)
    {
        if (part->getState() == MergeTreeDataPartState::Outdated
            || part->getState() == MergeTreeDataPartState::Deleting)
            continue;

        if (part->hasProjection())
            return true;
    }
    return false;
}

bool MergeTreeData::areAsynchronousInsertsEnabled() const
{
    return (*getSettings())[MergeTreeSetting::async_insert];
}

MergeTreeData::ProjectionPartsVector MergeTreeData::getAllProjectionPartsVector(MergeTreeData::DataPartStateVector * out_states) const
{
    ProjectionPartsVector res;
    auto lock = lockParts();
    for (const auto & part : data_parts_by_info)
    {
        res.data_parts.push_back(part);
        for (const auto & [p_name, projection_part] : part->getProjectionParts())
            res.projection_parts.push_back(projection_part);
    }

    if (out_states != nullptr)
    {
        out_states->resize(res.projection_parts.size());
        for (size_t i = 0; i < res.projection_parts.size(); ++i)
            (*out_states)[i] = res.projection_parts[i]->getParentPart()->getState();
    }
    return res;
}

DetachedPartsInfo MergeTreeData::getDetachedParts() const
{
    DetachedPartsInfo res;

    for (const auto & disk : getDisks())
    {
        /// While it is possible to have detached parts on readonly/write-once disks
        /// (if they were produced on another machine, where it wasn't readonly)
        /// to avoid wasting resources for slow disks, avoid trying to enumerate them.
        if (disk->isReadOnly() || disk->isWriteOnce())
            continue;

        String detached_path = fs::path(relative_data_path) / DETACHED_DIR_NAME;

        /// Note: we don't care about TOCTOU issue here.
        if (disk->existsDirectory(detached_path))
        {
            for (auto it = disk->iterateDirectory(detached_path); it->isValid(); it->next())
            {
                res.push_back(DetachedPartInfo::parseDetachedPartName(disk, it->name(), format_version));
            }
        }
    }
    return res;
}

void MergeTreeData::validateDetachedPartName(const String & name)
{
    if (name.find('/') != std::string::npos || name == "." || name == "..")
        throw DB::Exception(ErrorCodes::INCORRECT_FILE_NAME, "Invalid part name '{}'", name);

    if (startsWith(name, "attaching_") || startsWith(name, "deleting_"))
        throw DB::Exception(ErrorCodes::BAD_DATA_PART_NAME, "Cannot drop part {}: "
                            "most likely it is used by another DROP or ATTACH query.", name);
}

void MergeTreeData::dropDetached(const ASTPtr & partition, bool part, ContextPtr local_context)
{
    PartsTemporaryRename renamed_parts(*this, DETACHED_DIR_NAME);

    if (part)
    {
        String part_name = partition->as<ASTLiteral &>().value.safeGet<String>();
        validateDetachedPartName(part_name);
        auto disk = getDiskForDetachedPart(part_name);
        renamed_parts.addPart(part_name, "deleting_" + part_name, disk);
    }
    else
    {
        String partition_id;
        bool all = partition->as<ASTPartition>()->all;
        if (!all)
            partition_id = getPartitionIDFromQuery(partition, local_context);
        DetachedPartsInfo detached_parts = getDetachedParts();
        for (const auto & part_info : detached_parts)
            if (part_info.valid_name && (all || part_info.partition_id == partition_id)
                && part_info.prefix != "attaching" && part_info.prefix != "deleting")
                renamed_parts.addPart(part_info.dir_name, "deleting_" + part_info.dir_name, part_info.disk);
    }

    LOG_DEBUG(log, "Will drop {} detached parts.", renamed_parts.old_and_new_names.size());

    renamed_parts.tryRenameAll();

    for (auto & [old_name, new_name, disk] : renamed_parts.old_and_new_names)
    {
        bool keep_shared = removeDetachedPart(disk, fs::path(relative_data_path) / DETACHED_DIR_NAME / new_name / "", old_name);
        LOG_DEBUG(log, "Dropped detached part {}, keep shared data: {}", old_name, keep_shared);
        old_name.clear();
    }
}

MergeTreeData::MutableDataPartsVector MergeTreeData::tryLoadPartsToAttach(const ASTPtr & partition, bool attach_part,
        ContextPtr local_context, PartsTemporaryRename & renamed_parts)
{
    const fs::path source_dir = DETACHED_DIR_NAME;

    /// Let's compose a list of parts that should be added.
    if (attach_part)
    {
        const String part_id = partition->as<ASTLiteral &>().value.safeGet<String>();
        validateDetachedPartName(part_id);
        if (temporary_parts.contains(source_dir / part_id))
        {
            LOG_WARNING(log, "Will not try to attach part {} because its directory is temporary, "
                             "probably it's being detached right now", part_id);
        }
        else
        {
            auto disk = getDiskForDetachedPart(part_id);
            renamed_parts.addPart(part_id, "attaching_" + part_id, disk);
        }
    }
    else
    {
        String partition_id;
        if (partition->as<ASTPartition>()->all)
        {
            LOG_DEBUG(log, "Looking for parts for all partitions in {}", source_dir);
        }
        else
        {
            partition_id = getPartitionIDFromQuery(partition, local_context);
            LOG_DEBUG(log, "Looking for parts for partition {} in {}", partition_id, source_dir);
        }

        ActiveDataPartSet active_parts(format_version);

        auto detached_parts = getDetachedParts();
        std::erase_if(detached_parts, [&partition_id](const DetachedPartInfo & part_info)
        {
            return !part_info.valid_name || !part_info.prefix.empty() || (!partition_id.empty() && part_info.partition_id != partition_id);
        });

        for (const auto & part_info : detached_parts)
        {
            if (temporary_parts.contains(String(DETACHED_DIR_NAME) + "/" + part_info.dir_name))
            {
                LOG_WARNING(log, "Will not try to attach part {} because its directory is temporary, "
                                 "probably it's being detached right now", part_info.dir_name);
                continue;
            }
            LOG_DEBUG(log, "Found part {}", part_info.dir_name);
            active_parts.add(part_info.dir_name);
        }

        LOG_DEBUG(log, "{} of them are active", active_parts.size());

        /// Inactive parts are renamed so they can not be attached in case of repeated ATTACH.
        for (const auto & part_info : detached_parts)
        {
            const String containing_part = active_parts.getContainingPart(part_info.dir_name);
            if (containing_part.empty())
                continue;

            LOG_DEBUG(log, "Found containing part {} for part {}", containing_part, part_info.dir_name);

            if (containing_part != part_info.dir_name)
                part_info.disk->moveDirectory(fs::path(relative_data_path) / source_dir / part_info.dir_name,
                    fs::path(relative_data_path) / source_dir / ("inactive_" + part_info.dir_name));
            else
                renamed_parts.addPart(part_info.dir_name, "attaching_" + part_info.dir_name, part_info.disk);
        }
    }


    /// Try to rename all parts before attaching to prevent race with DROP DETACHED and another ATTACH.
    renamed_parts.tryRenameAll();

    /// Synchronously check that added parts exist and are not broken. We will write checksums.txt if it does not exist.
    LOG_DEBUG(log, "Checking {} parts", renamed_parts.old_and_new_names.size());
    MutableDataPartsVector loaded_parts;
    loaded_parts.reserve(renamed_parts.old_and_new_names.size());

    for (const auto & [old_name, new_name, disk] : renamed_parts.old_and_new_names)
    {
        LOG_DEBUG(log, "Checking part {}", new_name);

        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + old_name, disk);
        auto part = getDataPartBuilder(old_name, single_disk_volume, source_dir / new_name, getReadSettings())
            .withPartFormatFromDisk()
            .build();

        loadPartAndFixMetadataImpl(part, local_context, getInMemoryMetadataPtr()->getMetadataVersion(), (*getSettings())[MergeTreeSetting::fsync_after_insert]);
        loaded_parts.push_back(part);
    }

    return loaded_parts;
}

namespace
{

inline ReservationPtr checkAndReturnReservation(UInt64 expected_size, ReservationPtr reservation)
{
    if (reservation)
        return reservation;

    throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot reserve {}, not enough space", ReadableSize(expected_size));
}

}

ReservationPtr MergeTreeData::reserveSpace(UInt64 expected_size) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    return getStoragePolicy()->reserveAndCheck(expected_size);
}

ReservationPtr MergeTreeData::reserveSpace(UInt64 expected_size, SpacePtr space)
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    auto reservation = tryReserveSpace(expected_size, space);
    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeData::reserveSpace(UInt64 expected_size, const IDataPartStorage & data_part_storage)
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    return data_part_storage.reserve(expected_size);
}

ReservationPtr MergeTreeData::tryReserveSpace(UInt64 expected_size, const IDataPartStorage & data_part_storage)
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    return data_part_storage.tryReserve(expected_size);
}

ReservationPtr MergeTreeData::tryReserveSpace(UInt64 expected_size, SpacePtr space)
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    return space->reserve(expected_size);
}

ReservationPtr MergeTreeData::reserveSpacePreferringTTLRules(
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 expected_size,
    const IMergeTreeDataPart::TTLInfos & ttl_infos,
    time_t time_of_move,
    size_t min_volume_index,
    bool is_insert,
    DiskPtr selected_disk) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    ReservationPtr reservation = tryReserveSpacePreferringTTLRules(
        metadata_snapshot, expected_size, ttl_infos, time_of_move, min_volume_index, is_insert, selected_disk);

    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeData::tryReserveSpacePreferringTTLRules(
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 expected_size,
    const IMergeTreeDataPart::TTLInfos & ttl_infos,
    time_t time_of_move,
    size_t min_volume_index,
    bool is_insert,
    DiskPtr selected_disk) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    ReservationPtr reservation;

    auto move_ttl_entry = selectTTLDescriptionForTTLInfos(metadata_snapshot->getMoveTTLs(), ttl_infos.moves_ttl, time_of_move, true);

    if (move_ttl_entry)
    {
        LOG_TRACE(log, "Trying to reserve {} to apply a TTL rule. Will try to reserve in the destination", ReadableSize(expected_size));
        SpacePtr destination_ptr = getDestinationForMoveTTL(*move_ttl_entry);
        bool perform_ttl_move_on_insert = is_insert && destination_ptr && shouldPerformTTLMoveOnInsert(destination_ptr);

        if (!destination_ptr)
        {
            if (move_ttl_entry->destination_type == DataDestinationType::VOLUME && !move_ttl_entry->if_exists)
                LOG_WARNING(
                    log,
                    "Would like to reserve space on volume '{}' by TTL rule of table '{}' but volume was not found",
                    move_ttl_entry->destination_name,
                    log.loadName());
            else if (move_ttl_entry->destination_type == DataDestinationType::DISK && !move_ttl_entry->if_exists)
                LOG_WARNING(
                    log,
                    "Would like to reserve space on disk '{}' by TTL rule of table '{}' but disk was not found",
                    move_ttl_entry->destination_name,
                    log.loadName());
        }
        else if (is_insert && !perform_ttl_move_on_insert)
        {
            LOG_TRACE(
                log,
                "TTL move on insert to {} {} for table {} is disabled",
                (move_ttl_entry->destination_type == DataDestinationType::VOLUME ? "volume" : "disk"),
                move_ttl_entry->destination_name,
                log.loadName());
        }
        else
        {
            reservation = destination_ptr->reserve(expected_size);
            if (reservation)
            {
                return reservation;
            }

            if (move_ttl_entry->destination_type == DataDestinationType::VOLUME)
                LOG_WARNING(
                    log,
                    "Would like to reserve space on volume '{}' by TTL rule of table '{}' but there is not enough space",
                    move_ttl_entry->destination_name,
                    log.loadName());
            else if (move_ttl_entry->destination_type == DataDestinationType::DISK)
                LOG_WARNING(
                    log,
                    "Would like to reserve space on disk '{}' by TTL rule of table '{}' but there is not enough space",
                    move_ttl_entry->destination_name,
                    log.loadName());

        }
    }

    // Prefer selected_disk
    if (selected_disk)
    {
        LOG_TRACE(
            log,
            "Trying to reserve {} on the selected disk: {} (with type {})",
            ReadableSize(expected_size),
            selected_disk->getName(),
            selected_disk->getDataSourceDescription().toString());
        reservation = selected_disk->reserve(expected_size);
    }

    if (!reservation)
    {
        LOG_TRACE(log, "Trying to reserve {} using storage policy from min volume index {}", ReadableSize(expected_size), min_volume_index);
        reservation = getStoragePolicy()->reserve(expected_size, min_volume_index);
    }

    return reservation;
}

SpacePtr MergeTreeData::getDestinationForMoveTTL(const TTLDescription & move_ttl) const
{
    auto policy = getStoragePolicy();

    if (move_ttl.destination_type == DataDestinationType::VOLUME)
        return policy->tryGetVolumeByName(move_ttl.destination_name);

    if (move_ttl.destination_type == DataDestinationType::DISK)
        return policy->tryGetDiskByName(move_ttl.destination_name);

    return {};
}

bool MergeTreeData::shouldPerformTTLMoveOnInsert(const SpacePtr & move_destination) const
{
    if (move_destination->isVolume())
    {
        auto volume = std::static_pointer_cast<IVolume>(move_destination);
        return volume->perform_ttl_move_on_insert;
    }
    if (move_destination->isDisk())
    {
        auto disk = std::static_pointer_cast<IDisk>(move_destination);
        if (auto volume = getStoragePolicy()->tryGetVolumeByDiskName(disk->getName()))
            return volume->perform_ttl_move_on_insert;
    }
    return false;
}

bool MergeTreeData::isPartInTTLDestination(const TTLDescription & ttl, const IMergeTreeDataPart & part) const
{
    auto policy = getStoragePolicy();
    if (ttl.destination_type == DataDestinationType::VOLUME)
    {
        for (const auto & disk : policy->getVolumeByName(ttl.destination_name)->getDisks())
            if (disk->getName() == part.getDataPartStorage().getDiskName())
                return true;
    }
    else if (ttl.destination_type == DataDestinationType::DISK)
        return policy->getDiskByName(ttl.destination_name)->getName() == part.getDataPartStorage().getDiskName();
    return false;
}

CompressionCodecPtr MergeTreeData::getCompressionCodecForPart(size_t part_size_compressed, const IMergeTreeDataPart::TTLInfos & ttl_infos, time_t current_time) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();

    const auto & recompression_ttl_entries = metadata_snapshot->getRecompressionTTLs();
    auto best_ttl_entry = selectTTLDescriptionForTTLInfos(recompression_ttl_entries, ttl_infos.recompression_ttl, current_time, true);

    if (best_ttl_entry)
        return CompressionCodecFactory::instance().get(best_ttl_entry->recompression_codec, {});

    return getContext()->chooseCompressionCodec(
        part_size_compressed,
        static_cast<double>(part_size_compressed) / getTotalActiveSizeInBytes());
}


MergeTreeData::DataParts MergeTreeData::getDataParts(const DataPartStates & affordable_states) const
{
    DataParts res;
    {
        auto lock = lockParts();
        for (auto state : affordable_states)
        {
            auto range = getDataPartsStateRange(state);
            res.insert(range.begin(), range.end());
        }
    }
    return res;
}

MergeTreeData::DataParts MergeTreeData::getDataPartsForInternalUsage() const
{
    return getDataParts({DataPartState::Active});
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorForInternalUsage() const
{
    return getDataPartsVectorForInternalUsage({DataPartState::Active});
}

MergeTreeData::DataPartPtr MergeTreeData::getAnyPartInPartition(
    const String & partition_id, DataPartsLock & /*data_parts_lock*/) const
{
    auto it = data_parts_by_state_and_info.lower_bound(DataPartStateAndPartitionID{DataPartState::Active, partition_id});

    if (it != data_parts_by_state_and_info.end() && (*it)->getState() == DataPartState::Active && (*it)->info.partition_id == partition_id)
        return *it;

    return nullptr;
}


MergeTreeData::Transaction::Transaction(MergeTreeData & data_, MergeTreeTransaction * txn_)
    : data(data_)
    , txn(txn_)
{
    if (txn)
        data.transactions_enabled.store(true);
}

void MergeTreeData::Transaction::rollbackPartsToTemporaryState()
{
    if (!isEmpty())
    {
        WriteBufferFromOwnString buf;
        buf << " Rollbacking parts state to temporary and removing from working set:";
        for (const auto & part : precommitted_parts)
            buf << " " << part->getDataPartStorage().getPartDirectory();
        buf << ".";
        LOG_DEBUG(data.log, "Undoing transaction.{}", buf.str());

        data.removePartsFromWorkingSetImmediatelyAndSetTemporaryState(
            DataPartsVector(precommitted_parts.begin(), precommitted_parts.end()));
    }

    clear();
}

TransactionID MergeTreeData::Transaction::getTID() const
{
    if (txn)
        return txn->tid;
    return Tx::PrehistoricTID;
}

void MergeTreeData::Transaction::addPart(MutableDataPartPtr & part, bool need_rename)
{
    precommitted_parts.insert(part);
    if (need_rename)
        precommitted_parts_need_rename.insert(part);
}

void MergeTreeData::Transaction::rollback(DataPartsLock * lock)
{
    if (!isEmpty())
    {
        for (const auto & part : precommitted_parts)
            part->version.creation_csn.store(Tx::RolledBackCSN);

        auto non_detached_precommitted_parts = precommitted_parts;

        /// Remove detached parts from working set.
        ///
        /// It is possible to have detached parts here, only when rename (in
        /// commit()) of detached parts had been broken (i.e. during ATTACH),
        /// i.e. the part itself is broken.
        DataPartsVector detached_precommitted_parts;
        for (auto it = non_detached_precommitted_parts.begin(); it != non_detached_precommitted_parts.end();)
        {
            const auto & part = *it;
            if (part->getDataPartStorage().getParentDirectory() == DETACHED_DIR_NAME)
            {
                detached_precommitted_parts.push_back(part);
                it = non_detached_precommitted_parts.erase(it);
            }
            else
                ++it;
        }

        WriteBufferFromOwnString buf;
        buf << "Removing parts:";
        for (const auto & part : non_detached_precommitted_parts)
            buf << " " << part->getDataPartStorage().getPartDirectory();
        buf << ".";
        if (!detached_precommitted_parts.empty())
        {
            buf << " Rollbacking parts state to temporary and removing from working set:";
            for (const auto & part : detached_precommitted_parts)
                buf << " " << part->getDataPartStorage().getPartDirectory();
            buf << ".";
        }
        LOG_DEBUG(data.log, "Undoing transaction {}. {}", getTID(), buf.str());

        /// It would be much better with TSA...
        auto our_lock = (lock) ? DataPartsLock() : data.lockParts();

        if (data.data_parts_indexes.empty())
        {
            /// Table was dropped concurrently and all parts (including PreActive parts) were cleared, so there's nothing to rollback
            if (!data.all_data_dropped)
            {
                Strings part_names;
                for (const auto & part : non_detached_precommitted_parts)
                    part_names.emplace_back(part->name);
                throw Exception(ErrorCodes::LOGICAL_ERROR, "There are some PreActive parts ({}) to rollback, "
                                "but data parts set is empty and table {} was not dropped. It's a bug",
                                fmt::join(part_names, ", "), data.getStorageID().getNameForLogs());
            }
        }
        else
        {
            data.removePartsFromWorkingSetImmediatelyAndSetTemporaryState(
                detached_precommitted_parts,
                &our_lock);

            data.removePartsFromWorkingSet(txn,
                DataPartsVector(non_detached_precommitted_parts.begin(), non_detached_precommitted_parts.end()),
                /* clear_without_timeout = */ true, &our_lock);
        }
    }

    clear();
}

void MergeTreeData::Transaction::clear()
{
    chassert(precommitted_parts.size() >= precommitted_parts_need_rename.size());
    precommitted_parts.clear();
    precommitted_parts_need_rename.clear();
}

void MergeTreeData::Transaction::renameParts()
{
    for (const auto & part_need_rename : precommitted_parts_need_rename)
    {
        LOG_TEST(data.log, "Renaming part to {}", part_need_rename->name);
        part_need_rename->renameTo(part_need_rename->name, true);
    }
    precommitted_parts_need_rename.clear();
}

MergeTreeData::DataPartsVector MergeTreeData::Transaction::commit(DataPartsLock * acquired_parts_lock)
{
    DataPartsVector total_covered_parts;

    if (!isEmpty())
    {
        if (!precommitted_parts_need_rename.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Parts had not been renamed");

        auto settings = data.getSettings();
        auto parts_lock = acquired_parts_lock ? DataPartsLock() : data.lockParts();
        auto * owing_parts_lock = acquired_parts_lock ? acquired_parts_lock : &parts_lock;

        for (const auto & part : precommitted_parts)
            if (part->getDataPartStorage().hasActiveTransaction())
                part->getDataPartStorage().commitTransaction();

        if (txn)
        {
            for (const auto & part : precommitted_parts)
            {
                DataPartPtr covering_part;
                DataPartsVector covered_active_parts = data.getActivePartsToReplace(part->info, part->name, covering_part, *owing_parts_lock);

                /// outdated parts should be also collected here
                /// the visible outdated parts should be tried to be removed
                /// more likely the conflict happens at the removing visible outdated parts, what is right actually
                DataPartsVector covered_outdated_parts = data.getCoveredOutdatedParts(part, *owing_parts_lock);

                LOG_TEST(data.log, "Got {} oudated parts covered by {} (TID {} CSN {}): {}",
                         covered_outdated_parts.size(), part->getNameWithState(), txn->tid, txn->getSnapshot(), fmt::join(getPartsNames(covered_outdated_parts), ", "));
                data.filterVisibleDataParts(covered_outdated_parts, txn->getSnapshot(), txn->tid);

                DataPartsVector covered_parts;
                covered_parts.reserve(covered_active_parts.size() + covered_outdated_parts.size());
                std::move(covered_active_parts.begin(), covered_active_parts.end(), std::back_inserter(covered_parts));
                std::move(covered_outdated_parts.begin(), covered_outdated_parts.end(), std::back_inserter(covered_parts));

                MergeTreeTransaction::addNewPartAndRemoveCovered(data.shared_from_this(), part, covered_parts, txn);
            }
        }

        NOEXCEPT_SCOPE({
            auto current_time = time(nullptr);

            size_t add_bytes = 0;
            size_t add_rows = 0;
            size_t add_parts = 0;

            size_t reduce_bytes = 0;
            size_t reduce_rows = 0;
            size_t reduce_parts = 0;

            for (const auto & part : precommitted_parts)
            {
                DataPartPtr covering_part;
                DataPartsVector covered_parts = data.getActivePartsToReplace(part->info, part->name, covering_part, *owing_parts_lock);
                if (covering_part)
                {
                    /// It's totally fine for zero-level parts, because of possible race condition between ReplicatedMergeTreeSink and
                    /// background queue execution (new part is added to ZK before this function is called,
                    /// so other replica may produce covering part and replication queue may download covering part).
                    if (part->info.level)
                        LOG_WARNING(data.log, "Tried to commit obsolete part {} covered by {}", part->name, covering_part->getNameWithState());
                    else
                        LOG_INFO(data.log, "Tried to commit obsolete part {} covered by {}", part->name, covering_part->getNameWithState());

                    part->remove_time.store(0, std::memory_order_relaxed); /// The part will be removed without waiting for old_parts_lifetime seconds.
                    data.modifyPartState(part, DataPartState::Outdated);
                }
                else
                {
                    if (!txn)
                        MergeTreeTransaction::addNewPartAndRemoveCovered(data.shared_from_this(), part, covered_parts, NO_TRANSACTION_RAW);

                    total_covered_parts.insert(total_covered_parts.end(), covered_parts.begin(), covered_parts.end());
                    for (const auto & covered_part : covered_parts)
                    {
                        covered_part->remove_time.store(current_time, std::memory_order_relaxed);

                        reduce_bytes += covered_part->getBytesOnDisk();
                        reduce_rows += covered_part->rows_count;

                        data.modifyPartState(covered_part, DataPartState::Outdated);
                        data.removePartContributionToColumnAndSecondaryIndexSizes(covered_part);
                    }

                    reduce_parts += covered_parts.size();

                    add_bytes += part->getBytesOnDisk();
                    add_rows += part->rows_count;
                    ++add_parts;

                    data.modifyPartState(part, DataPartState::Active);
                    data.addPartContributionToColumnAndSecondaryIndexSizes(part);
                }
            }

            data.updateSerializationHints(precommitted_parts, total_covered_parts, parts_lock);

            if (reduce_parts == 0)
            {
                for (const auto & part : precommitted_parts)
                    data.updateObjectColumns(part, parts_lock);
            }
            else
                data.resetObjectColumnsFromActiveParts(parts_lock);

            ssize_t diff_bytes = add_bytes - reduce_bytes;
            ssize_t diff_rows = add_rows - reduce_rows;
            ssize_t diff_parts  = add_parts - reduce_parts;
            data.increaseDataVolume(diff_bytes, diff_rows, diff_parts);
        });
    }

    clear();

    return total_covered_parts;
}

bool MergeTreeData::isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(
    const ASTPtr & node, const StorageMetadataPtr & metadata_snapshot) const
{
    const String column_name = node->getColumnName();

    for (const auto & name : metadata_snapshot->getPrimaryKeyColumns())
        if (column_name == name)
            return true;

    for (const auto & name : getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()))
        if (column_name == name)
            return true;

    if (const auto * func = node->as<ASTFunction>())
        if (func->arguments->children.size() == 1)
            return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(func->arguments->children.front(), metadata_snapshot);

    return false;
}

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

Block MergeTreeData::getMinMaxCountProjectionBlock(
    const StorageMetadataPtr & metadata_snapshot,
    const Names & required_columns,
    const ActionsDAG * filter_dag,
    const DataPartsVector & parts,
    const PartitionIdToMaxBlock * max_block_numbers_to_read,
    ContextPtr query_context) const
{
    if (!metadata_snapshot->minmax_count_projection)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot find the definition of minmax_count projection but it's used in current query. "
                        "It's a bug");

    auto block = metadata_snapshot->minmax_count_projection->sample_block.cloneEmpty();
    bool need_primary_key_max_column = false;
    const auto & primary_key_max_column_name = metadata_snapshot->minmax_count_projection->primary_key_max_column_name;
    NameSet required_columns_set(required_columns.begin(), required_columns.end());

    if (!primary_key_max_column_name.empty())
        need_primary_key_max_column = required_columns_set.contains(primary_key_max_column_name);

    auto partition_minmax_count_columns = block.mutateColumns();
    auto partition_minmax_count_column_names = block.getNames();
    auto insert = [](ColumnAggregateFunction & column, const Field & value)
    {
        auto func = column.getAggregateFunction();
        Arena & arena = column.createOrGetArena();
        size_t size_of_state = func->sizeOfData();
        size_t align_of_state = func->alignOfData();
        auto * place = arena.alignedAlloc(size_of_state, align_of_state);
        func->create(place);
        if (const AggregateFunctionCount * /*agg_count*/ _ = typeid_cast<const AggregateFunctionCount *>(func.get()))
            AggregateFunctionCount::set(place, value.safeGet<UInt64>());
        else
        {
            auto value_column = func->getArgumentTypes().front()->createColumnConst(1, value)->convertToFullColumnIfConst();
            const auto * value_column_ptr = value_column.get();
            func->add(place, &value_column_ptr, 0, &arena);
        }
        column.insertFrom(place);
    };

    Block virtual_columns_block;
    auto virtual_block = getHeaderWithVirtualsForFilter(metadata_snapshot);
    bool has_virtual_column = std::any_of(required_columns.begin(), required_columns.end(), [&](const auto & name) { return virtual_block.has(name); });
    if (has_virtual_column || filter_dag)
    {
        virtual_columns_block = getBlockWithVirtualsForFilter(metadata_snapshot, parts, /*ignore_empty=*/true);
        if (virtual_columns_block.rows() == 0)
            return {};
    }

    size_t rows = parts.size();
    ColumnPtr part_name_column;
    std::optional<PartitionPruner> partition_pruner;
    std::optional<KeyCondition> minmax_idx_condition;
    DataTypes minmax_columns_types;
    if (filter_dag)
    {
        if (metadata_snapshot->hasPartitionKey())
        {
            const auto & partition_key = metadata_snapshot->getPartitionKey();
            auto minmax_columns_names = getMinMaxColumnsNames(partition_key);
            minmax_columns_types = getMinMaxColumnsTypes(partition_key);

            minmax_idx_condition.emplace(
                filter_dag, query_context, minmax_columns_names,
                getMinMaxExpr(partition_key, ExpressionActionsSettings::fromContext(query_context)));
            partition_pruner.emplace(metadata_snapshot, filter_dag, query_context, false /* strict */);
        }

        const auto * predicate = filter_dag->getOutputs().at(0);

        // Generate valid expressions for filtering
        VirtualColumnUtils::filterBlockWithPredicate(
            predicate, virtual_columns_block, query_context, /*allow_filtering_with_partial_predicate =*/true);

        rows = virtual_columns_block.rows();
        part_name_column = virtual_columns_block.getByName("_part").column;
    }

    auto filter_column = ColumnUInt8::create();
    auto & filter_column_data = filter_column->getData();

    DataPartsVector real_parts;
    real_parts.reserve(rows);
    for (size_t row = 0, part_idx = 0; row < rows; ++row, ++part_idx)
    {
        if (part_name_column)
        {
            while (parts[part_idx]->name != part_name_column->getDataAt(row))
                ++part_idx;
        }

        const auto & part = parts[part_idx];

        if (part->isEmpty())
            continue;

        if (!part->minmax_idx->initialized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Found a non-empty part with uninitialized minmax_idx. It's a bug");

        filter_column_data.emplace_back();

        if (max_block_numbers_to_read)
        {
            auto blocks_iterator = max_block_numbers_to_read->find(part->info.partition_id);
            if (blocks_iterator == max_block_numbers_to_read->end() || part->info.max_block > blocks_iterator->second)
                continue;
        }

        if (minmax_idx_condition
            && !minmax_idx_condition->checkInHyperrectangle(part->minmax_idx->hyperrectangle, minmax_columns_types).can_be_true)
            continue;

        if (partition_pruner)
        {
            if (partition_pruner->canBePruned(*part))
                continue;
        }

        /// It's extremely rare that some parts have final marks while others don't. To make it
        /// straightforward, disable minmax_count projection when `max(pk)' encounters any part with
        /// no final mark.
        if (need_primary_key_max_column && !part->index_granularity.hasFinalMark())
            return {};

        real_parts.push_back(part);
        filter_column_data.back() = 1;
    }

    if (real_parts.empty())
        return {};

    FilterDescription filter(*filter_column);
    for (size_t i = 0; i < virtual_columns_block.columns(); ++i)
    {
        ColumnPtr & column = virtual_columns_block.safeGetByPosition(i).column;
        column = column->filter(*filter.data, -1);
    }

    size_t pos = 0;
    for (size_t i : metadata_snapshot->minmax_count_projection->partition_value_indices)
    {
        if (required_columns_set.contains(partition_minmax_count_column_names[pos]))
            for (const auto & part : real_parts)
                partition_minmax_count_columns[pos]->insert(part->partition.value[i]);
        ++pos;
    }

    size_t minmax_idx_size = real_parts.front()->minmax_idx->hyperrectangle.size();
    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        if (required_columns_set.contains(partition_minmax_count_column_names[pos]))
        {
            for (const auto & part : real_parts)
            {
                const auto & range = part->minmax_idx->hyperrectangle[i];
                auto & min_column = assert_cast<ColumnAggregateFunction &>(*partition_minmax_count_columns[pos]);
                insert(min_column, range.left);
            }
        }
        ++pos;

        if (required_columns_set.contains(partition_minmax_count_column_names[pos]))
        {
            for (const auto & part : real_parts)
            {
                const auto & range = part->minmax_idx->hyperrectangle[i];
                auto & max_column = assert_cast<ColumnAggregateFunction &>(*partition_minmax_count_columns[pos]);
                insert(max_column, range.right);
            }
        }
        ++pos;
    }

    if (!primary_key_max_column_name.empty())
    {
        if (required_columns_set.contains(partition_minmax_count_column_names[pos]))
        {
            for (const auto & part : real_parts)
            {
                const auto & primary_key_column = *part->getIndex()->at(0);
                auto & min_column = assert_cast<ColumnAggregateFunction &>(*partition_minmax_count_columns[pos]);
                insert(min_column, primary_key_column[0]);
            }
        }
        ++pos;

        if (required_columns_set.contains(partition_minmax_count_column_names[pos]))
        {
            for (const auto & part : real_parts)
            {
                const auto & primary_key_column = *part->getIndex()->at(0);
                auto & max_column = assert_cast<ColumnAggregateFunction &>(*partition_minmax_count_columns[pos]);
                insert(max_column, primary_key_column[primary_key_column.size() - 1]);
            }
        }
        ++pos;
    }

    bool has_count
        = std::any_of(required_columns.begin(), required_columns.end(), [&](const auto & name) { return startsWith(name, "count"); });
    if (has_count)
    {
        for (const auto & part : real_parts)
        {
            auto & column = assert_cast<ColumnAggregateFunction &>(*partition_minmax_count_columns.back());
            insert(column, part->rows_count);
        }
    }

    block.setColumns(std::move(partition_minmax_count_columns));

    Block res;
    for (const auto & name : required_columns)
    {
        if (virtual_columns_block.has(name))
            res.insert(virtual_columns_block.getByName(name));
        else if (block.has(name))
            res.insert(block.getByName(name));
        else if (startsWith(name, "count")) // special case to match count(...) variants
        {
            const auto & column = block.getByName("count()");
            res.insert({column.column, column.type, name});
        }
        else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot find column {} in minmax_count projection but query analysis still selects this projection. It's a bug",
                name);
    }
    return res;
}

ActionDAGNodes MergeTreeData::getFiltersForPrimaryKeyAnalysis(const InterpreterSelectQuery & select)
{
    const auto & analysis_result = select.getAnalysisResult();
    const auto & before_where = analysis_result.before_where;
    const auto & where_column_name = analysis_result.where_column_name;

    ActionDAGNodes filter_nodes;
    if (auto additional_filter_info = select.getAdditionalQueryInfo())
        filter_nodes.nodes.push_back(&additional_filter_info->actions.findInOutputs(additional_filter_info->column_name));

    if (before_where)
        filter_nodes.nodes.push_back(&before_where->dag.findInOutputs(where_column_name));

    return filter_nodes;
}

QueryProcessingStage::Enum MergeTreeData::getQueryProcessingStage(
    ContextPtr query_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr &,
    SelectQueryInfo &) const
{
    /// with new analyzer, Planner make decision regarding parallel replicas usage, and so about processing stage on reading
    if (!query_context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        const auto & settings = query_context->getSettingsRef();
        if (query_context->canUseParallelReplicasCustomKey())
        {
            if (query_context->getClientInfo().distributed_depth > 0)
                return QueryProcessingStage::FetchColumns;

            if (!supportsReplication() && !settings[Setting::parallel_replicas_for_non_replicated_merge_tree])
                return QueryProcessingStage::Enum::FetchColumns;

            if (to_stage >= QueryProcessingStage::WithMergeableState
                && query_context->canUseParallelReplicasCustomKeyForCluster(*query_context->getClusterForParallelReplicas()))
                return QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
        }

        if (query_context->getClientInfo().collaborate_with_initiator)
            return QueryProcessingStage::Enum::FetchColumns;

        /// Parallel replicas
        if (query_context->canUseParallelReplicasOnInitiator() && to_stage >= QueryProcessingStage::WithMergeableState)
        {
            /// ReplicatedMergeTree
            if (supportsReplication())
                return QueryProcessingStage::Enum::WithMergeableState;

            /// For non-replicated MergeTree we allow them only if parallel_replicas_for_non_replicated_merge_tree is enabled
            if (settings[Setting::parallel_replicas_for_non_replicated_merge_tree])
                return QueryProcessingStage::Enum::WithMergeableState;
        }
    }

    return QueryProcessingStage::Enum::FetchColumns;
}


UInt64 MergeTreeData::estimateNumberOfRowsToRead(
    ContextPtr query_context, const StorageSnapshotPtr & storage_snapshot, const SelectQueryInfo & query_info) const
{
    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);

    MergeTreeDataSelectExecutor reader(*this);
    auto result_ptr = reader.estimateNumMarksToRead(
        snapshot_data.parts,
        snapshot_data.mutations_snapshot,
        storage_snapshot->metadata->getColumns().getAll().getNames(),
        storage_snapshot->metadata,
        query_info,
        query_context,
        query_context->getSettingsRef()[Setting::max_threads]);

    UInt64 total_rows = result_ptr->selected_rows;
    if (query_info.trivial_limit > 0 && query_info.trivial_limit < total_rows)
        total_rows = query_info.trivial_limit;
    return total_rows;
}

void MergeTreeData::checkColumnFilenamesForCollision(const StorageInMemoryMetadata & metadata, bool throw_on_error) const
{
    auto settings = getDefaultSettings();
    if (metadata.settings_changes)
    {
        const auto & changes = metadata.settings_changes->as<const ASTSetQuery &>().changes;
        settings->applyChanges(changes);
    }

    checkColumnFilenamesForCollision(metadata.getColumns(), *settings, throw_on_error);
}

void MergeTreeData::checkColumnFilenamesForCollision(const ColumnsDescription & columns, const MergeTreeSettings & settings, bool throw_on_error) const
{
    std::unordered_map<String, std::pair<String, String>> stream_name_to_full_name;
    auto columns_list = Nested::collect(columns.getAllPhysical());

    for (const auto & column : columns_list)
    {
        std::unordered_map<String, String> column_streams;

        auto callback = [&](const auto & substream_path)
        {
            String stream_name;
            auto full_stream_name = ISerialization::getFileNameForStream(column, substream_path);

            if (settings[MergeTreeSetting::replace_long_file_name_to_hash] && full_stream_name.size() > settings[MergeTreeSetting::max_file_name_length])
                stream_name = sipHash128String(full_stream_name);
            else
                stream_name = full_stream_name;

            column_streams.emplace(stream_name, full_stream_name);
        };

        auto serialization = column.type->getDefaultSerialization();
        serialization->enumerateStreams(callback);

        if (column.type->supportsSparseSerialization() && settings[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization] < 1.0)
        {
            auto sparse_serialization = column.type->getSparseSerialization();
            sparse_serialization->enumerateStreams(callback);
        }

        for (const auto & [stream_name, full_stream_name] : column_streams)
        {
            auto [it, inserted] = stream_name_to_full_name.emplace(stream_name, std::pair{full_stream_name, column.name});
            if (!inserted)
            {
                const auto & [other_full_name, other_column_name] = it->second;
                auto other_type = columns.getPhysical(other_column_name).type;

                auto message = fmt::format(
                    "Columns '{} {}' and '{} {}' have streams ({} and {}) with collision in file name {}",
                    column.name, column.type->getName(), other_column_name, other_type->getName(), full_stream_name, other_full_name, stream_name);

                if (settings[MergeTreeSetting::replace_long_file_name_to_hash])
                    message += ". It may be a collision between a filename for one column and a hash of filename for another column (see setting 'replace_long_file_name_to_hash')";

                if (throw_on_error)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}", message);

                LOG_ERROR(log, "Table definition is incorrect. {}. It may lead to corruption of data or crashes. You need to resolve it manually", message);
                return;
            }
        }
    }
}

MergeTreeData & MergeTreeData::checkStructureAndGetMergeTreeData(IStorage & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const
{
    MergeTreeData * src_data = dynamic_cast<MergeTreeData *>(&source_table);
    if (!src_data)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Table {} supports attachPartitionFrom only for MergeTree family of table engines. Got {}",
                        source_table.getStorageID().getNameForLogs(), source_table.getName());

    if (my_snapshot->getColumns().getAllPhysical().sizeOfDifference(src_snapshot->getColumns().getAllPhysical()))
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Tables have different structure");

    auto query_to_string = [] (const ASTPtr & ast)
    {
        return ast ? queryToString(ast) : "";
    };

    if (query_to_string(my_snapshot->getSortingKeyAST()) != query_to_string(src_snapshot->getSortingKeyAST()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different ordering");

    if (query_to_string(my_snapshot->getPartitionKeyAST()) != query_to_string(src_snapshot->getPartitionKeyAST()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different partition key");

    if (format_version != src_data->format_version)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different format_version");

    if (query_to_string(my_snapshot->getPrimaryKeyAST()) != query_to_string(src_snapshot->getPrimaryKeyAST()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different primary key");

    const auto check_definitions = [](const auto & my_descriptions, const auto & src_descriptions)
    {
        if (my_descriptions.size() != src_descriptions.size())
            return false;

        std::unordered_set<std::string> my_query_strings;
        for (const auto & description : my_descriptions)
            my_query_strings.insert(queryToString(description.definition_ast));

        for (const auto & src_description : src_descriptions)
            if (!my_query_strings.contains(queryToString(src_description.definition_ast)))
                return false;

        return true;
    };

    if (!check_definitions(my_snapshot->getSecondaryIndices(), src_snapshot->getSecondaryIndices()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different secondary indices");

    if (!check_definitions(my_snapshot->getProjections(), src_snapshot->getProjections()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different projections");

    return *src_data;
}

MergeTreeData & MergeTreeData::checkStructureAndGetMergeTreeData(
    const StoragePtr & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const
{
    return checkStructureAndGetMergeTreeData(*source_table, src_snapshot, my_snapshot);
}

/// must_on_same_disk=false is used only when attach partition; Both for same disk and different disk.
std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> MergeTreeData::cloneAndLoadDataPart(
    const MergeTreeData::DataPartPtr & src_part,
    const String & tmp_part_prefix,
    const MergeTreePartInfo & dst_part_info,
    const StorageMetadataPtr & metadata_snapshot,
    const IDataPartStorage::ClonePartParams & params,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    bool must_on_same_disk)
{
    chassert(!isStaticStorage());

    /// Check that the storage policy contains the disk where the src_part is located.
    bool on_same_disk = false;
    for (const DiskPtr & disk : getStoragePolicy()->getDisks())
    {
        if (disk->getName() == src_part->getDataPartStorage().getDiskName())
        {
            on_same_disk = true;
            break;
        }
    }
    if (!on_same_disk && must_on_same_disk)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Could not clone and load part {} because disk does not belong to storage policy",
            quoteString(src_part->getDataPartStorage().getFullPath()));

    String dst_part_name = src_part->getNewName(dst_part_info);
    String tmp_dst_part_name = tmp_part_prefix + dst_part_name;
    auto temporary_directory_lock = getTemporaryPartDirectoryHolder(tmp_dst_part_name);

    auto reservation = src_part->getDataPartStorage().reserve(src_part->getBytesOnDisk());
    auto src_part_storage = src_part->getDataPartStoragePtr();

    scope_guard src_flushed_tmp_dir_lock;
    MergeTreeData::MutableDataPartPtr src_flushed_tmp_part;

    String with_copy;
    if (params.copy_instead_of_hardlink)
        with_copy = " (copying data)";

    std::shared_ptr<IDataPartStorage> dst_part_storage{};
    if (on_same_disk)
    {
        dst_part_storage = src_part_storage->freeze(
            relative_data_path,
            tmp_dst_part_name,
            read_settings,
            write_settings,
            /* save_metadata_callback= */ {},
            params);
    }
    else
    {
        auto reservation_on_dst = getStoragePolicy()->reserve(src_part->getBytesOnDisk());
        if (!reservation_on_dst)
            throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Not enough space on disk.");
        dst_part_storage = src_part_storage->freezeRemote(
            relative_data_path,
            tmp_dst_part_name,
            /* dst_disk = */reservation_on_dst->getDisk(),
            read_settings,
            write_settings,
            /* save_metadata_callback= */ {},
            params
        );
    }

    if (params.metadata_version_to_write.has_value())
    {
        chassert(!params.keep_metadata_version);
        auto out_metadata = dst_part_storage->writeFile(IMergeTreeDataPart::METADATA_VERSION_FILE_NAME, 4096, getContext()->getWriteSettings());
        writeText(metadata_snapshot->getMetadataVersion(), *out_metadata);
        out_metadata->finalize();
        if ((*getSettings())[MergeTreeSetting::fsync_after_insert])
            out_metadata->sync();
    }

    LOG_DEBUG(log, "Clone{} part {} to {}{}",
              src_flushed_tmp_part ? " flushed" : "",
              src_part_storage->getFullPath(),
              std::string(fs::path(dst_part_storage->getFullRootPath()) / tmp_dst_part_name),
              with_copy);

    auto dst_data_part = MergeTreeDataPartBuilder(*this, dst_part_name, dst_part_storage, getReadSettings())
        .withPartFormatFromDisk()
        .build();

    if (!params.copy_instead_of_hardlink && params.hardlinked_files)
    {
        params.hardlinked_files->source_part_name = src_part->name;
        params.hardlinked_files->source_table_shared_id = src_part->storage.getTableSharedID();

        for (auto it = src_part->getDataPartStorage().iterate(); it->isValid(); it->next())
        {
            if (!params.files_to_copy_instead_of_hardlinks.contains(it->name())
                && it->name() != IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME_DEPRECATED
                && it->name() != IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME)
            {
                params.hardlinked_files->hardlinks_from_source_part.insert(it->name());
            }
        }

        auto projections = src_part->getProjectionParts();
        for (const auto & [name, projection_part] : projections)
        {
            const auto & projection_storage = projection_part->getDataPartStorage();
            for (auto it = projection_storage.iterate(); it->isValid(); it->next())
            {
                auto file_name_with_projection_prefix = fs::path(projection_storage.getPartDirectory()) / it->name();
                if (!params.files_to_copy_instead_of_hardlinks.contains(file_name_with_projection_prefix)
                    && it->name() != IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME_DEPRECATED
                    && it->name() != IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME)
                {
                    params.hardlinked_files->hardlinks_from_source_part.insert(file_name_with_projection_prefix);
                }
            }
        }
    }

    /// We should write version metadata on part creation to distinguish it from parts that were created without transaction.
    TransactionID tid = params.txn ? params.txn->tid : Tx::PrehistoricTID;
    dst_data_part->version.setCreationTID(tid, nullptr);
    dst_data_part->storeVersionMetadata();

    dst_data_part->is_temp = true;

    dst_data_part->loadColumnsChecksumsIndexes(require_part_metadata, true);
    dst_data_part->modification_time = dst_part_storage->getLastModified().epochTime();
    return std::make_pair(dst_data_part, std::move(temporary_directory_lock));
}

bool MergeTreeData::canUseAdaptiveGranularity() const
{
    const auto settings = getSettings();
    return (*settings)[MergeTreeSetting::index_granularity_bytes] != 0
        && ((*settings)[MergeTreeSetting::enable_mixed_granularity_parts] || !has_non_adaptive_index_granularity_parts);
}

String MergeTreeData::getFullPathOnDisk(const DiskPtr & disk) const
{
    return disk->getPath() + relative_data_path;
}


DiskPtr MergeTreeData::tryGetDiskForDetachedPart(const String & part_name) const
{
    const auto disks = getStoragePolicy()->getDisks();

    for (const DiskPtr & disk : disks)
        if (disk->existsDirectory(fs::path(relative_data_path) / DETACHED_DIR_NAME / part_name))
            return disk;

    return nullptr;
}

DiskPtr MergeTreeData::getDiskForDetachedPart(const String & part_name) const
{
    if (auto disk = tryGetDiskForDetachedPart(part_name))
        return disk;
    throw DB::Exception(ErrorCodes::BAD_DATA_PART_NAME, "Detached part \"{}\" not found", part_name);
}


Strings MergeTreeData::getDataPaths() const
{
    Strings res;
    auto disks = getStoragePolicy()->getDisks();
    for (const auto & disk : disks)
        res.push_back(getFullPathOnDisk(disk));
    return res;
}


void MergeTreeData::reportBrokenPart(MergeTreeData::DataPartPtr data_part) const
{
    if (!data_part)
        return;

    if (data_part->isProjectionPart())
    {
        String parent_part_name = data_part->getParentPartName();
        auto parent_part = getPartIfExists(parent_part_name, {DataPartState::PreActive, DataPartState::Active, DataPartState::Outdated});

        if (!parent_part)
        {
            LOG_WARNING(log, "Did not find parent part {} for potentially broken projection part {}",
                        parent_part_name, data_part->getDataPartStorage().getFullPath());
            return;
        }

        data_part = parent_part;
    }

    if (data_part->getDataPartStorage().isBroken())
    {
        auto parts = getDataPartsForInternalUsage();
        LOG_WARNING(log, "Scanning parts to recover on broken disk {}@{}.", data_part->getDataPartStorage().getDiskName(), data_part->getDataPartStorage().getDiskPath());

        for (const auto & part : parts)
        {
            if (part->getDataPartStorage().getDiskName() == data_part->getDataPartStorage().getDiskName())
                broken_part_callback(part->name);
        }
    }
    else
    {
        MergeTreeDataPartState state = MergeTreeDataPartState::Temporary;
        {
            auto lock = lockParts();
            state = data_part->getState();
        }

        if (state == MergeTreeDataPartState::Active)
            broken_part_callback(data_part->name);
        else
            LOG_DEBUG(log, "Will not check potentially broken part {} because it's not active", data_part->getNameWithState());
    }
}

MergeTreeData::MatcherFn MergeTreeData::getPartitionMatcher(const ASTPtr & partition_ast, ContextPtr local_context) const
{
    bool prefixed = false;
    String id;

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// Month-partitioning specific - partition value can represent a prefix of the partition to freeze.
        if (const auto * partition_lit = partition_ast->as<ASTPartition &>().value->as<ASTLiteral>())
        {
            id = partition_lit->value.getType() == Field::Types::UInt64
                 ? toString(partition_lit->value.safeGet<UInt64>())
                 : partition_lit->value.safeGet<String>();
            prefixed = true;
        }
        else
            id = getPartitionIDFromQuery(partition_ast, local_context);
    }
    else
        id = getPartitionIDFromQuery(partition_ast, local_context);

    return [prefixed, id](const String & partition_id)
    {
        if (prefixed)
            return startsWith(partition_id, id);

        return id == partition_id;
    };
}

PartitionCommandsResultInfo MergeTreeData::freezePartition(
    const ASTPtr & partition_ast,
    const String & with_name,
    ContextPtr local_context,
    TableLockHolder &)
{
    return freezePartitionsByMatcher(getPartitionMatcher(partition_ast, local_context), with_name, local_context);
}

PartitionCommandsResultInfo MergeTreeData::freezeAll(
    const String & with_name,
    ContextPtr local_context,
    TableLockHolder &)
{
    return freezePartitionsByMatcher([] (const String &) { return true; }, with_name, local_context);
}

PartitionCommandsResultInfo MergeTreeData::freezePartitionsByMatcher(
    MatcherFn matcher,
    const String & with_name,
    ContextPtr local_context)
{
    auto settings = getSettings();

    String clickhouse_path = fs::canonical(local_context->getPath());
    String default_shadow_path = fs::path(clickhouse_path) / "shadow/";
    fs::create_directories(default_shadow_path);
    auto increment = Increment(fs::path(default_shadow_path) / "increment.txt").get(true);

    const String shadow_path = "shadow/";

    /// Acquire a snapshot of active data parts to prevent removing while doing backup.
    const auto data_parts = getVisibleDataPartsVector(local_context);

    bool has_zero_copy_part = false;
    for (const auto & part : data_parts)
    {
        if (part->isStoredOnRemoteDiskWithZeroCopySupport())
        {
            has_zero_copy_part = true;
            break;
        }
    }

    if (supportsReplication() && (*settings)[MergeTreeSetting::disable_freeze_partition_for_zero_copy_replication]
        && (*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication] && has_zero_copy_part)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "FREEZE PARTITION queries are disabled.");

    String backup_name = (!with_name.empty() ? escapeForFileName(with_name) : toString(increment));
    String backup_path = fs::path(shadow_path) / backup_name / "";

    for (const auto & disk : getStoragePolicy()->getDisks())
        disk->onFreeze(backup_path);

    PartitionCommandsResultInfo result;

    size_t parts_processed = 0;
    for (const auto & part : data_parts)
    {
        if (!matcher(part->info.partition_id))
            continue;

        LOG_DEBUG(log, "Freezing part {} snapshot will be placed at {}", part->name, backup_path);

        auto data_part_storage = part->getDataPartStoragePtr();
        String backup_part_path = fs::path(backup_path) / relative_data_path;

        scope_guard src_flushed_tmp_dir_lock;
        MergeTreeData::MutableDataPartPtr src_flushed_tmp_part;

        auto callback = [this, &part, &backup_part_path](const DiskPtr & disk)
        {
            // Store metadata for replicated table.
            // Do nothing for non-replicated.
            createAndStoreFreezeMetadata(disk, part, fs::path(backup_part_path) / part->getDataPartStorage().getPartDirectory());
        };

        IDataPartStorage::ClonePartParams params
        {
            .make_source_readonly = true
        };
        auto new_storage = data_part_storage->freeze(
            backup_part_path,
            part->getDataPartStorage().getPartDirectory(),
            local_context->getReadSettings(),
            local_context->getWriteSettings(),
            callback,
            params);

        part->is_frozen.store(true, std::memory_order_relaxed);
        result.push_back(PartitionCommandResultInfo{
            .command_type = "FREEZE PART",
            .partition_id = part->info.partition_id,
            .part_name = part->name,
            .backup_path = new_storage->getFullRootPath(),
            .part_backup_path = new_storage->getFullPath(),
            .backup_name = backup_name,
        });
        ++parts_processed;
    }

    LOG_DEBUG(log, "Froze {} parts", parts_processed);
    return result;
}

void MergeTreeData::createAndStoreFreezeMetadata(DiskPtr, DataPartPtr, String) const
{

}

PartitionCommandsResultInfo MergeTreeData::unfreezePartition(
    const ASTPtr & partition,
    const String & backup_name,
    ContextPtr local_context,
    TableLockHolder &)
{
    return unfreezePartitionsByMatcher(getPartitionMatcher(partition, local_context), backup_name, local_context);
}

PartitionCommandsResultInfo MergeTreeData::unfreezeAll(
    const String & backup_name,
    ContextPtr local_context,
    TableLockHolder &)
{
    return unfreezePartitionsByMatcher([] (const String &) { return true; }, backup_name, local_context);
}

bool MergeTreeData::removeDetachedPart(DiskPtr disk, const String & path, const String &)
{
    disk->removeRecursive(path);

    return false;
}

PartitionCommandsResultInfo MergeTreeData::unfreezePartitionsByMatcher(MatcherFn matcher, const String & backup_name, ContextPtr local_context)
{
    auto backup_path = fs::path("shadow") / escapeForFileName(backup_name) / relative_data_path;

    LOG_DEBUG(log, "Unfreezing parts by path {}", backup_path.generic_string());

    auto disks = getStoragePolicy()->getDisks();

    return Unfreezer(local_context).unfreezePartitionsFromTableDirectory(matcher, backup_name, disks, backup_path);
}

bool MergeTreeData::canReplacePartition(const DataPartPtr & src_part) const
{
    const auto settings = getSettings();

    if (!(*settings)[MergeTreeSetting::enable_mixed_granularity_parts] || (*settings)[MergeTreeSetting::index_granularity_bytes] == 0)
    {
        if (!canUseAdaptiveGranularity() && src_part->index_granularity_info.mark_type.adaptive)
            return false;
        if (canUseAdaptiveGranularity() && !src_part->index_granularity_info.mark_type.adaptive)
            return false;
    }

    return true;
}

void MergeTreeData::writePartLog(
    PartLogElement::Type type,
    const ExecutionStatus & execution_status,
    UInt64 elapsed_ns,
    const String & new_part_name,
    const DataPartPtr & result_part,
    const DataPartsVector & source_parts,
    const MergeListEntry * merge_entry,
    std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters)
try
{
    auto table_id = getStorageID();
    auto part_log = getContext()->getPartLog(table_id.database_name);
    if (!part_log)
        return;

    PartLogElement part_log_elem;

    part_log_elem.event_type = type;

    if (part_log_elem.event_type == PartLogElement::MERGE_PARTS
        || part_log_elem.event_type == PartLogElement::MERGE_PARTS_START)
    {
        if (merge_entry)
        {
            part_log_elem.merge_reason = PartLogElement::getMergeReasonType((*merge_entry)->merge_type);
            part_log_elem.merge_algorithm = PartLogElement::getMergeAlgorithm((*merge_entry)->merge_algorithm);
        }
    }

    part_log_elem.error = static_cast<UInt16>(execution_status.code);
    part_log_elem.exception = execution_status.message;

    // construct event_time and event_time_microseconds using the same time point
    // so that the two times will always be equal up to a precision of a second.
    const auto time_now = std::chrono::system_clock::now();
    part_log_elem.event_time = timeInSeconds(time_now);
    part_log_elem.event_time_microseconds = timeInMicroseconds(time_now);

    /// TODO: Stop stopwatch in outer code to exclude ZK timings and so on
    part_log_elem.duration_ms = elapsed_ns / 1000000;

    part_log_elem.database_name = table_id.database_name;
    part_log_elem.table_name = table_id.table_name;
    part_log_elem.table_uuid = table_id.uuid;
    part_log_elem.partition_id = MergeTreePartInfo::fromPartName(new_part_name, format_version).partition_id;

    {
        const DataPart * result_or_source_data_part = nullptr;
        if (result_part)
            result_or_source_data_part = result_part.get();
        else if (!source_parts.empty())
            result_or_source_data_part = source_parts.at(0).get();
        if (result_or_source_data_part)
        {
            WriteBufferFromString out(part_log_elem.partition);
            result_or_source_data_part->partition.serializeText(*this, out, {});
        }
    }

    part_log_elem.part_name = new_part_name;

    if (result_part)
    {
        part_log_elem.disk_name = result_part->getDataPartStorage().getDiskName();
        part_log_elem.path_on_disk = result_part->getDataPartStorage().getFullPath();
        part_log_elem.bytes_compressed_on_disk = result_part->getBytesOnDisk();
        part_log_elem.bytes_uncompressed = result_part->getBytesUncompressedOnDisk();
        part_log_elem.rows = result_part->rows_count;
        part_log_elem.part_type = result_part->getType();
    }

    part_log_elem.source_part_names.reserve(source_parts.size());
    for (const auto & source_part : source_parts)
        part_log_elem.source_part_names.push_back(source_part->name);

    if (merge_entry)
    {
        part_log_elem.rows_read = (*merge_entry)->rows_read;
        part_log_elem.bytes_read_uncompressed = (*merge_entry)->bytes_read_uncompressed;

        part_log_elem.rows = (*merge_entry)->rows_written;
        part_log_elem.peak_memory_usage = (*merge_entry)->getMemoryTracker().getPeak();
    }

    if (profile_counters)
    {
        part_log_elem.profile_counters = profile_counters;
    }

    part_log->add(std::move(part_log_elem));
}
catch (...)
{
    tryLogCurrentException(log, __PRETTY_FUNCTION__);
}

StorageMergeTree::PinnedPartUUIDsPtr MergeTreeData::getPinnedPartUUIDs() const
{
    std::lock_guard lock(pinned_part_uuids_mutex);
    return pinned_part_uuids;
}

MergeTreeData::CurrentlyMovingPartsTagger::CurrentlyMovingPartsTagger(MergeTreeMovingParts && moving_parts_, MergeTreeData & data_)
    : parts_to_move(std::move(moving_parts_)), data(data_)
{
    for (const auto & moving_part : parts_to_move)
        if (!data.currently_moving_parts.emplace(moving_part.part).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot move part '{}'. It's already moving.", moving_part.part->name);
}

MergeTreeData::CurrentlyMovingPartsTagger::~CurrentlyMovingPartsTagger()
{
    std::lock_guard lock(data.moving_parts_mutex);
    for (auto & moving_part : parts_to_move)
    {
        /// Something went completely wrong
        if (!data.currently_moving_parts.contains(moving_part.part))
            std::terminate();
        data.currently_moving_parts.erase(moving_part.part);
    }
}

bool MergeTreeData::scheduleDataMovingJob(BackgroundJobsAssignee & assignee)
{
    if (parts_mover.moves_blocker.isCancelled())
        return false;

    auto moving_tagger = selectPartsForMove();
    if (moving_tagger->parts_to_move.empty())
        return false;

    assignee.scheduleMoveTask(std::make_shared<ExecutableLambdaAdapter>(
        [this, moving_tagger] () mutable
        {
            ReadSettings read_settings = Context::getGlobalContextInstance()->getReadSettings();
            WriteSettings write_settings = Context::getGlobalContextInstance()->getWriteSettings();
            return moveParts(moving_tagger, read_settings, write_settings, /* wait_for_move_if_zero_copy= */ false) == MovePartsOutcome::PartsMoved;
        }, moves_assignee_trigger, getStorageID()));
    return true;
}

bool MergeTreeData::areBackgroundMovesNeeded() const
{
    auto policy = getStoragePolicy();

    if (policy->getVolumes().size() > 1)
        return true;

    return policy->getVolumes().size() == 1 && policy->getVolumes()[0]->getDisks().size() > 1;
}

std::future<MovePartsOutcome> MergeTreeData::movePartsToSpace(const CurrentlyMovingPartsTaggerPtr & moving_tagger, const ReadSettings & read_settings, const WriteSettings & write_settings, bool async)
{
    auto finish_move_promise = std::make_shared<std::promise<MovePartsOutcome>>();
    auto finish_move_future = finish_move_promise->get_future();

    if (async)
    {
        bool is_scheduled = background_moves_assignee.scheduleMoveTask(std::make_shared<ExecutableLambdaAdapter>(
            [this, finish_move_promise, moving_tagger, read_settings, write_settings] () mutable
            {
                auto outcome = moveParts(moving_tagger, read_settings, write_settings, /* wait_for_move_if_zero_copy= */ true);

                finish_move_promise->set_value(outcome);

                return outcome == MovePartsOutcome::PartsMoved;
            }, moves_assignee_trigger, getStorageID()));

        if (!is_scheduled)
            finish_move_promise->set_value(MovePartsOutcome::CannotScheduleMove);
    }
    else
    {
        auto outcome = moveParts(moving_tagger, read_settings, write_settings, /* wait_for_move_if_zero_copy= */ true);
        finish_move_promise->set_value(outcome);
    }

    return finish_move_future;
}

MergeTreeData::CurrentlyMovingPartsTaggerPtr MergeTreeData::selectPartsForMove()
{
    MergeTreeMovingParts parts_to_move;

    auto can_move = [this](const DataPartPtr & part, String * reason) -> bool
    {
        if (partIsAssignedToBackgroundOperation(part))
        {
            *reason = "part already assigned to background operation.";
            return false;
        }
        if (currently_moving_parts.contains(part))
        {
            *reason = "part is already moving.";
            return false;
        }

        return true;
    };

    std::lock_guard moving_lock(moving_parts_mutex);

    parts_mover.selectPartsForMove(parts_to_move, can_move, moving_lock);
    return std::make_shared<CurrentlyMovingPartsTagger>(std::move(parts_to_move), *this);
}

MergeTreeData::CurrentlyMovingPartsTaggerPtr MergeTreeData::checkPartsForMove(const DataPartsVector & parts, SpacePtr space)
{
    std::lock_guard moving_lock(moving_parts_mutex);

    MergeTreeMovingParts parts_to_move;
    for (const auto & part : parts)
    {
        auto reservation = space->reserve(part->getBytesOnDisk());
        if (!reservation)
            throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Move is not possible. Not enough space on '{}'", space->getName());

        auto reserved_disk = reservation->getDisk();

        if (reserved_disk->existsDirectory(relative_data_path + part->name))
            throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Move is not possible: {} already exists",
                fullPath(reserved_disk, relative_data_path + part->name));

        if (currently_moving_parts.contains(part) || partIsAssignedToBackgroundOperation(part))
            throw Exception(ErrorCodes::PART_IS_TEMPORARILY_LOCKED,
                            "Cannot move part '{}' because it's participating in background process", part->name);

        parts_to_move.emplace_back(part, std::move(reservation));
    }
    return std::make_shared<CurrentlyMovingPartsTagger>(std::move(parts_to_move), *this);
}

MovePartsOutcome MergeTreeData::moveParts(const CurrentlyMovingPartsTaggerPtr & moving_tagger, const ReadSettings & read_settings, const WriteSettings & write_settings, bool wait_for_move_if_zero_copy)
{
    LOG_INFO(log, "Got {} parts to move.", moving_tagger->parts_to_move.size());

    const auto settings = getSettings();

    MovePartsOutcome result{MovePartsOutcome::PartsMoved};
    for (const auto & moving_part : moving_tagger->parts_to_move)
    {
        Stopwatch stopwatch;
        MergeTreePartsMover::TemporaryClonedPart cloned_part;
        ProfileEventsScope profile_events_scope;

        auto write_part_log = [&](const ExecutionStatus & execution_status)
        {
            writePartLog(
                PartLogElement::Type::MOVE_PART,
                execution_status,
                stopwatch.elapsed(),
                moving_part.part->name,
                cloned_part.part,
                {moving_part.part},
                nullptr,
                profile_events_scope.getSnapshot());
        };

        // Register in global moves list (StorageSystemMoves)
        auto moves_list_entry = getContext()->getMovesList().insert(
            getStorageID(),
            moving_part.part->name,
            moving_part.reserved_space->getDisk()->getName(),
            moving_part.reserved_space->getDisk()->getPath(),
            moving_part.part->getBytesOnDisk());

        try
        {
            /// If zero-copy replication enabled than replicas shouldn't try to
            /// move parts to another disk simultaneously. For this purpose we
            /// use shared lock across replicas. NOTE: it's not 100% reliable,
            /// because we are not checking lock while finishing part move.
            /// However it's not dangerous at all, we will just have very rare
            /// copies of some part.
            ///
            /// FIXME: this code is related to Replicated merge tree, and not
            /// common for ordinary merge tree. So it's a bad design and should
            /// be fixed.
            auto disk = moving_part.reserved_space->getDisk();
            if (supportsReplication() && disk->supportZeroCopyReplication() && (*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
            {
                /// This loop is not endless, if shutdown called/connection failed/replica became readonly
                /// we will return true from waitZeroCopyLock and createZeroCopyLock will return nullopt.
                while (true)
                {
                    /// If we acquired lock than let's try to move. After one
                    /// replica will actually move the part from disk to some
                    /// zero-copy storage other replicas will just fetch
                    /// metainformation.
                    if (auto lock = tryCreateZeroCopyExclusiveLock(moving_part.part->name, disk); lock)
                    {
                        if (lock->isLocked())
                        {
                            cloned_part = parts_mover.clonePart(moving_part, read_settings, write_settings);
                            parts_mover.swapClonedPart(cloned_part);
                            break;
                        }
                        if (wait_for_move_if_zero_copy)
                        {
                            LOG_DEBUG(log, "Other replica is working on move of {}, will wait until lock disappear", moving_part.part->name);
                            /// Wait and checks not only for timeout but also for shutdown and so on.
                            while (!waitZeroCopyLockToDisappear(*lock, 3000))
                            {
                                LOG_DEBUG(log, "Waiting until some replica will move {} and zero copy lock disappear", moving_part.part->name);
                            }
                        }
                        else
                            break;
                    }
                    else
                    {
                        /// Move will be retried but with backoff.
                        LOG_DEBUG(log, "Move of part {} postponed, because zero copy mode enabled and someone other moving this part right now", moving_part.part->name);
                        result = MovePartsOutcome::MoveWasPostponedBecauseOfZeroCopy;
                        break;
                    }
                }
            }
            else /// Ordinary move as it should be
            {
                cloned_part = parts_mover.clonePart(moving_part, read_settings, write_settings);
                parts_mover.swapClonedPart(cloned_part);
            }
            write_part_log({});
        }
        catch (...)
        {
            write_part_log(ExecutionStatus::fromCurrentException("", true));
            throw;
        }
    }
    return result;
}

bool MergeTreeData::partsContainSameProjections(const DataPartPtr & left, const DataPartPtr & right, PreformattedMessage & out_reason)
{
    auto remove_broken_parts_from_consideration = [](auto & parts)
    {
        std::set<String> broken_projection_parts;
        for (const auto & [name, part] : parts)
        {
            if (part->is_broken)
                broken_projection_parts.emplace(name);
        }
        for (const auto & name : broken_projection_parts)
            parts.erase(name);
    };

    auto left_projection_parts = left->getProjectionParts();
    auto right_projection_parts = right->getProjectionParts();

    remove_broken_parts_from_consideration(left_projection_parts);
    remove_broken_parts_from_consideration(right_projection_parts);

    if (left_projection_parts.size() != right_projection_parts.size())
    {
        out_reason = PreformattedMessage::create(
            "Parts have different number of projections: {} in part '{}' and {} in part '{}'",
            left_projection_parts.size(),
            left->name,
            right_projection_parts.size(),
            right->name
        );
        return false;
    }

    for (const auto & [name, _] : left_projection_parts)
    {
        if (!right_projection_parts.contains(name))
        {
            out_reason = PreformattedMessage::create(
                "The part '{}' doesn't have projection '{}' while part '{}' does", right->name, name, left->name
            );
            return false;
        }
    }
    return true;
}

bool MergeTreeData::canUsePolymorphicParts() const
{
    String unused;
    return canUsePolymorphicParts(*getSettings(), unused);
}


void MergeTreeData::checkDropCommandDoesntAffectInProgressMutations(const AlterCommand & command, const std::map<std::string, MutationCommands> & unfinished_mutations, ContextPtr local_context) const
{
    if (!command.isDropSomething() || unfinished_mutations.empty())
        return;

    auto throw_exception = [] (
        const std::string & mutation_name,
        const std::string & entity_name,
        const std::string & identifier_name)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot drop {} {} because it's affected by mutation with ID '{}' which is not finished yet. "
            "Wait this mutation, or KILL it with command "
            "\"KILL MUTATION WHERE mutation_id = '{}'\"",
            entity_name,
            backQuoteIfNeed(identifier_name),
            mutation_name,
            mutation_name);
    };

    for (const auto & [mutation_name, commands] : unfinished_mutations)
    {
        for (const MutationCommand & mutation_command : commands)
        {
            if (command.type == AlterCommand::DROP_INDEX && mutation_command.index_name == command.index_name)
            {
                throw_exception(mutation_name, "index", command.index_name);
            }
            else if (command.type == AlterCommand::DROP_PROJECTION
                     && mutation_command.projection_name == command.projection_name)
            {
                throw_exception(mutation_name, "projection", command.projection_name);
            }
            else if (command.type == AlterCommand::DROP_COLUMN)
            {
                if (mutation_command.column_name == command.column_name)
                    throw_exception(mutation_name, "column", command.column_name);

                if (mutation_command.predicate)
                {
                    auto query_tree = buildQueryTree(mutation_command.predicate, local_context);
                    auto identifiers = collectIdentifiersFullNames(query_tree);

                    if (identifiers.contains(command.column_name))
                        throw_exception(mutation_name, "column", command.column_name);
                }

                for (const auto & [name, expr] : mutation_command.column_to_update_expression)
                {
                    if (name == command.column_name)
                        throw_exception(mutation_name, "column", command.column_name);

                    auto query_tree = buildQueryTree(expr, local_context);
                    auto identifiers = collectIdentifiersFullNames(query_tree);
                    if (identifiers.contains(command.column_name))
                        throw_exception(mutation_name, "column", command.column_name);
                }
            }
            else if (command.type == AlterCommand::DROP_STATISTICS)
            {
                for (const auto & stats_col1 : command.statistics_columns)
                    for (const auto & stats_col2 : mutation_command.statistics_columns)
                        if (stats_col1 == stats_col2)
                            throw_exception(mutation_name, "statistics", stats_col1);
            }
        }
    }
}

bool MergeTreeData::canUsePolymorphicParts(const MergeTreeSettings & settings, String & out_reason) const
{
    if (!canUseAdaptiveGranularity())
    {
        if ((settings[MergeTreeSetting::min_rows_for_wide_part] != 0 || settings[MergeTreeSetting::min_bytes_for_wide_part] != 0
            || settings[MergeTreeSetting::min_rows_for_compact_part] != 0 || settings[MergeTreeSetting::min_bytes_for_compact_part] != 0))
        {
            out_reason = fmt::format(
                "Table can't create parts with adaptive granularity, but settings"
                " min_rows_for_wide_part = {}"
                ", min_bytes_for_wide_part = {}"
                ". Parts with non-adaptive granularity can be stored only in Wide (default) format.",
                settings[MergeTreeSetting::min_rows_for_wide_part], settings[MergeTreeSetting::min_bytes_for_wide_part]);
        }

        return false;
    }

    return true;
}

AlterConversionsPtr MergeTreeData::getAlterConversionsForPart(
    const MergeTreeDataPartPtr & part,
    const MutationsSnapshotPtr & mutations,
    const StorageMetadataPtr & metadata,
    const ContextPtr & query_context)
{
    auto commands = mutations->getAlterMutationCommandsForPart(part);
    auto result = std::make_shared<AlterConversions>(metadata, query_context);

    for (const auto & command : commands | std::views::reverse)
        result->addMutationCommand(command);

    return result;
}

size_t MergeTreeData::getTotalMergesWithTTLInMergeList() const
{
    return getContext()->getMergeList().getMergesWithTTLCount();
}

void MergeTreeData::addPartContributionToDataVolume(const DataPartPtr & part)
{
    increaseDataVolume(part->getBytesOnDisk(), part->rows_count, 1);
}

void MergeTreeData::removePartContributionToDataVolume(const DataPartPtr & part)
{
    increaseDataVolume(-part->getBytesOnDisk(), -part->rows_count, -1);
}

void MergeTreeData::increaseDataVolume(ssize_t bytes, ssize_t rows, ssize_t parts)
{
    total_active_size_bytes.fetch_add(bytes);
    total_active_size_rows.fetch_add(rows);
    total_active_size_parts.fetch_add(parts);
}

void MergeTreeData::setDataVolume(size_t bytes, size_t rows, size_t parts)
{
    total_active_size_bytes.store(bytes);
    total_active_size_rows.store(rows);
    total_active_size_parts.store(parts);
}

bool MergeTreeData::insertQueryIdOrThrow(const String & query_id, size_t max_queries) const
{
    std::lock_guard lock(query_id_set_mutex);
    return insertQueryIdOrThrowNoLock(query_id, max_queries);
}

bool MergeTreeData::insertQueryIdOrThrowNoLock(const String & query_id, size_t max_queries) const
{
    if (query_id_set.find(query_id) != query_id_set.end())
        return false;
    if (query_id_set.size() >= max_queries)
        throw Exception(
            ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
            "Too many simultaneous queries for table {}. Maximum is: {}",
            log.loadName(),
            max_queries);
    query_id_set.insert(query_id);
    return true;
}

void MergeTreeData::removeQueryId(const String & query_id) const
{
    std::lock_guard lock(query_id_set_mutex);
    removeQueryIdNoLock(query_id);
}

void MergeTreeData::removeQueryIdNoLock(const String & query_id) const
{
    if (query_id_set.find(query_id) == query_id_set.end())
        LOG_WARNING(log, "We have query_id removed but it's not recorded. This is a bug");
    else
        query_id_set.erase(query_id);
}

std::shared_ptr<QueryIdHolder> MergeTreeData::getQueryIdHolder(const String & query_id, UInt64 max_concurrent_queries) const
{
    auto lock = std::lock_guard<std::mutex>(query_id_set_mutex);
    if (insertQueryIdOrThrowNoLock(query_id, max_concurrent_queries))
    {
        try
        {
            return std::make_shared<QueryIdHolder>(query_id, *this);
        }
        catch (...)
        {
            /// If we fail to construct the holder, remove query_id explicitly to avoid leak.
            removeQueryIdNoLock(query_id);
            throw;
        }
    }
    return nullptr;
}

ReservationPtr MergeTreeData::balancedReservation(
    const StorageMetadataPtr & metadata_snapshot,
    size_t part_size,
    size_t max_volume_index,
    const String & part_name,
    const MergeTreePartInfo & part_info,
    MergeTreeData::DataPartsVector covered_parts,
    std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
    const IMergeTreeDataPart::TTLInfos * ttl_infos,
    bool is_insert)
{
    ReservationPtr reserved_space;
    auto min_bytes_to_rebalance_partition_over_jbod = (*getSettings())[MergeTreeSetting::min_bytes_to_rebalance_partition_over_jbod];
    if (tagger_ptr && min_bytes_to_rebalance_partition_over_jbod > 0 && part_size >= min_bytes_to_rebalance_partition_over_jbod)
    {
        try
        {
            const auto & disks = getStoragePolicy()->getVolume(max_volume_index)->getDisks();
            std::map<String, size_t> disk_occupation;
            std::map<String, std::vector<String>> disk_parts_for_logging;
            for (const auto & disk : disks)
                disk_occupation.emplace(disk->getName(), 0);

            std::set<String> committed_big_parts_from_partition;
            std::set<String> submerging_big_parts_from_partition;
            std::lock_guard lock(currently_submerging_emerging_mutex);

            for (const auto & part : currently_submerging_big_parts)
            {
                if (part_info.partition_id == part->info.partition_id)
                    submerging_big_parts_from_partition.insert(part->name);
            }

            {
                auto lock_parts = lockParts();
                if (covered_parts.empty())
                {
                    // It's a part fetch. Calculate `covered_parts` here.
                    MergeTreeData::DataPartPtr covering_part;
                    covered_parts = getActivePartsToReplace(part_info, part_name, covering_part, lock_parts);
                }

                // Remove irrelevant parts.
                std::erase_if(covered_parts,
                        [min_bytes_to_rebalance_partition_over_jbod](const auto & part)
                        {
                            return !(part->isStoredOnDisk() && part->getBytesOnDisk() >= min_bytes_to_rebalance_partition_over_jbod);
                        });

                // Include current submerging big parts which are not yet in `currently_submerging_big_parts`
                for (const auto & part : covered_parts)
                    submerging_big_parts_from_partition.insert(part->name);

                for (const auto & part : getDataPartsStateRange(MergeTreeData::DataPartState::Active))
                {
                    if (part->isStoredOnDisk() && part->getBytesOnDisk() >= min_bytes_to_rebalance_partition_over_jbod
                        && part_info.partition_id == part->info.partition_id)
                    {
                        auto name = part->getDataPartStorage().getDiskName();
                        auto it = disk_occupation.find(name);
                        if (it != disk_occupation.end())
                        {
                            if (submerging_big_parts_from_partition.find(part->name) == submerging_big_parts_from_partition.end())
                            {
                                it->second += part->getBytesOnDisk();
                                disk_parts_for_logging[name].push_back(formatReadableSizeWithBinarySuffix(part->getBytesOnDisk()));
                                committed_big_parts_from_partition.insert(part->name);
                            }
                            else
                            {
                                disk_parts_for_logging[name].push_back(formatReadableSizeWithBinarySuffix(part->getBytesOnDisk()) + " (submerging)");
                            }
                        }
                        else
                        {
                            // Part is on different volume. Ignore it.
                        }
                    }
                }
            }

            for (const auto & [name, emerging_part] : currently_emerging_big_parts)
            {
                // It's possible that the emerging big parts are committed and get added twice. Thus a set is used to deduplicate.
                if (committed_big_parts_from_partition.find(name) == committed_big_parts_from_partition.end()
                    && part_info.partition_id == emerging_part.partition_id)
                {
                    auto it = disk_occupation.find(emerging_part.disk_name);
                    if (it != disk_occupation.end())
                    {
                        it->second += emerging_part.estimate_bytes;
                        disk_parts_for_logging[emerging_part.disk_name].push_back(
                            formatReadableSizeWithBinarySuffix(emerging_part.estimate_bytes) + " (emerging)");
                    }
                    else
                    {
                        // Part is on different volume. Ignore it.
                    }
                }
            }

            size_t min_occupation_size = std::numeric_limits<size_t>::max();
            std::vector<String> candidates;
            for (const auto & [disk_name, size] : disk_occupation)
            {
                if (size < min_occupation_size)
                {
                    min_occupation_size = size;
                    candidates = {disk_name};
                }
                else if (size == min_occupation_size)
                {
                    candidates.push_back(disk_name);
                }
            }

            if (!candidates.empty())
            {
                // Random pick one disk from best candidates
                std::shuffle(candidates.begin(), candidates.end(), thread_local_rng);
                String selected_disk_name = candidates.front();
                WriteBufferFromOwnString log_str;
                writeCString("\nbalancer: \n", log_str);
                for (const auto & [disk_name, per_disk_parts] : disk_parts_for_logging)
                    writeString(fmt::format("  {}: [{}]\n", disk_name, fmt::join(per_disk_parts, ", ")), log_str);
                LOG_DEBUG(log, fmt::runtime(log_str.str()));

                if (ttl_infos)
                    reserved_space = tryReserveSpacePreferringTTLRules(
                        metadata_snapshot,
                        part_size,
                        *ttl_infos,
                        time(nullptr),
                        max_volume_index,
                        is_insert,
                        getStoragePolicy()->getDiskByName(selected_disk_name));
                else
                    reserved_space = tryReserveSpace(part_size, getStoragePolicy()->getDiskByName(selected_disk_name));

                if (reserved_space)
                {
                    currently_emerging_big_parts.emplace(
                        part_name, EmergingPartInfo{reserved_space->getDisk(0)->getName(), part_info.partition_id, part_size});

                    for (const auto & part : covered_parts)
                    {
                        if (currently_submerging_big_parts.contains(part))
                            LOG_WARNING(log, "currently_submerging_big_parts contains duplicates. JBOD might lose balance");
                        else
                            currently_submerging_big_parts.insert(part);
                    }

                    // Record submerging big parts in the tagger to clean them up.
                    tagger_ptr->emplace(*this, part_name, std::move(covered_parts), log.load());
                }
            }
        }
        catch (...)
        {
            LOG_DEBUG(log, "JBOD balancer encounters an error. Fallback to random disk selection");
            tryLogCurrentException(log);
        }
    }
    return reserved_space;
}

ColumnsDescription MergeTreeData::getConcreteObjectColumns(
    const DataPartsVector & parts, const ColumnsDescription & storage_columns)
{
    return DB::getConcreteObjectColumns(
        parts.begin(), parts.end(),
        storage_columns, [](const auto & part) -> const auto & { return part->getColumns(); });
}

ColumnsDescription MergeTreeData::getConcreteObjectColumns(
    boost::iterator_range<DataPartIteratorByStateAndInfo> range, const ColumnsDescription & storage_columns)
{
    return DB::getConcreteObjectColumns(
        range.begin(), range.end(),
        storage_columns, [](const auto & part) -> const auto & { return part->getColumns(); });
}

void MergeTreeData::resetObjectColumnsFromActiveParts(const DataPartsLock & /*lock*/)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & columns = metadata_snapshot->getColumns();
    if (!hasDynamicSubcolumns(columns))
        return;

    auto range = getDataPartsStateRange(DataPartState::Active);
    object_columns = getConcreteObjectColumns(range, columns);
}

void MergeTreeData::updateObjectColumns(const DataPartPtr & part, const DataPartsLock & /*lock*/)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & columns = metadata_snapshot->getColumns();
    if (!hasDynamicSubcolumns(columns))
        return;

    DB::updateObjectColumns(object_columns, columns, part->getColumns());
}

template <typename DataPartPtr>
static void updateSerializationHintsForPart(const DataPartPtr & part, const ColumnsDescription & storage_columns, SerializationInfoByName & hints, bool remove)
{
    const auto & part_columns = part->getColumnsDescription();
    for (const auto & [name, info] : part->getSerializationInfos())
    {
        auto new_hint = hints.tryGet(name);
        if (!new_hint)
            continue;

        /// Structure may change after alter. Do not add info for such items.
        /// Instead it will be updated on commit of the result part of alter.
        if (part_columns.tryGetPhysical(name) != storage_columns.tryGetPhysical(name))
            continue;

        chassert(new_hint->structureEquals(*info));
        if (remove)
            new_hint->remove(*info);
        else
            new_hint->add(*info);
    }
}

void MergeTreeData::resetSerializationHints(const DataPartsLock & /*lock*/)
{
    SerializationInfo::Settings settings =
    {
        .ratio_of_defaults_for_sparse = (*getSettings())[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization],
        .choose_kind = true,
    };

    const auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & storage_columns = metadata_snapshot->getColumns();

    serialization_hints = SerializationInfoByName(storage_columns.getAllPhysical(), settings);
    auto range = getDataPartsStateRange(DataPartState::Active);

    for (const auto & part : range)
        updateSerializationHintsForPart(part, storage_columns, serialization_hints, false);
}

template <typename AddedParts, typename RemovedParts>
void MergeTreeData::updateSerializationHints(const AddedParts & added_parts, const RemovedParts & removed_parts, const DataPartsLock & /*lock*/)
{
    const auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & storage_columns = metadata_snapshot->getColumns();

    for (const auto & part : added_parts)
        updateSerializationHintsForPart(part, storage_columns, serialization_hints, false);

    for (const auto & part : removed_parts)
        updateSerializationHintsForPart(part, storage_columns, serialization_hints, true);
}

SerializationInfoByName MergeTreeData::getSerializationHints() const
{
    auto lock = lockParts();
    SerializationInfoByName res;
    for (const auto & [name, info] : serialization_hints)
        res.emplace(name, info->clone());
    return res;
}

bool MergeTreeData::supportsTrivialCountOptimization(const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context) const
{
    if (hasLightweightDeletedMask())
        return false;

    if (!storage_snapshot)
        return !query_context->getSettingsRef()[Setting::apply_mutations_on_fly];

    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);
    return !snapshot_data.mutations_snapshot->hasDataMutations();
}

Int64 MergeTreeData::getMinMetadataVersion(const DataPartsVector & parts)
{
    Int64 version = -1;
    for (const auto & part : parts)
    {
        Int64 part_version = part->getMetadataVersion();
        if (version == -1 || part_version < version)
            version = part_version;
    }
    return version;
}

StorageSnapshotPtr MergeTreeData::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    auto snapshot_data = std::make_unique<SnapshotData>();
    ColumnsDescription object_columns_copy;

    {
        auto lock = lockParts();
        snapshot_data->parts = getVisibleDataPartsVectorUnlocked(query_context, lock);
        object_columns_copy = object_columns;
    }

    IMutationsSnapshot::Params params
    {
        .metadata_version = metadata_snapshot->getMetadataVersion(),
        .min_part_metadata_version = getMinMetadataVersion(snapshot_data->parts),
        .need_data_mutations = query_context->getSettingsRef()[Setting::apply_mutations_on_fly],
    };

    snapshot_data->mutations_snapshot = getMutationsSnapshot(params);
    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, std::move(object_columns_copy), std::move(snapshot_data));
}

StorageSnapshotPtr MergeTreeData::getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr) const
{
    auto lock = lockParts();
    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, object_columns, std::make_unique<SnapshotData>());
}

void MergeTreeData::incrementInsertedPartsProfileEvent(MergeTreeDataPartType type)
{
    switch (type.getValue())
    {
        case MergeTreeDataPartType::Wide:
            ProfileEvents::increment(ProfileEvents::InsertedWideParts);
            break;
        case MergeTreeDataPartType::Compact:
            ProfileEvents::increment(ProfileEvents::InsertedCompactParts);
            break;
        default:
            break;
    }
}

void MergeTreeData::incrementMergedPartsProfileEvent(MergeTreeDataPartType type)
{
    switch (type.getValue())
    {
        case MergeTreeDataPartType::Wide:
            ProfileEvents::increment(ProfileEvents::MergedIntoWideParts);
            break;
        case MergeTreeDataPartType::Compact:
            ProfileEvents::increment(ProfileEvents::MergedIntoCompactParts);
            break;
        default:
            break;
    }
}

std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> MergeTreeData::createEmptyPart(
        MergeTreePartInfo & new_part_info, const MergeTreePartition & partition, const String & new_part_name,
        const MergeTreeTransactionPtr & txn)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto settings = getSettings();

    auto block = metadata_snapshot->getSampleBlock();
    NamesAndTypesList columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());
    setAllObjectsToDummyTupleType(columns);

    auto minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
    minmax_idx->update(block, getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));

    DB::IMergeTreeDataPart::TTLInfos move_ttl_infos;
    VolumePtr volume = getStoragePolicy()->getVolume(0);
    ReservationPtr reservation = reserveSpacePreferringTTLRules(metadata_snapshot, 0, move_ttl_infos, time(nullptr), 0, true);
    VolumePtr data_part_volume = createVolumeFromReservation(reservation, volume);

    auto tmp_dir_holder = getTemporaryPartDirectoryHolder(EMPTY_PART_TMP_PREFIX + new_part_name);
    auto new_data_part = getDataPartBuilder(new_part_name, data_part_volume, EMPTY_PART_TMP_PREFIX + new_part_name, getReadSettings())
        .withBytesAndRowsOnDisk(0, 0)
        .withPartInfo(new_part_info)
        .build();

    if ((*settings)[MergeTreeSetting::assign_part_uuids])
        new_data_part->uuid = UUIDHelpers::generateV4();

    new_data_part->setColumns(columns, {}, metadata_snapshot->getMetadataVersion());
    new_data_part->rows_count = block.rows();
    new_data_part->existing_rows_count = block.rows();

    new_data_part->partition = partition;

    new_data_part->minmax_idx = std::move(minmax_idx);
    new_data_part->is_temp = true;
    /// In case of replicated merge tree with zero copy replication
    /// Here Clickhouse claims that this new part can be deleted in temporary state without unlocking the blobs
    /// The blobs have to be removed along with the part, this temporary part owns them and does not share them yet.
    new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;

    auto new_data_part_storage = new_data_part->getDataPartStoragePtr();
    new_data_part_storage->beginTransaction();

    SyncGuardPtr sync_guard;
    if (new_data_part->isStoredOnDisk())
    {
        /// The name could be non-unique in case of stale files from previous runs.
        if (new_data_part_storage->exists())
        {
            /// The path has to be unique, all tmp directories are deleted at startup in case of stale files from previous runs.
            /// New part have to capture its name, therefore there is no concurrentcy in directory creation
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "New empty part is about to matirialize but the dirrectory already exist"
                            ", new part {}"
                            ", directory {}",
                            new_part_name, new_data_part_storage->getFullPath());
        }

        new_data_part_storage->createDirectories();

        if ((*getSettings())[MergeTreeSetting::fsync_part_directory])
            sync_guard = new_data_part_storage->getDirectorySyncGuard();
    }

    /// This effectively chooses minimal compression method:
    ///  either default lz4 or compression method with zero thresholds on absolute and relative part size.
    auto compression_codec = getContext()->chooseCompressionCodec(0, 0);

    const auto & index_factory = MergeTreeIndexFactory::instance();
    MergedBlockOutputStream out(new_data_part, metadata_snapshot, columns,
        index_factory.getMany(metadata_snapshot->getSecondaryIndices()),
        ColumnsStatistics{},
        compression_codec, txn ? txn->tid : Tx::PrehistoricTID);

    bool sync_on_insert = (*settings)[MergeTreeSetting::fsync_after_insert];

    out.write(block);
    /// Here is no projections as no data inside
    out.finalizePart(new_data_part, sync_on_insert);

    new_data_part_storage->precommitTransaction();
    return std::make_pair(std::move(new_data_part), std::move(tmp_dir_holder));
}

bool MergeTreeData::allowRemoveStaleMovingParts() const
{
    return ConfigHelper::getBool(getContext()->getConfigRef(), "allow_remove_stale_moving_parts", /* default_ = */ true);
}

CurrentlySubmergingEmergingTagger::~CurrentlySubmergingEmergingTagger()
{
    std::lock_guard lock(storage.currently_submerging_emerging_mutex);

    for (const auto & part : submerging_parts)
    {
        if (!storage.currently_submerging_big_parts.contains(part))
        {
            LOG_ERROR(log, "currently_submerging_big_parts doesn't contain part {} to erase. This is a bug", part->name);
            assert(false);
        }
        else
            storage.currently_submerging_big_parts.erase(part);
    }
    storage.currently_emerging_big_parts.erase(emerging_part_name);
}

bool MergeTreeData::initializeDiskOnConfigChange(const std::set<String> & new_added_disks)
{
    auto storage_policy = getStoragePolicy();
    const auto format_version_path = fs::path(relative_data_path) / MergeTreeData::FORMAT_VERSION_FILE_NAME;
    for (const auto & name : new_added_disks)
    {
        auto disk = storage_policy->tryGetDiskByName(name);
        if (disk)
        {
            disk->createDirectories(relative_data_path);
            disk->createDirectories(fs::path(relative_data_path) / MergeTreeData::DETACHED_DIR_NAME);
            auto buf = disk->writeFile(format_version_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, getContext()->getWriteSettings());
            writeIntText(format_version.toUnderType(), *buf);
            buf->finalize();
            if (getContext()->getSettingsRef()[Setting::fsync_metadata])
                buf->sync();
        }
    }
    return true;
}

void MergeTreeData::unloadPrimaryKeys()
{
    for (auto & part : getAllDataPartsVector())
    {
        const_cast<IMergeTreeDataPart &>(*part).unloadIndex();
    }
}

size_t MergeTreeData::unloadPrimaryKeysOfOutdatedParts()
{
    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock lock(unload_primary_key_mutex, std::defer_lock);
    if (!lock.try_lock())
        return 0;

    DataPartsVector parts_to_unload_index;

    {
        auto parts_lock = lockParts();
        auto parts_range = getDataPartsStateRange(DataPartState::Outdated);

        for (const auto & part : parts_range)
        {
            /// Outdated part may be hold by SELECT query and still needs the index.
            /// This check requires lock of index_mutex but if outdated part is unique then there is no
            /// contention on it, so it's relatively cheap and it's ok to check under a global parts lock.
            if (isSharedPtrUnique(part) && part->isIndexLoaded())
                parts_to_unload_index.push_back(part);
        }
    }

    for (const auto & part : parts_to_unload_index)
    {
        const_cast<IMergeTreeDataPart &>(*part).unloadIndex();
        LOG_TEST(log, "Unloaded primary key for outdated part {}", part->name);
    }

    return parts_to_unload_index.size();
}

void MergeTreeData::verifySortingKey(const KeyDescription & sorting_key)
{
    /// Aggregate functions already forbidden, but SimpleAggregateFunction are not
    for (const auto & data_type : sorting_key.data_types)
    {
        if (dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(data_type->getCustomName()))
            throw Exception(ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_KEY, "Column with type {} is not allowed in key expression", data_type->getCustomName()->getName());
    }
}

static void updateMutationsCounters(
    Int64 & num_data_mutations_to_apply,
    Int64 & num_metadata_mutations_to_apply,
    const MutationCommands & commands,
    Int64 increment)
{
    if (num_data_mutations_to_apply < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "On-fly data mutations counter is negative ({})", num_data_mutations_to_apply);

    if (num_metadata_mutations_to_apply < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "On-fly metadata mutations counter is negative ({})", num_metadata_mutations_to_apply);

    bool has_data_mutation = false;
    bool has_metadata_mutation = false;

    for (const auto & command : commands)
    {
        if (!has_data_mutation && AlterConversions::isSupportedDataMutation(command.type))
        {
            num_data_mutations_to_apply += increment;
            has_data_mutation = true;

            if (num_data_mutations_to_apply < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "On-fly data mutations counter is negative ({})", num_data_mutations_to_apply);
        }

        if (!has_metadata_mutation && AlterConversions::isSupportedMetadataMutation(command.type))
        {
            num_metadata_mutations_to_apply += increment;
            has_metadata_mutation = true;

            if (num_metadata_mutations_to_apply < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "On-fly metadata mutations counter is negative ({})", num_metadata_mutations_to_apply);
        }
    }
}

void incrementMutationsCounters(
    Int64 & num_data_mutations_to_apply,
    Int64 & num_metadata_mutations_to_apply,
    const MutationCommands & commands)
{
    updateMutationsCounters(num_data_mutations_to_apply, num_metadata_mutations_to_apply, commands, 1);
}

void decrementMutationsCounters(
    Int64 & num_data_mutations_to_apply,
    Int64 & num_metadata_mutations_to_apply,
    const MutationCommands & commands)
{
    updateMutationsCounters(num_data_mutations_to_apply, num_metadata_mutations_to_apply, commands, -1);
}

}
