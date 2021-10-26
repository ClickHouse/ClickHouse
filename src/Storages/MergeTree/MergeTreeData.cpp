#include <Storages/MergeTree/MergeTreeData.h>

#include <Backups/BackupEntryFromImmutableFile.h>
#include <Backups/BackupEntryFromSmallFile.h>
#include <Backups/IBackup.h>
#include <Compression/CompressedReadBuffer.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/localBackup.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Increment.h>
#include <Common/SimpleIncrement.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Formats/IInputFormat.h>
#include <AggregateFunctions/AggregateFunctionCount.h>

#include <boost/range/adaptor/filtered.hpp>
#include <boost/algorithm/string/join.hpp>

#include <base/insertAtEnd.h>
#include <base/scope_guard_safe.h>

#include <algorithm>
#include <iomanip>
#include <optional>
#include <set>
#include <thread>
#include <typeinfo>
#include <typeindex>
#include <unordered_set>
#include <filesystem>


namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event RejectedInserts;
    extern const Event DelayedInserts;
    extern const Event DelayedInsertsMilliseconds;
    extern const Event DuplicatedInsertedBlocks;
}

namespace CurrentMetrics
{
    extern const Metric DelayedInserts;
    extern const Metric BackgroundMovePoolTask;
}


namespace
{
    constexpr UInt64 RESERVATION_MIN_ESTIMATION_SIZE = 1u * 1024u * 1024u; /// 1MB
}


namespace DB
{

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
    extern const int UNKNOWN_PART_TYPE;
    extern const int UNKNOWN_DISK;
    extern const int NOT_ENOUGH_SPACE;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int INCORRECT_QUERY;
}

static void checkSampleExpression(const StorageInMemoryMetadata & metadata, bool allow_sampling_expression_not_in_primary_key, bool check_sample_column_is_correct)
{
    if (metadata.sampling_key.column_names.empty())
        throw Exception("There are no columns in sampling expression", ErrorCodes::INCORRECT_QUERY);

    const auto & pk_sample_block = metadata.getPrimaryKey().sample_block;
    if (!pk_sample_block.has(metadata.sampling_key.column_names[0]) && !allow_sampling_expression_not_in_primary_key)
        throw Exception("Sampling expression must be present in the primary key", ErrorCodes::BAD_ARGUMENTS);

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
        throw Exception(
            "Invalid sampling column type in storage parameters: " + sampling_column_type->getName()
            + ". Must be one unsigned integer type",
            ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

MergeTreeData::MergeTreeData(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool require_part_metadata_,
    bool attach,
    BrokenPartCallback broken_part_callback_)
    : IStorage(table_id_)
    , WithMutableContext(context_->getGlobalContext())
    , merging_params(merging_params_)
    , require_part_metadata(require_part_metadata_)
    , relative_data_path(relative_data_path_)
    , broken_part_callback(broken_part_callback_)
    , log_name(table_id_.getNameForLogs())
    , log(&Poco::Logger::get(log_name))
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
    allow_nullable_key = attach || settings->allow_nullable_key;

    if (relative_data_path.empty())
        throw Exception("MergeTree storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    /// Check sanity of MergeTreeSettings. Only when table is created.
    if (!attach)
        settings->sanityCheck(getContext()->getSettingsRef());

    MergeTreeDataFormatVersion min_format_version(0);
    if (!date_column_name.empty())
    {
        try
        {

            checkPartitionKeyAndInitMinMax(metadata_.partition_key);
            setProperties(metadata_, metadata_, attach);
            if (minmax_idx_date_column_pos == -1)
                throw Exception("Could not find Date column", ErrorCodes::BAD_TYPE_OF_FIELD);
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
        min_format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    }
    setProperties(metadata_, metadata_, attach);

    /// NOTE: using the same columns list as is read when performing actual merges.
    merging_params.check(metadata_);

    if (metadata_.sampling_key.definition_ast != nullptr)
    {
        /// This is for backward compatibility.
        checkSampleExpression(metadata_, attach || settings->compatibility_allow_sampling_expression_not_in_primary_key,
                              settings->check_sample_column_is_correct && !attach);
    }

    checkTTLExpressions(metadata_, metadata_);

    /// format_file always contained on any data path
    PathWithDisk version_file;
    /// Creating directories, if not exist.
    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
    {
        disk->createDirectories(path);
        disk->createDirectories(fs::path(path) / MergeTreeData::DETACHED_DIR_NAME);
        String current_version_file_path = fs::path(path) / MergeTreeData::FORMAT_VERSION_FILE_NAME;
        if (disk->exists(current_version_file_path))
        {
            if (!version_file.first.empty())
            {
                LOG_ERROR(log, "Duplication of version file {} and {}", fullPath(version_file.second, version_file.first), current_version_file_path);
                throw Exception("Multiple format_version.txt file", ErrorCodes::CORRUPTED_DATA);
            }
            version_file = {current_version_file_path, disk};
        }
    }

    /// If not choose any
    if (version_file.first.empty())
        version_file = {fs::path(relative_data_path) / MergeTreeData::FORMAT_VERSION_FILE_NAME, getStoragePolicy()->getAnyDisk()};

    bool version_file_exists = version_file.second->exists(version_file.first);

    // When data path or file not exists, ignore the format_version check
    if (!attach || !version_file_exists)
    {
        format_version = min_format_version;
        if (!version_file.second->isReadOnly())
        {
            auto buf = version_file.second->writeFile(version_file.first);
            writeIntText(format_version.toUnderType(), *buf);
            if (getContext()->getSettingsRef().fsync_metadata)
                buf->sync();
        }
    }
    else
    {
        auto buf = version_file.second->readFile(version_file.first);
        UInt32 read_format_version;
        readIntText(read_format_version, *buf);
        format_version = read_format_version;
        if (!buf->eof())
            throw Exception("Bad version file: " + fullPath(version_file.second, version_file.first), ErrorCodes::CORRUPTED_DATA);
    }

    if (format_version < min_format_version)
    {
        if (min_format_version == MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING.toUnderType())
            throw Exception(
                "MergeTree data format version on disk doesn't support custom partitioning",
                ErrorCodes::METADATA_MISMATCH);
    }

    String reason;
    if (!canUsePolymorphicParts(*settings, &reason) && !reason.empty())
        LOG_WARNING(log, "{} Settings 'min_rows_for_wide_part', 'min_bytes_for_wide_part', "
            "'min_rows_for_compact_part' and 'min_bytes_for_compact_part' will be ignored.", reason);

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

StoragePolicyPtr MergeTreeData::getStoragePolicy() const
{
    return getContext()->getStoragePolicy(getSettings()->storage_policy);
}

static void checkKeyExpression(const ExpressionActions & expr, const Block & sample_block, const String & key_name, bool allow_nullable_key)
{
    if (expr.hasArrayJoin())
        throw Exception(key_name + " key cannot contain array joins", ErrorCodes::ILLEGAL_COLUMN);

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
            throw Exception{key_name + " key cannot contain constants", ErrorCodes::ILLEGAL_COLUMN};

        if (!allow_nullable_key && element.type->isNullable())
            throw Exception{key_name + " key cannot contain nullable columns", ErrorCodes::ILLEGAL_COLUMN};
    }
}

void MergeTreeData::checkProperties(
    const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata, bool attach) const
{
    if (!new_metadata.sorting_key.definition_ast)
        throw Exception("ORDER BY cannot be empty", ErrorCodes::BAD_ARGUMENTS);

    KeyDescription new_sorting_key = new_metadata.sorting_key;
    KeyDescription new_primary_key = new_metadata.primary_key;

    size_t sorting_key_size = new_sorting_key.column_names.size();
    size_t primary_key_size = new_primary_key.column_names.size();
    if (primary_key_size > sorting_key_size)
        throw Exception("Primary key must be a prefix of the sorting key, but its length: "
            + toString(primary_key_size) + " is greater than the sorting key length: " + toString(sorting_key_size),
            ErrorCodes::BAD_ARGUMENTS);

    NameSet primary_key_columns_set;

    for (size_t i = 0; i < sorting_key_size; ++i)
    {
        const String & sorting_key_column = new_sorting_key.column_names[i];

        if (i < primary_key_size)
        {
            const String & pk_column = new_primary_key.column_names[i];
            if (pk_column != sorting_key_column)
                throw Exception("Primary key must be a prefix of the sorting key, but the column in the position "
                    + toString(i) + " is " + sorting_key_column +", not " + pk_column,
                    ErrorCodes::BAD_ARGUMENTS);

            if (!primary_key_columns_set.emplace(pk_column).second)
                throw Exception("Primary key contains duplicate columns", ErrorCodes::BAD_ARGUMENTS);

        }
    }

    auto all_columns = new_metadata.columns.getAllPhysical();

    /// Order by check AST
    if (old_metadata.hasSortingKey())
    {
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
                    throw Exception("Existing column " + backQuoteIfNeed(col) + " is used in the expression that was "
                        "added to the sorting key. You can add expressions that use only the newly added columns",
                        ErrorCodes::BAD_ARGUMENTS);

                if (new_metadata.columns.getDefaults().count(col))
                    throw Exception("Newly added column " + backQuoteIfNeed(col) + " has a default expression, so adding "
                        "expressions that use it to the sorting key is forbidden",
                        ErrorCodes::BAD_ARGUMENTS);
            }
        }
    }

    if (!new_metadata.secondary_indices.empty())
    {
        std::unordered_set<String> indices_names;

        for (const auto & index : new_metadata.secondary_indices)
        {

            MergeTreeIndexFactory::instance().validate(index, attach);

            if (indices_names.find(index.name) != indices_names.end())
                throw Exception(
                        "Index with name " + backQuote(index.name) + " already exists",
                        ErrorCodes::LOGICAL_ERROR);

            indices_names.insert(index.name);
        }
    }

    if (!new_metadata.projections.empty())
    {
        std::unordered_set<String> projections_names;

        for (const auto & projection : new_metadata.projections)
        {
            if (projections_names.find(projection.name) != projections_names.end())
                throw Exception(
                        "Projection with name " + backQuote(projection.name) + " already exists",
                        ErrorCodes::LOGICAL_ERROR);

            projections_names.insert(projection.name);
        }
    }

    checkKeyExpression(*new_sorting_key.expression, new_sorting_key.sample_block, "Sorting", allow_nullable_key);
}

void MergeTreeData::setProperties(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata, bool attach)
{
    checkProperties(new_metadata, old_metadata, attach);
    setInMemoryMetadata(new_metadata);
}

namespace
{

ExpressionActionsPtr getCombinedIndicesExpression(
    const KeyDescription & key,
    const IndicesDescription & indices,
    const ColumnsDescription & columns,
    ContextPtr context)
{
    ASTPtr combined_expr_list = key.expression_list_ast->clone();

    for (const auto & index : indices)
        for (const auto & index_expr : index.expression_list_ast->children)
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

    return std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(partition_key_columns), settings);
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

ExpressionActionsPtr MergeTreeData::getPrimaryKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot) const
{
    return getCombinedIndicesExpression(metadata_snapshot->getPrimaryKey(), metadata_snapshot->getSecondaryIndices(), metadata_snapshot->getColumns(), getContext());
}

ExpressionActionsPtr MergeTreeData::getSortingKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot) const
{
    return getCombinedIndicesExpression(metadata_snapshot->getSortingKey(), metadata_snapshot->getSecondaryIndices(), metadata_snapshot->getColumns(), getContext());
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
            if (columns_ttl_forbidden.count(name))
                throw Exception("Trying to set TTL for key column " + name, ErrorCodes::ILLEGAL_COLUMN);
        }
    }
    auto new_table_ttl = new_metadata.table_ttl;

    if (new_table_ttl.definition_ast)
    {
        for (const auto & move_ttl : new_table_ttl.move_ttl)
        {
            if (!getDestinationForMoveTTL(move_ttl))
            {
                String message;
                if (move_ttl.destination_type == DataDestinationType::DISK)
                    message = "No such disk " + backQuote(move_ttl.destination_name) + " for given storage policy.";
                else
                    message = "No such volume " + backQuote(move_ttl.destination_name) + " for given storage policy.";
                throw Exception(message, ErrorCodes::BAD_TTL_EXPRESSION);
            }
        }
    }
}


void MergeTreeData::checkStoragePolicy(const StoragePolicyPtr & new_storage_policy) const
{
    const auto old_storage_policy = getStoragePolicy();
    old_storage_policy->checkCompatibleWith(new_storage_policy);
}


void MergeTreeData::MergingParams::check(const StorageInMemoryMetadata & metadata) const
{
    const auto columns = metadata.getColumns().getAllPhysical();

    if (!sign_column.empty() && mode != MergingParams::Collapsing && mode != MergingParams::VersionedCollapsing)
        throw Exception("Sign column for MergeTree cannot be specified in modes except Collapsing or VersionedCollapsing.",
                        ErrorCodes::LOGICAL_ERROR);

    if (!version_column.empty() && mode != MergingParams::Replacing && mode != MergingParams::VersionedCollapsing)
        throw Exception("Version column for MergeTree cannot be specified in modes except Replacing or VersionedCollapsing.",
                        ErrorCodes::LOGICAL_ERROR);

    if (!columns_to_sum.empty() && mode != MergingParams::Summing)
        throw Exception("List of columns to sum for MergeTree cannot be specified in all modes except Summing.",
                        ErrorCodes::LOGICAL_ERROR);

    /// Check that if the sign column is needed, it exists and is of type Int8.
    auto check_sign_column = [this, & columns](bool is_optional, const std::string & storage)
    {
        if (sign_column.empty())
        {
            if (is_optional)
                return;

            throw Exception("Logical error: Sign column for storage " + storage + " is empty", ErrorCodes::LOGICAL_ERROR);
        }

        bool miss_column = true;
        for (const auto & column : columns)
        {
            if (column.name == sign_column)
            {
                if (!typeid_cast<const DataTypeInt8 *>(column.type.get()))
                    throw Exception("Sign column (" + sign_column + ") for storage " + storage + " must have type Int8."
                            " Provided column of type " + column.type->getName() + ".", ErrorCodes::BAD_TYPE_OF_FIELD);
                miss_column = false;
                break;
            }
        }
        if (miss_column)
            throw Exception("Sign column " + sign_column + " does not exist in table declaration.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    };

    /// that if the version_column column is needed, it exists and is of unsigned integer type.
    auto check_version_column = [this, & columns](bool is_optional, const std::string & storage)
    {
        if (version_column.empty())
        {
            if (is_optional)
                return;

            throw Exception("Logical error: Version column for storage " + storage + " is empty", ErrorCodes::LOGICAL_ERROR);
        }

        bool miss_column = true;
        for (const auto & column : columns)
        {
            if (column.name == version_column)
            {
                if (!column.type->canBeUsedAsVersion())
                    throw Exception("The column " + version_column +
                        " cannot be used as a version column for storage " + storage +
                        " because it is of type " + column.type->getName() +
                        " (must be of an integer type or of type Date/DateTime/DateTime64)", ErrorCodes::BAD_TYPE_OF_FIELD);
                miss_column = false;
                break;
            }
        }
        if (miss_column)
            throw Exception("Version column " + version_column + " does not exist in table declaration.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    };

    if (mode == MergingParams::Collapsing)
        check_sign_column(false, "CollapsingMergeTree");

    if (mode == MergingParams::Summing)
    {
        /// If columns_to_sum are set, then check that such columns exist.
        for (const auto & column_to_sum : columns_to_sum)
        {
            auto check_column_to_sum_exists = [& column_to_sum](const NameAndTypePair & name_and_type)
            {
                return column_to_sum == Nested::extractTableName(name_and_type.name);
            };
            if (columns.end() == std::find_if(columns.begin(), columns.end(), check_column_to_sum_exists))
                throw Exception(
                        "Column " + column_to_sum + " listed in columns to sum does not exist in table declaration.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
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
                throw Exception("Columns: " + boost::algorithm::join(names_intersection, ", ") +
                " listed both in columns to sum and in partition key. That is not allowed.", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    if (mode == MergingParams::Replacing)
        check_version_column(true, "ReplacingMergeTree");

    if (mode == MergingParams::VersionedCollapsing)
    {
        check_sign_column(false, "VersionedCollapsingMergeTree");
        check_version_column(false, "VersionedCollapsingMergeTree");
    }

    /// TODO Checks for Graphite mode.
}


DataTypePtr MergeTreeData::getPartitionValueType() const
{
    DataTypePtr partition_value_type;
    auto partition_types = getInMemoryMetadataPtr()->partition_key.sample_block.getDataTypes();
    if (partition_types.empty())
        partition_value_type = std::make_shared<DataTypeUInt8>();
    else
        partition_value_type = std::make_shared<DataTypeTuple>(std::move(partition_types));
    return partition_value_type;
}


Block MergeTreeData::getSampleBlockWithVirtualColumns() const
{
    DataTypePtr partition_value_type = getPartitionValueType();
    return {
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "_part"),
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "_partition_id"),
        ColumnWithTypeAndName(ColumnUUID::create(), std::make_shared<DataTypeUUID>(), "_part_uuid"),
        ColumnWithTypeAndName(partition_value_type->createColumn(), partition_value_type, "_partition_value")};
}


Block MergeTreeData::getBlockWithVirtualPartColumns(const MergeTreeData::DataPartsVector & parts, bool one_part, bool ignore_empty) const
{
    auto block = getSampleBlockWithVirtualColumns();
    MutableColumns columns = block.mutateColumns();

    auto & part_column = columns[0];
    auto & partition_id_column = columns[1];
    auto & part_uuid_column = columns[2];
    auto & partition_value_column = columns[3];

    bool has_partition_value = typeid_cast<const ColumnTuple *>(partition_value_column.get());
    for (const auto & part_or_projection : parts)
    {
        if (ignore_empty && part_or_projection->isEmpty())
            continue;
        const auto * part = part_or_projection->isProjectionPart() ? part_or_projection->getParentPart() : part_or_projection.get();
        part_column->insert(part->name);
        partition_id_column->insert(part->info.partition_id);
        part_uuid_column->insert(part->uuid);
        Tuple tuple(part->partition.value.begin(), part->partition.value.end());
        if (has_partition_value)
            partition_value_column->insert(tuple);

        if (one_part)
        {
            part_column = ColumnConst::create(std::move(part_column), 1);
            partition_id_column = ColumnConst::create(std::move(partition_id_column), 1);
            part_uuid_column = ColumnConst::create(std::move(part_uuid_column), 1);
            if (has_partition_value)
                partition_value_column = ColumnConst::create(std::move(partition_value_column), 1);
            break;
        }
    }

    block.setColumns(std::move(columns));
    if (!has_partition_value)
        block.erase("_partition_value");
    return block;
}


std::optional<UInt64> MergeTreeData::totalRowsByPartitionPredicateImpl(
    const SelectQueryInfo & query_info, ContextPtr local_context, const DataPartsVector & parts) const
{
    if (parts.empty())
        return 0u;
    auto metadata_snapshot = getInMemoryMetadataPtr();
    ASTPtr expression_ast;
    Block virtual_columns_block = getBlockWithVirtualPartColumns(parts, true /* one_part */);

    // Generate valid expressions for filtering
    bool valid = VirtualColumnUtils::prepareFilterBlockWithQuery(query_info.query, local_context, virtual_columns_block, expression_ast);

    PartitionPruner partition_pruner(metadata_snapshot, query_info, local_context, true /* strict */);
    if (partition_pruner.isUseless() && !valid)
        return {};

    std::unordered_set<String> part_values;
    if (valid && expression_ast)
    {
        virtual_columns_block = getBlockWithVirtualPartColumns(parts, false /* one_part */);
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, virtual_columns_block, local_context, expression_ast);
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

    __builtin_unreachable();
}

Int64 MergeTreeData::getMaxBlockNumber() const
{
    auto lock = lockParts();

    Int64 max_block_num = 0;
    for (const DataPartPtr & part : data_parts_by_info)
        max_block_num = std::max({max_block_num, part->info.max_block, part->info.mutation});

    return max_block_num;
}

void MergeTreeData::loadDataPartsFromDisk(
    DataPartsVector & broken_parts_to_detach,
    DataPartsVector & duplicate_parts_to_remove,
    ThreadPool & pool,
    size_t num_parts,
    std::queue<std::vector<std::pair<String, DiskPtr>>> & parts_queue,
    bool skip_sanity_checks,
    const MergeTreeSettingsPtr & settings)
{
    /// Parallel loading of data parts.
    pool.setMaxThreads(std::min(size_t(settings->max_part_loading_threads), num_parts));
    size_t num_threads = pool.getMaxThreads();
    std::vector<size_t> parts_per_thread(num_threads, num_parts / num_threads);
    for (size_t i = 0ul; i < num_parts % num_threads; ++i)
        ++parts_per_thread[i];

    /// Prepare data parts for parallel loading. Threads will focus on given disk first, then steal
    /// others' tasks when finish current disk part loading process.
    std::vector<std::vector<std::pair<String, DiskPtr>>> threads_parts(num_threads);
    std::set<size_t> remaining_thread_parts;
    std::queue<size_t> threads_queue;
    for (size_t i = 0; i < num_threads; ++i)
    {
        remaining_thread_parts.insert(i);
        threads_queue.push(i);
    }

    while (!parts_queue.empty())
    {
        assert(!threads_queue.empty());
        size_t i = threads_queue.front();
        auto & need_parts = parts_per_thread[i];
        assert(need_parts > 0);
        auto & thread_parts = threads_parts[i];
        auto & current_parts = parts_queue.front();
        assert(!current_parts.empty());
        auto parts_to_grab = std::min(need_parts, current_parts.size());

        thread_parts.insert(thread_parts.end(), current_parts.end() - parts_to_grab, current_parts.end());
        current_parts.resize(current_parts.size() - parts_to_grab);
        need_parts -= parts_to_grab;

        /// Before processing next thread, change disk if possible.
        /// Different threads will likely start loading parts from different disk,
        /// which may improve read parallelism for JBOD.

        /// If current disk still has some parts, push it to the tail.
        if (!current_parts.empty())
            parts_queue.push(std::move(current_parts));
        parts_queue.pop();

        /// If current thread still want some parts, push it to the tail.
        if (need_parts > 0)
            threads_queue.push(i);
        threads_queue.pop();
    }
    assert(threads_queue.empty());
    assert(std::all_of(threads_parts.begin(), threads_parts.end(), [](const std::vector<std::pair<String, DiskPtr>> & parts)
    {
        return !parts.empty();
    }));

    size_t suspicious_broken_parts = 0;
    size_t suspicious_broken_parts_bytes = 0;
    std::atomic<bool> has_adaptive_parts = false;
    std::atomic<bool> has_non_adaptive_parts = false;

    std::mutex mutex;
    auto load_part = [&](const String & part_name, const DiskPtr & part_disk_ptr)
    {
        auto part_opt = MergeTreePartInfo::tryParsePartName(part_name, format_version);
        if (!part_opt)
            return;
        const auto & part_info = *part_opt;
        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, part_disk_ptr, 0);
        auto part = createPart(part_name, part_info, single_disk_volume, part_name);
        bool broken = false;

        String part_path = fs::path(relative_data_path) / part_name;
        String marker_path = fs::path(part_path) / IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME;
        if (part_disk_ptr->exists(marker_path))
        {
            /// NOTE: getBytesOnDisk() cannot be used here, since it maybe zero of checksums.txt will not exist
            size_t size_of_part = IMergeTreeDataPart::calculateTotalSizeOnDisk(part->volume->getDisk(), part->getFullRelativePath());
            LOG_WARNING(log,
                "Detaching stale part {}{} (size: {}), which should have been deleted after a move. "
                "That can only happen after unclean restart of ClickHouse after move of a part having an operation blocking that stale copy of part.",
                getFullPathOnDisk(part_disk_ptr), part_name, formatReadableSizeWithBinarySuffix(size_of_part));
            std::lock_guard loading_lock(mutex);
            broken_parts_to_detach.push_back(part);
            ++suspicious_broken_parts;
            suspicious_broken_parts_bytes += size_of_part;
            return;
        }

        try
        {
            part->loadColumnsChecksumsIndexes(require_part_metadata, true);
        }
        catch (const Exception & e)
        {
            /// Don't count the part as broken if there is not enough memory to load it.
            /// In fact, there can be many similar situations.
            /// But it is OK, because there is a safety guard against deleting too many parts.
            if (isNotEnoughMemoryErrorCode(e.code()))
                throw;

            broken = true;
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        catch (...)
        {
            broken = true;
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        /// Ignore broken parts that can appear as a result of hard server restart.
        if (broken)
        {
            /// NOTE: getBytesOnDisk() cannot be used here, since it maybe zero of checksums.txt will not exist
            size_t size_of_part = IMergeTreeDataPart::calculateTotalSizeOnDisk(part->volume->getDisk(), part->getFullRelativePath());

            LOG_ERROR(log,
                "Detaching broken part {}{} (size: {}). "
                "If it happened after update, it is likely because of backward incompability. "
                "You need to resolve this manually",
                getFullPathOnDisk(part_disk_ptr), part_name, formatReadableSizeWithBinarySuffix(size_of_part));
            std::lock_guard loading_lock(mutex);
            broken_parts_to_detach.push_back(part);
            ++suspicious_broken_parts;
            suspicious_broken_parts_bytes += size_of_part;
            return;
        }
        if (!part->index_granularity_info.is_adaptive)
            has_non_adaptive_parts.store(true, std::memory_order_relaxed);
        else
            has_adaptive_parts.store(true, std::memory_order_relaxed);

        part->modification_time = part_disk_ptr->getLastModified(fs::path(relative_data_path) / part_name).epochTime();
        /// Assume that all parts are Committed, covered parts will be detected and marked as Outdated later
        part->setState(DataPartState::Committed);

        std::lock_guard loading_lock(mutex);
        auto [it, inserted] = data_parts_indexes.insert(part);
        /// Remove duplicate parts with the same checksum.
        if (!inserted)
        {
            if ((*it)->checksums.getTotalChecksumHex() == part->checksums.getTotalChecksumHex())
            {
                LOG_ERROR(log, "Remove duplicate part {}", part->getFullPath());
                duplicate_parts_to_remove.push_back(part);
            }
            else
                throw Exception("Part " + part->name + " already exists but with different checksums", ErrorCodes::DUPLICATE_DATA_PART);
        }

        addPartContributionToDataVolume(part);
    };

    std::mutex part_select_mutex;
    try
    {
        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            pool.scheduleOrThrowOnError([&, thread]
            {
                while (true)
                {
                    std::pair<String, DiskPtr> thread_part;
                    {
                        const std::lock_guard lock{part_select_mutex};

                        if (remaining_thread_parts.empty())
                            return;

                        /// Steal task if nothing to do
                        auto thread_idx = thread;
                        if (threads_parts[thread].empty())
                        {
                            // Try random steal tasks from the next thread
                            std::uniform_int_distribution<size_t> distribution(0, remaining_thread_parts.size() - 1);
                            auto it = remaining_thread_parts.begin();
                            std::advance(it, distribution(thread_local_rng));
                            thread_idx = *it;
                        }
                        auto & thread_parts = threads_parts[thread_idx];
                        thread_part = thread_parts.back();
                        thread_parts.pop_back();
                        if (thread_parts.empty())
                            remaining_thread_parts.erase(thread_idx);
                    }
                    load_part(thread_part.first, thread_part.second);
                }
            });
        }
    }
    catch (...)
    {
        /// If this is not done, then in case of an exception, tasks will be destroyed before the threads are completed, and it will be bad.
        pool.wait();
        throw;
    }

    pool.wait();

    if (has_non_adaptive_parts && has_adaptive_parts && !settings->enable_mixed_granularity_parts)
        throw Exception(
            "Table contains parts with adaptive and non adaptive marks, but `setting enable_mixed_granularity_parts` is disabled",
            ErrorCodes::LOGICAL_ERROR);

    has_non_adaptive_index_granularity_parts = has_non_adaptive_parts;

    if (suspicious_broken_parts > settings->max_suspicious_broken_parts && !skip_sanity_checks)
        throw Exception(ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS,
            "Suspiciously many ({}) broken parts to remove.",
            suspicious_broken_parts);

    if (suspicious_broken_parts_bytes > settings->max_suspicious_broken_parts_bytes && !skip_sanity_checks)
        throw Exception(ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS,
            "Suspiciously big size ({}) of all broken parts to remove.",
            formatReadableSizeWithBinarySuffix(suspicious_broken_parts_bytes));
}


void MergeTreeData::loadDataPartsFromWAL(
    DataPartsVector & /* broken_parts_to_detach */,
    DataPartsVector & duplicate_parts_to_remove,
    MutableDataPartsVector & parts_from_wal,
    DataPartsLock & part_lock)
{
    for (auto & part : parts_from_wal)
    {
        if (getActiveContainingPart(part->info, DataPartState::Committed, part_lock))
            continue;

        part->modification_time = time(nullptr);
        /// Assume that all parts are Committed, covered parts will be detected and marked as Outdated later
        part->setState(DataPartState::Committed);

        auto [it, inserted] = data_parts_indexes.insert(part);
        if (!inserted)
        {
            if ((*it)->checksums.getTotalChecksumHex() == part->checksums.getTotalChecksumHex())
            {
                LOG_ERROR(log, "Remove duplicate part {}", part->getFullPath());
                duplicate_parts_to_remove.push_back(part);
            }
            else
                throw Exception("Part " + part->name + " already exists but with different checksums", ErrorCodes::DUPLICATE_DATA_PART);
        }

        addPartContributionToDataVolume(part);
    }
}


void MergeTreeData::loadDataParts(bool skip_sanity_checks)
{
    LOG_DEBUG(log, "Loading data parts");

    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto settings = getSettings();
    MutableDataPartsVector parts_from_wal;
    Strings part_file_names;

    auto disks = getStoragePolicy()->getDisks();

    /// Only check if user did touch storage configuration for this table.
    if (!getStoragePolicy()->isDefaultPolicy() && !skip_sanity_checks)
    {
        /// Check extra parts at different disks, in order to not allow to miss data parts at undefined disks.
        std::unordered_set<String> defined_disk_names;

        for (const auto & disk_ptr : disks)
            defined_disk_names.insert(disk_ptr->getName());

        for (const auto & [disk_name, disk] : getContext()->getDisksMap())
        {
            if (defined_disk_names.count(disk_name) == 0 && disk->exists(relative_data_path))
            {
                for (const auto it = disk->iterateDirectory(relative_data_path); it->isValid(); it->next())
                {
                    if (MergeTreePartInfo::tryParsePartName(it->name(), format_version))
                        throw Exception(ErrorCodes::UNKNOWN_DISK,
                            "Part {} was found on disk {} which is not defined in the storage policy",
                            backQuote(it->name()), backQuote(disk_name));
                }
            }
        }
    }

    /// Collect part names by disk.
    std::map<String, std::vector<std::pair<String, DiskPtr>>> disk_part_map;
    std::map<String, MutableDataPartsVector> disk_wal_part_map;
    ThreadPool pool(disks.size());
    std::mutex wal_init_lock;
    for (const auto & disk_ptr : disks)
    {
        auto & disk_parts = disk_part_map[disk_ptr->getName()];
        auto & disk_wal_parts = disk_wal_part_map[disk_ptr->getName()];

        pool.scheduleOrThrowOnError([&, disk_ptr]()
        {
            for (auto it = disk_ptr->iterateDirectory(relative_data_path); it->isValid(); it->next())
            {
                /// Skip temporary directories, file 'format_version.txt' and directory 'detached'.
                if (startsWith(it->name(), "tmp") || it->name() == MergeTreeData::FORMAT_VERSION_FILE_NAME
                    || it->name() == MergeTreeData::DETACHED_DIR_NAME)
                    continue;

                if (!startsWith(it->name(), MergeTreeWriteAheadLog::WAL_FILE_NAME))
                    disk_parts.emplace_back(std::make_pair(it->name(), disk_ptr));
                else if (it->name() == MergeTreeWriteAheadLog::DEFAULT_WAL_FILE_NAME && settings->in_memory_parts_enable_wal)
                {
                    std::unique_lock lock(wal_init_lock);
                    if (write_ahead_log != nullptr)
                        throw Exception(
                            "There are multiple WAL files appeared in current storage policy. You need to resolve this manually",
                            ErrorCodes::CORRUPTED_DATA);

                    write_ahead_log = std::make_shared<MergeTreeWriteAheadLog>(*this, disk_ptr, it->name());
                    for (auto && part : write_ahead_log->restore(metadata_snapshot, getContext()))
                        disk_wal_parts.push_back(std::move(part));
                }
                else if (settings->in_memory_parts_enable_wal)
                {
                    MergeTreeWriteAheadLog wal(*this, disk_ptr, it->name());
                    for (auto && part : wal.restore(metadata_snapshot, getContext()))
                        disk_wal_parts.push_back(std::move(part));
                }
            }
        });
    }

    pool.wait();

    for (auto & [_, disk_wal_parts] : disk_wal_part_map)
        parts_from_wal.insert(
            parts_from_wal.end(), std::make_move_iterator(disk_wal_parts.begin()), std::make_move_iterator(disk_wal_parts.end()));

    size_t num_parts = 0;
    std::queue<std::vector<std::pair<String, DiskPtr>>> parts_queue;
    for (auto & [_, disk_parts] : disk_part_map)
    {
        if (disk_parts.empty())
            continue;
        num_parts += disk_parts.size();
        parts_queue.push(std::move(disk_parts));
    }

    auto part_lock = lockParts();
    data_parts_indexes.clear();

    if (num_parts == 0 && parts_from_wal.empty())
    {
        LOG_DEBUG(log, "There are no data parts");
        return;
    }


    DataPartsVector broken_parts_to_detach;
    DataPartsVector duplicate_parts_to_remove;

    if (num_parts > 0)
        loadDataPartsFromDisk(
            broken_parts_to_detach, duplicate_parts_to_remove, pool, num_parts, parts_queue, skip_sanity_checks, settings);

    if (!parts_from_wal.empty())
        loadDataPartsFromWAL(broken_parts_to_detach, duplicate_parts_to_remove, parts_from_wal, part_lock);

    for (auto & part : broken_parts_to_detach)
        part->renameToDetached("broken-on-start"); /// detached parts must not have '_' in prefixes

    for (auto & part : duplicate_parts_to_remove)
        part->remove();

    /// Delete from the set of current parts those parts that are covered by another part (those parts that
    /// were merged), but that for some reason are still not deleted from the filesystem.
    /// Deletion of files will be performed later in the clearOldParts() method.

    if (data_parts_indexes.size() >= 2)
    {
        /// Now all parts are committed, so data_parts_by_state_and_info == committed_parts_range
        auto prev_jt = data_parts_by_state_and_info.begin();
        auto curr_jt = std::next(prev_jt);

        auto deactivate_part = [&] (DataPartIteratorByStateAndInfo it)
        {
            (*it)->remove_time.store((*it)->modification_time, std::memory_order_relaxed);
            modifyPartState(it, DataPartState::Outdated);
            removePartContributionToDataVolume(*it);
        };

        (*prev_jt)->assertState({DataPartState::Committed});

        while (curr_jt != data_parts_by_state_and_info.end() && (*curr_jt)->getState() == DataPartState::Committed)
        {
            /// Don't consider data parts belonging to different partitions.
            if ((*curr_jt)->info.partition_id != (*prev_jt)->info.partition_id)
            {
                ++prev_jt;
                ++curr_jt;
                continue;
            }

            if ((*curr_jt)->contains(**prev_jt))
            {
                deactivate_part(prev_jt);
                prev_jt = curr_jt;
                ++curr_jt;
            }
            else if ((*prev_jt)->contains(**curr_jt))
            {
                auto next = std::next(curr_jt);
                deactivate_part(curr_jt);
                curr_jt = next;
            }
            else
            {
                ++prev_jt;
                ++curr_jt;
            }
        }
    }

    calculateColumnAndSecondaryIndexSizesImpl();


    LOG_DEBUG(log, "Loaded data parts ({} items)", data_parts_indexes.size());
}


/// Is the part directory old.
/// True if its modification time and the modification time of all files inside it is less then threshold.
/// (Only files on the first level of nesting are considered).
static bool isOldPartDirectory(const DiskPtr & disk, const String & directory_path, time_t threshold)
{
    if (disk->getLastModified(directory_path).epochTime() >= threshold)
        return false;

    for (auto it = disk->iterateDirectory(directory_path); it->isValid(); it->next())
        if (disk->getLastModified(it->path()).epochTime() >= threshold)
            return false;

    return true;
}


void MergeTreeData::clearOldTemporaryDirectories(const MergeTreeDataMergerMutator & merger_mutator, size_t custom_directories_lifetime_seconds)
{
    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock lock(clear_old_temporary_directories_mutex, std::defer_lock);
    if (!lock.try_lock())
        return;

    const auto settings = getSettings();
    time_t current_time = time(nullptr);
    ssize_t deadline = current_time - custom_directories_lifetime_seconds;

    /// Delete temporary directories older than a day.
    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
    {
        for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
        {
            const std::string & basename = it->name();
            if (!startsWith(basename, "tmp_"))
            {
                continue;
            }
            const std::string & full_path = fullPath(disk, it->path());
            if (merger_mutator.hasTemporaryPart(basename))
            {
                LOG_WARNING(log, "{} is an active destination for one of merge/mutation (consider increasing temporary_directories_lifetime setting)", full_path);
                continue;
            }

            try
            {
                if (disk->isDirectory(it->path()) && isOldPartDirectory(disk, it->path(), deadline))
                {
                    LOG_WARNING(log, "Removing temporary directory {}", full_path);
                    disk->removeRecursive(it->path());
                }
            }
            /// see getModificationTime()
            catch (const ErrnoException & e)
            {
                if (e.getErrno() == ENOENT)
                {
                    /// If the file is already deleted, do nothing.
                }
                else
                    throw;
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
}


MergeTreeData::DataPartsVector MergeTreeData::grabOldParts(bool force)
{
    DataPartsVector res;

    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock lock(grab_old_parts_mutex, std::defer_lock);
    if (!lock.try_lock())
        return res;

    time_t now = time(nullptr);
    std::vector<DataPartIteratorByStateAndInfo> parts_to_delete;

    {
        auto parts_lock = lockParts();

        auto outdated_parts_range = getDataPartsStateRange(DataPartState::Outdated);
        for (auto it = outdated_parts_range.begin(); it != outdated_parts_range.end(); ++it)
        {
            const DataPartPtr & part = *it;

            auto part_remove_time = part->remove_time.load(std::memory_order_relaxed);

            if (part.unique() && /// Grab only parts that are not used by anyone (SELECTs for example).
                ((part_remove_time < now &&
                now - part_remove_time > getSettings()->old_parts_lifetime.totalSeconds()) || force
                || isInMemoryPart(part))) /// Remove in-memory parts immediately to not store excessive data in RAM
            {
                parts_to_delete.emplace_back(it);
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
        LOG_TRACE(log, "Found {} old parts to remove.", res.size());

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
                throw Exception("Deleting data part " + part->name + " doesn't exist", ErrorCodes::LOGICAL_ERROR);

            (*it)->assertState({DataPartState::Deleting});

            data_parts_indexes.erase(it);
        }
    }

    /// Data parts is still alive (since DataPartsVector holds shared_ptrs) and contain useful metainformation for logging
    /// NOTE: There is no need to log parts deletion somewhere else, all deleting parts pass through this function and pass away

    auto table_id = getStorageID();
    if (auto part_log = getContext()->getPartLog(table_id.database_name))
    {
        PartLogElement part_log_elem;

        part_log_elem.event_type = PartLogElement::REMOVE_PART;

        const auto time_now = std::chrono::system_clock::now();
        part_log_elem.event_time = time_in_seconds(time_now);
        part_log_elem.event_time_microseconds = time_in_microseconds(time_now);

        part_log_elem.duration_ms = 0; //-V1048

        part_log_elem.database_name = table_id.database_name;
        part_log_elem.table_name = table_id.table_name;

        for (const auto & part : parts)
        {
            part_log_elem.partition_id = part->info.partition_id;
            part_log_elem.part_name = part->name;
            part_log_elem.bytes_compressed_on_disk = part->getBytesOnDisk();
            part_log_elem.rows = part->rows_count;

            part_log->add(part_log_elem);
        }
    }
}

void MergeTreeData::clearOldPartsFromFilesystem(bool force)
{
    DataPartsVector parts_to_remove = grabOldParts(force);
    clearPartsFromFilesystem(parts_to_remove);
    removePartsFinally(parts_to_remove);

    /// This is needed to close files to avoid they reside on disk after being deleted.
    /// NOTE: we can drop files from cache more selectively but this is good enough.
    if (!parts_to_remove.empty())
        getContext()->dropMMappedFileCache();
}

void MergeTreeData::clearPartsFromFilesystem(const DataPartsVector & parts_to_remove)
{
    const auto settings = getSettings();
    if (parts_to_remove.size() > 1 && settings->max_part_removal_threads > 1 && parts_to_remove.size() > settings->concurrent_part_removal_threshold)
    {
        /// Parallel parts removal.

        size_t num_threads = std::min<size_t>(settings->max_part_removal_threads, parts_to_remove.size());
        ThreadPool pool(num_threads);

        /// NOTE: Under heavy system load you may get "Cannot schedule a task" from ThreadPool.
        for (const DataPartPtr & part : parts_to_remove)
        {
            pool.scheduleOrThrowOnError([&, thread_group = CurrentThread::getGroup()]
            {
                SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
                );
                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                LOG_DEBUG(log, "Removing part from filesystem {}", part->name);
                part->remove();
            });
        }

        pool.wait();
    }
    else
    {
        for (const DataPartPtr & part : parts_to_remove)
        {
            LOG_DEBUG(log, "Removing part from filesystem {}", part->name);
            part->remove();
        }
    }
}

void MergeTreeData::clearOldWriteAheadLogs()
{
    DataPartsVector parts = getDataPartsVector();
    std::vector<std::pair<Int64, Int64>> all_block_numbers_on_disk;
    std::vector<std::pair<Int64, Int64>> block_numbers_on_disk;

    for (const auto & part : parts)
        if (part->isStoredOnDisk())
            all_block_numbers_on_disk.emplace_back(part->info.min_block, part->info.max_block);

    if (all_block_numbers_on_disk.empty())
        return;

    std::sort(all_block_numbers_on_disk.begin(), all_block_numbers_on_disk.end());
    block_numbers_on_disk.push_back(all_block_numbers_on_disk[0]);
    for (size_t i = 1; i < all_block_numbers_on_disk.size(); ++i)
    {
        if (all_block_numbers_on_disk[i].first == all_block_numbers_on_disk[i - 1].second + 1)
            block_numbers_on_disk.back().second = all_block_numbers_on_disk[i].second;
        else
            block_numbers_on_disk.push_back(all_block_numbers_on_disk[i]);
    }

    auto is_range_on_disk = [&block_numbers_on_disk](Int64 min_block, Int64 max_block)
    {
        auto lower = std::lower_bound(block_numbers_on_disk.begin(), block_numbers_on_disk.end(), std::make_pair(min_block, Int64(-1L)));
        if (lower != block_numbers_on_disk.end() && min_block >= lower->first && max_block <= lower->second)
            return true;

        if (lower != block_numbers_on_disk.begin())
        {
            --lower;
            if (min_block >= lower->first && max_block <= lower->second)
                return true;
        }

        return false;
    };

    auto disks = getStoragePolicy()->getDisks();
    for (auto disk_it = disks.rbegin(); disk_it != disks.rend(); ++disk_it)
    {
        auto disk_ptr = *disk_it;
        for (auto it = disk_ptr->iterateDirectory(relative_data_path); it->isValid(); it->next())
        {
            auto min_max_block_number = MergeTreeWriteAheadLog::tryParseMinMaxBlockNumber(it->name());
            if (min_max_block_number && is_range_on_disk(min_max_block_number->first, min_max_block_number->second))
            {
                LOG_DEBUG(log, "Removing from filesystem the outdated WAL file " + it->name());
                disk_ptr->removeFile(relative_data_path + it->name());
            }
        }
    }
}

void MergeTreeData::clearEmptyParts()
{
    if (!getSettings()->remove_empty_parts)
        return;

    auto parts = getDataPartsVector();
    for (const auto & part : parts)
    {
        if (part->rows_count == 0)
            dropPartNoWaitNoThrow(part->name);
    }
}

void MergeTreeData::rename(const String & new_table_path, const StorageID & new_table_id)
{
    auto disks = getStoragePolicy()->getDisks();

    for (const auto & disk : disks)
    {
        if (disk->exists(new_table_path))
            throw Exception{"Target path already exists: " + fullPath(disk, new_table_path), ErrorCodes::DIRECTORY_ALREADY_EXISTS};
    }

    for (const auto & disk : disks)
    {
        auto new_table_path_parent = parentPath(new_table_path);
        disk->createDirectories(new_table_path_parent);
        disk->moveDirectory(relative_data_path, new_table_path);
    }

    if (!getStorageID().hasUUID())
        getContext()->dropCaches();

    relative_data_path = new_table_path;
    renameInMemory(new_table_id);
}

void MergeTreeData::dropAllData()
{
    LOG_TRACE(log, "dropAllData: waiting for locks.");

    auto lock = lockParts();

    LOG_TRACE(log, "dropAllData: removing data from memory.");

    DataPartsVector all_parts(data_parts_by_info.begin(), data_parts_by_info.end());

    data_parts_indexes.clear();
    column_sizes.clear();

    /// Tables in atomic databases have UUID and stored in persistent locations.
    /// No need to drop caches (that are keyed by filesystem path) because collision is not possible.
    if (!getStorageID().hasUUID())
        getContext()->dropCaches();

    LOG_TRACE(log, "dropAllData: removing data from filesystem.");

    /// Removing of each data part before recursive removal of directory is to speed-up removal, because there will be less number of syscalls.
    clearPartsFromFilesystem(all_parts);

    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
    {
        try
        {
            disk->removeRecursive(path);
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
    LOG_TRACE(log, "dropIfEmpty");

    auto lock = lockParts();

    if (!data_parts_by_info.empty())
        return;

    try
    {
        for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
        {
            /// Non recursive, exception is thrown if there are more files.
            disk->removeFileIfExists(fs::path(path) / MergeTreeData::FORMAT_VERSION_FILE_NAME);
            disk->removeDirectory(fs::path(path) / MergeTreeData::DETACHED_DIR_NAME);
            disk->removeDirectory(path);
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
        throw Exception("Cannot alter version column " + backQuoteIfNeed(column_name) +
            " to type " + new_type->getName() +
            " because version column must be of an integer type or of type Date or DateTime"
            , ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);

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
        throw Exception("Cannot alter version column " + backQuoteIfNeed(column_name) +
            " from type " + old_type->getName() +
            " to type " + new_type->getName() + " because new type will change sort order of version column." +
            " The only possible conversion is expansion of the number of bytes of the current type."
            , ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
    }

    /// Check alter to smaller size: UInt64 -> UInt32 and so on
    if (new_type->getSizeOfValueInMemory() < old_type->getSizeOfValueInMemory())
    {
        throw Exception("Cannot alter version column " + backQuoteIfNeed(column_name) +
            " from type " + old_type->getName() +
            " to type " + new_type->getName() + " because new type is smaller than current in the number of bytes." +
            " The only possible conversion is expansion of the number of bytes of the current type."
            , ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
    }
}

}

void MergeTreeData::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    /// Check that needed transformations can be applied to the list of columns without considering type conversions.
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    const auto & settings = local_context->getSettingsRef();

    if (!settings.allow_non_metadata_alters)
    {

        auto mutation_commands = commands.getMutationCommands(new_metadata, settings.materialize_ttl_after_modify, getContext());

        if (!mutation_commands.empty())
            throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "The following alter commands: '{}' will modify data on disk, but setting `allow_non_metadata_alters` is disabled", queryToString(mutation_commands.ast()));
    }
    commands.apply(new_metadata, getContext());

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

    for (const auto & index : old_metadata.getSecondaryIndices())
    {
        for (const String & col : index.expression->getRequiredColumns())
            columns_alter_type_forbidden.insert(col);
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

    NameSet dropped_columns;

    std::map<String, const IDataType *> old_types;
    for (const auto & column : old_metadata.getColumns().getAllPhysical())
        old_types.emplace(column.name, column.type.get());

    NamesAndTypesList columns_to_check_conversion;
    auto name_deps = getDependentViewsByColumn(local_context);
    for (const AlterCommand & command : commands)
    {
        /// Just validate partition expression
        if (command.partition)
        {
            getPartitionIDFromQuery(command.partition, getContext());
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
            else if (command.type == AlterCommand::DROP_COLUMN)
            {
                throw Exception(
                    "Trying to ALTER DROP version " + backQuoteIfNeed(command.column_name) + " column",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }
            else if (command.type == AlterCommand::RENAME_COLUMN)
            {
                throw Exception(
                    "Trying to ALTER RENAME version " + backQuoteIfNeed(command.column_name) + " column",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }
        }

        if (command.type == AlterCommand::MODIFY_ORDER_BY && !is_custom_partitioned)
        {
            throw Exception(
                "ALTER MODIFY ORDER BY is not supported for default-partitioned tables created with the old syntax",
                ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::MODIFY_TTL && !is_custom_partitioned)
        {
            throw Exception(
                "ALTER MODIFY TTL is not supported for default-partitioned tables created with the old syntax",
                ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::MODIFY_SAMPLE_BY)
        {
            if (!is_custom_partitioned)
                throw Exception(
                    "ALTER MODIFY SAMPLE BY is not supported for default-partitioned tables created with the old syntax",
                    ErrorCodes::BAD_ARGUMENTS);

            checkSampleExpression(new_metadata, getSettings()->compatibility_allow_sampling_expression_not_in_primary_key,
                                  getSettings()->check_sample_column_is_correct);
        }
        if (command.type == AlterCommand::ADD_INDEX && !is_custom_partitioned)
        {
            throw Exception(
                "ALTER ADD INDEX is not supported for tables with the old syntax",
                ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::ADD_PROJECTION && !is_custom_partitioned)
        {
            throw Exception(
                "ALTER ADD PROJECTION is not supported for tables with the old syntax",
                ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::RENAME_COLUMN)
        {
            if (columns_in_keys.count(command.column_name))
            {
                throw Exception(
                    "Trying to ALTER RENAME key " + backQuoteIfNeed(command.column_name) + " column which is a part of key expression",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            if (columns_in_keys.count(command.column_name))
            {
                throw Exception(
                    "Trying to ALTER DROP key " + backQuoteIfNeed(command.column_name) + " column which is a part of key expression",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }

            if (!command.clear)
            {
                const auto & deps_mv = name_deps[command.column_name];
                if (!deps_mv.empty())
                {
                    throw Exception(
                        "Trying to ALTER DROP column " + backQuoteIfNeed(command.column_name) + " which is referenced by materialized view "
                            + toString(deps_mv),
                        ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
                }
            }

            dropped_columns.emplace(command.column_name);
        }
        else if (command.isRequireMutationStage(getInMemoryMetadata()))
        {
            /// This alter will override data on disk. Let's check that it doesn't
            /// modify immutable column.
            if (columns_alter_type_forbidden.count(command.column_name))
                throw Exception("ALTER of key column " + backQuoteIfNeed(command.column_name) + " is forbidden",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);

            if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                if (columns_alter_type_check_safe_for_partition.count(command.column_name))
                {
                    auto it = old_types.find(command.column_name);

                    assert(it != old_types.end());
                    if (!isSafeForKeyConversion(it->second, command.data_type.get()))
                        throw Exception("ALTER of partition key column " + backQuoteIfNeed(command.column_name) + " from type "
                                + it->second->getName() + " to type " + command.data_type->getName()
                                + " is not safe because it can change the representation of partition key",
                            ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
                }

                if (columns_alter_type_metadata_only.count(command.column_name))
                {
                    auto it = old_types.find(command.column_name);
                    assert(it != old_types.end());
                    if (!isSafeForKeyConversion(it->second, command.data_type.get()))
                        throw Exception("ALTER of key column " + backQuoteIfNeed(command.column_name) + " from type "
                                    + it->second->getName() + " to type " + command.data_type->getName()
                                    + " is not safe because it can change the representation of primary key",
                            ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
                }

                if (old_metadata.getColumns().has(command.column_name))
                {
                    columns_to_check_conversion.push_back(
                        new_metadata.getColumns().getPhysical(command.column_name));
                }
            }
        }
    }

    checkProperties(new_metadata, old_metadata);
    checkTTLExpressions(new_metadata, old_metadata);

    if (!columns_to_check_conversion.empty())
    {
        auto old_header = old_metadata.getSampleBlock();
        performRequiredConversions(old_header, columns_to_check_conversion, getContext());
    }

    if (old_metadata.hasSettingsChanges())
    {
        const auto current_changes = old_metadata.getSettingsChanges()->as<const ASTSetQuery &>().changes;
        const auto & new_changes = new_metadata.settings_changes->as<const ASTSetQuery &>().changes;
        for (const auto & changed_setting : new_changes)
        {
            const auto & setting_name = changed_setting.name;
            const auto & new_value = changed_setting.value;
            MergeTreeSettings::checkCanSet(setting_name, new_value);
            const Field * current_value = current_changes.tryGet(setting_name);

            if ((!current_value || *current_value != new_value)
                && MergeTreeSettings::isReadonlySetting(setting_name))
            {
                throw Exception{"Setting '" + setting_name + "' is readonly for storage '" + getName() + "'",
                                 ErrorCodes::READONLY_SETTING};
            }

            if (!current_value && MergeTreeSettings::isPartFormatSetting(setting_name))
            {
                MergeTreeSettings copy = *getSettings();
                copy.applyChange(changed_setting);
                String reason;
                if (!canUsePolymorphicParts(copy, &reason) && !reason.empty())
                    throw Exception("Can't change settings. Reason: " + reason, ErrorCodes::NOT_IMPLEMENTED);
            }

            if (setting_name == "storage_policy")
                checkStoragePolicy(getContext()->getStoragePolicy(new_value.safeGet<String>()));
        }

        /// Check if it is safe to reset the settings
        for (const auto & current_setting : current_changes)
        {
            const auto & setting_name = current_setting.name;
            const Field * new_value = new_changes.tryGet(setting_name);
            /// Prevent unsetting readonly setting
            if (MergeTreeSettings::isReadonlySetting(setting_name) && !new_value)
            {
                throw Exception{"Setting '" + setting_name + "' is readonly for storage '" + getName() + "'",
                                ErrorCodes::READONLY_SETTING};
            }

            if (MergeTreeSettings::isPartFormatSetting(setting_name) && !new_value)
            {
                /// Use default settings + new and check if doesn't affect part format settings
                auto copy = getDefaultSettings();
                copy->applyChanges(new_changes);
                String reason;
                if (!canUsePolymorphicParts(*copy, &reason) && !reason.empty())
                    throw Exception("Can't change settings. Reason: " + reason, ErrorCodes::NOT_IMPLEMENTED);
            }

        }
    }

    for (const auto & part : getDataPartsVector())
    {
        bool at_least_one_column_rest = false;
        for (const auto & column : part->getColumns())
        {
            if (!dropped_columns.count(column.name))
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
                "Cannot drop or clear column{} '{}', because all columns in part '{}' will be removed from disk. Empty parts are not allowed", postfix, boost::algorithm::join(dropped_columns, ", "), part->name);
        }
    }
}


void MergeTreeData::checkMutationIsPossible(const MutationCommands & /*commands*/, const Settings & /*settings*/) const
{
    /// Some validation will be added
}

MergeTreeDataPartType MergeTreeData::choosePartType(size_t bytes_uncompressed, size_t rows_count) const
{
    const auto settings = getSettings();
    if (!canUsePolymorphicParts(*settings))
        return MergeTreeDataPartType::WIDE;

    if (bytes_uncompressed < settings->min_bytes_for_compact_part || rows_count < settings->min_rows_for_compact_part)
        return MergeTreeDataPartType::IN_MEMORY;

    if (bytes_uncompressed < settings->min_bytes_for_wide_part || rows_count < settings->min_rows_for_wide_part)
        return MergeTreeDataPartType::COMPACT;

    return MergeTreeDataPartType::WIDE;
}

MergeTreeDataPartType MergeTreeData::choosePartTypeOnDisk(size_t bytes_uncompressed, size_t rows_count) const
{
    const auto settings = getSettings();
    if (!canUsePolymorphicParts(*settings))
        return MergeTreeDataPartType::WIDE;

    if (bytes_uncompressed < settings->min_bytes_for_wide_part || rows_count < settings->min_rows_for_wide_part)
        return MergeTreeDataPartType::COMPACT;

    return MergeTreeDataPartType::WIDE;
}


MergeTreeData::MutableDataPartPtr MergeTreeData::createPart(const String & name,
    MergeTreeDataPartType type, const MergeTreePartInfo & part_info,
    const VolumePtr & volume, const String & relative_path, const IMergeTreeDataPart * parent_part) const
{
    if (type == MergeTreeDataPartType::COMPACT)
        return std::make_shared<MergeTreeDataPartCompact>(*this, name, part_info, volume, relative_path, parent_part);
    else if (type == MergeTreeDataPartType::WIDE)
        return std::make_shared<MergeTreeDataPartWide>(*this, name, part_info, volume, relative_path, parent_part);
    else if (type == MergeTreeDataPartType::IN_MEMORY)
        return std::make_shared<MergeTreeDataPartInMemory>(*this, name, part_info, volume, relative_path, parent_part);
    else
        throw Exception("Unknown type of part " + relative_path, ErrorCodes::UNKNOWN_PART_TYPE);
}

static MergeTreeDataPartType getPartTypeFromMarkExtension(const String & mrk_ext)
{
    if (mrk_ext == getNonAdaptiveMrkExtension())
        return MergeTreeDataPartType::WIDE;
    if (mrk_ext == getAdaptiveMrkExtension(MergeTreeDataPartType::WIDE))
        return MergeTreeDataPartType::WIDE;
    if (mrk_ext == getAdaptiveMrkExtension(MergeTreeDataPartType::COMPACT))
        return MergeTreeDataPartType::COMPACT;

    throw Exception("Can't determine part type, because of unknown mark extension " + mrk_ext, ErrorCodes::UNKNOWN_PART_TYPE);
}

MergeTreeData::MutableDataPartPtr MergeTreeData::createPart(
    const String & name, const VolumePtr & volume, const String & relative_path, const IMergeTreeDataPart * parent_part) const
{
    return createPart(name, MergeTreePartInfo::fromPartName(name, format_version), volume, relative_path, parent_part);
}

MergeTreeData::MutableDataPartPtr MergeTreeData::createPart(
    const String & name, const MergeTreePartInfo & part_info,
    const VolumePtr & volume, const String & relative_path, const IMergeTreeDataPart * parent_part) const
{
    MergeTreeDataPartType type;
    auto full_path = fs::path(relative_data_path) / (parent_part ? parent_part->relative_path : "") / relative_path / "";
    auto mrk_ext = MergeTreeIndexGranularityInfo::getMarksExtensionFromFilesystem(volume->getDisk(), full_path);

    if (mrk_ext)
        type = getPartTypeFromMarkExtension(*mrk_ext);
    else
    {
        /// Didn't find any mark file, suppose that part is empty.
        type = choosePartTypeOnDisk(0, 0);
    }

    return createPart(name, type, part_info, volume, relative_path, parent_part);
}

void MergeTreeData::changeSettings(
        const ASTPtr & new_settings,
        AlterLockHolder & /* table_lock_holder */)
{
    if (new_settings)
    {
        bool has_storage_policy_changed = false;

        const auto & new_changes = new_settings->as<const ASTSetQuery &>().changes;

        for (const auto & change : new_changes)
        {
            if (change.name == "storage_policy")
            {
                StoragePolicyPtr new_storage_policy = getContext()->getStoragePolicy(change.value.safeGet<String>());
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
                        if (disk->exists(relative_data_path))
                            throw Exception("New storage policy contain disks which already contain data of a table with the same name", ErrorCodes::LOGICAL_ERROR);
                    }

                    for (const String & disk_name : all_diff_disk_names)
                    {
                        auto disk = new_storage_policy->getDiskByName(disk_name);
                        disk->createDirectories(relative_data_path);
                        disk->createDirectories(fs::path(relative_data_path) / MergeTreeData::DETACHED_DIR_NAME);
                    }
                    /// FIXME how would that be done while reloading configuration???

                    has_storage_policy_changed = true;
                }
            }
        }

        /// Reset to default settings before applying existing.
        auto copy = getDefaultSettings();
        copy->applyChanges(new_changes);
        copy->sanityCheck(getContext()->getSettingsRef());

        storage_settings.set(std::move(copy));
        StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
        new_metadata.setSettingsChanges(new_settings);
        setInMemoryMetadata(new_metadata);

        if (has_storage_policy_changed)
            startBackgroundMovesIfNeeded();
    }
}

void MergeTreeData::PartsTemporaryRename::addPart(const String & old_name, const String & new_name)
{
    old_and_new_names.push_back({old_name, new_name});
    for (const auto & [path, disk] : storage.getRelativeDataPathsWithDisks())
    {
        for (auto it = disk->iterateDirectory(fs::path(path) / source_dir); it->isValid(); it->next())
        {
            if (it->name() == old_name)
            {
                old_part_name_to_path_and_disk[old_name] = {path, disk};
                break;
            }
        }
    }
}

void MergeTreeData::PartsTemporaryRename::tryRenameAll()
{
    renamed = true;
    for (size_t i = 0; i < old_and_new_names.size(); ++i)
    {
        try
        {
            const auto & [old_name, new_name] = old_and_new_names[i];
            if (old_name.empty() || new_name.empty())
                throw DB::Exception("Empty part name. Most likely it's a bug.", ErrorCodes::INCORRECT_FILE_NAME);
            const auto & [path, disk] = old_part_name_to_path_and_disk[old_name];
            const auto full_path = fs::path(path) / source_dir; /// for old_name
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
    for (const auto & [old_name, new_name] : old_and_new_names)
    {
        if (old_name.empty())
            continue;

        try
        {
            const auto & [path, disk] = old_part_name_to_path_and_disk[old_name];
            const String full_path = fs::path(path) / source_dir; /// for old_name
            disk->moveFile(fs::path(full_path) / new_name, fs::path(full_path) / old_name);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


MergeTreeData::DataPartsVector MergeTreeData::getActivePartsToReplace(
    const MergeTreePartInfo & new_part_info,
    const String & new_part_name,
    DataPartPtr & out_covering_part,
    DataPartsLock & /* data_parts_lock */) const
{
    /// Parts contained in the part are consecutive in data_parts, intersecting the insertion place for the part itself.
    auto it_middle = data_parts_by_state_and_info.lower_bound(DataPartStateAndInfo{DataPartState::Committed, new_part_info});
    auto committed_parts_range = getDataPartsStateRange(DataPartState::Committed);

    /// Go to the left.
    DataPartIteratorByStateAndInfo begin = it_middle;
    while (begin != committed_parts_range.begin())
    {
        auto prev = std::prev(begin);

        if (!new_part_info.contains((*prev)->info))
        {
            if ((*prev)->info.contains(new_part_info))
            {
                out_covering_part = *prev;
                return {};
            }

            if (!new_part_info.isDisjoint((*prev)->info))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects previous part {}. It is a bug.",
                                new_part_name, (*prev)->getNameWithState());

            break;
        }

        begin = prev;
    }

    /// Go to the right.
    DataPartIteratorByStateAndInfo end = it_middle;
    while (end != committed_parts_range.end())
    {
        if ((*end)->info == new_part_info)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected duplicate part {}. It is a bug.", (*end)->getNameWithState());

        if (!new_part_info.contains((*end)->info))
        {
            if ((*end)->info.contains(new_part_info))
            {
                out_covering_part = *end;
                return {};
            }

            if (!new_part_info.isDisjoint((*end)->info))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects next part {}. It is a bug.",
                                new_part_name, (*end)->getNameWithState());

            break;
        }

        ++end;
    }

    return DataPartsVector{begin, end};
}


bool MergeTreeData::renameTempPartAndAdd(MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction, MergeTreeDeduplicationLog * deduplication_log)
{
    if (out_transaction && &out_transaction->data != this)
        throw Exception("MergeTreeData::Transaction for one table cannot be used with another. It is a bug.",
            ErrorCodes::LOGICAL_ERROR);

    DataPartsVector covered_parts;
    {
        auto lock = lockParts();
        if (!renameTempPartAndReplace(part, increment, out_transaction, lock, &covered_parts, deduplication_log))
            return false;
    }
    if (!covered_parts.empty())
        throw Exception("Added part " + part->name + " covers " + toString(covered_parts.size())
            + " existing part(s) (including " + covered_parts[0]->name + ")", ErrorCodes::LOGICAL_ERROR);

    return true;
}


bool MergeTreeData::renameTempPartAndReplace(
    MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction,
    std::unique_lock<std::mutex> & lock, DataPartsVector * out_covered_parts, MergeTreeDeduplicationLog * deduplication_log)
{
    if (out_transaction && &out_transaction->data != this)
        throw Exception("MergeTreeData::Transaction for one table cannot be used with another. It is a bug.",
            ErrorCodes::LOGICAL_ERROR);

    part->assertState({DataPartState::Temporary});

    MergeTreePartInfo part_info = part->info;
    String part_name;

    if (DataPartPtr existing_part_in_partition = getAnyPartInPartition(part->info.partition_id, lock))
    {
        if (part->partition.value != existing_part_in_partition->partition.value)
            throw Exception(
                "Partition value mismatch between two parts with the same partition ID. Existing part: "
                + existing_part_in_partition->name + ", newly added part: " + part->name,
                ErrorCodes::CORRUPTED_DATA);
    }

    /** It is important that obtaining new block number and adding that block to parts set is done atomically.
      * Otherwise there is race condition - merge of blocks could happen in interval that doesn't yet contain new part.
      */
    if (increment)
    {
        part_info.min_block = part_info.max_block = increment->get();
        part_info.mutation = 0; /// it's equal to min_block by default
        part_name = part->getNewName(part_info);
    }
    else /// Parts from ReplicatedMergeTree already have names
        part_name = part->name;

    LOG_TRACE(log, "Renaming temporary part {} to {}.", part->relative_path, part_name);

    if (auto it_duplicate = data_parts_by_info.find(part_info); it_duplicate != data_parts_by_info.end())
    {
        String message = "Part " + (*it_duplicate)->getNameWithState() + " already exists";

        if ((*it_duplicate)->checkState({DataPartState::Outdated, DataPartState::Deleting}))
            throw Exception(message + ", but it will be deleted soon", ErrorCodes::PART_IS_TEMPORARILY_LOCKED);

        throw Exception(message, ErrorCodes::DUPLICATE_DATA_PART);
    }

    DataPartPtr covering_part;
    DataPartsVector covered_parts = getActivePartsToReplace(part_info, part_name, covering_part, lock);
    DataPartsVector covered_parts_in_memory;

    if (covering_part)
    {
        LOG_WARNING(log, "Tried to add obsolete part {} covered by {}", part_name, covering_part->getNameWithState());
        return false;
    }

    /// Deduplication log used only from non-replicated MergeTree. Replicated
    /// tables have their own mechanism. We try to deduplicate at such deep
    /// level, because only here we know real part name which is required for
    /// deduplication.
    if (deduplication_log)
    {
        String block_id = part->getZeroLevelPartBlockID();
        auto res = deduplication_log->addPart(block_id, part_info);
        if (!res.second)
        {
            ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks);
            LOG_INFO(log, "Block with ID {} already exists as part {}; ignoring it", block_id, res.first.getPartName());
            return false;
        }
    }

    /// All checks are passed. Now we can rename the part on disk.
    /// So, we maintain invariant: if a non-temporary part in filesystem then it is in data_parts
    ///
    /// If out_transaction is null, we commit the part to the active set immediately, else add it to the transaction.
    part->name = part_name;
    part->info = part_info;
    part->is_temp = false;
    part->setState(DataPartState::PreCommitted);
    part->renameTo(part_name, true);

    auto part_it = data_parts_indexes.insert(part).first;

    if (out_transaction)
    {
        out_transaction->precommitted_parts.insert(part);
    }
    else
    {
        size_t reduce_bytes = 0;
        size_t reduce_rows = 0;
        size_t reduce_parts = 0;
        auto current_time = time(nullptr);
        for (const DataPartPtr & covered_part : covered_parts)
        {
            covered_part->remove_time.store(current_time, std::memory_order_relaxed);
            modifyPartState(covered_part, DataPartState::Outdated);
            removePartContributionToColumnAndSecondaryIndexSizes(covered_part);
            reduce_bytes += covered_part->getBytesOnDisk();
            reduce_rows += covered_part->rows_count;
            ++reduce_parts;
        }

        decreaseDataVolume(reduce_bytes, reduce_rows, reduce_parts);

        modifyPartState(part_it, DataPartState::Committed);
        addPartContributionToColumnAndSecondaryIndexSizes(part);
        addPartContributionToDataVolume(part);
    }

    auto part_in_memory = asInMemoryPart(part);
    if (part_in_memory && getSettings()->in_memory_parts_enable_wal)
    {
        auto wal = getWriteAheadLog();
        wal->addPart(part_in_memory);
    }

    if (out_covered_parts)
    {
        for (DataPartPtr & covered_part : covered_parts)
            out_covered_parts->emplace_back(std::move(covered_part));
    }

    return true;
}

MergeTreeData::DataPartsVector MergeTreeData::renameTempPartAndReplace(
    MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction, MergeTreeDeduplicationLog * deduplication_log)
{
    if (out_transaction && &out_transaction->data != this)
        throw Exception("MergeTreeData::Transaction for one table cannot be used with another. It is a bug.",
            ErrorCodes::LOGICAL_ERROR);

    DataPartsVector covered_parts;
    {
        auto lock = lockParts();
        renameTempPartAndReplace(part, increment, out_transaction, lock, &covered_parts, deduplication_log);
    }
    return covered_parts;
}

void MergeTreeData::removePartsFromWorkingSet(const MergeTreeData::DataPartsVector & remove, bool clear_without_timeout, DataPartsLock & /*acquired_lock*/)
{
    auto remove_time = clear_without_timeout ? 0 : time(nullptr);

    for (const DataPartPtr & part : remove)
    {
        if (part->getState() == IMergeTreeDataPart::State::Committed)
        {
            removePartContributionToColumnAndSecondaryIndexSizes(part);
            removePartContributionToDataVolume(part);
        }

        if (part->getState() == IMergeTreeDataPart::State::Committed || clear_without_timeout)
            part->remove_time.store(remove_time, std::memory_order_relaxed);

        if (part->getState() != IMergeTreeDataPart::State::Outdated)
            modifyPartState(part, IMergeTreeDataPart::State::Outdated);

        if (isInMemoryPart(part) && getSettings()->in_memory_parts_enable_wal)
            getWriteAheadLog()->dropPart(part->name);
    }
}

void MergeTreeData::removePartsFromWorkingSetImmediatelyAndSetTemporaryState(const DataPartsVector & remove)
{
    auto lock = lockParts();

    for (const auto & part : remove)
    {
        auto it_part = data_parts_by_info.find(part->info);
        if (it_part == data_parts_by_info.end())
            throw Exception("Part " + part->getNameWithState() + " not found in data_parts", ErrorCodes::LOGICAL_ERROR);

        modifyPartState(part, IMergeTreeDataPart::State::Temporary);
        /// Erase immediately
        data_parts_indexes.erase(it_part);
    }
}

void MergeTreeData::removePartsFromWorkingSet(const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock * acquired_lock)
{
    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();

    for (const auto & part : remove)
    {
        if (!data_parts_by_info.count(part->info))
            throw Exception("Part " + part->getNameWithState() + " not found in data_parts", ErrorCodes::LOGICAL_ERROR);

        part->assertState({DataPartState::PreCommitted, DataPartState::Committed, DataPartState::Outdated});
    }

    removePartsFromWorkingSet(remove, clear_without_timeout, lock);
}

MergeTreeData::DataPartsVector MergeTreeData::removePartsInRangeFromWorkingSet(const MergeTreePartInfo & drop_range, bool clear_without_timeout,
                                                                               DataPartsLock & lock)
{
    DataPartsVector parts_to_remove;

    if (drop_range.min_block > drop_range.max_block)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid drop range: {}", drop_range.getPartName());

    auto partition_range = getDataPartsPartitionRange(drop_range.partition_id);

    for (const DataPartPtr & part : partition_range)
    {
        if (part->info.partition_id != drop_range.partition_id)
            throw Exception("Unexpected partition_id of part " + part->name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);

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
                LOG_INFO(log, "Skipping drop range for part {} because covering part {} already exists", drop_range.getPartName(), part->name);
                return {};
            }
        }

        if (part->info.min_block < drop_range.min_block)
        {
            if (drop_range.min_block <= part->info.max_block)
            {
                /// Intersect left border
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected merged part {} intersecting drop range {}",
                                part->name, drop_range.getPartName());
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
                            part->name, drop_range.getPartName());
        }

        if (part->getState() != DataPartState::Deleting)
            parts_to_remove.emplace_back(part);
    }

    removePartsFromWorkingSet(parts_to_remove, clear_without_timeout, lock);

    return parts_to_remove;
}

void MergeTreeData::forgetPartAndMoveToDetached(const MergeTreeData::DataPartPtr & part_to_detach, const String & prefix, bool
restore_covered)
{
    if (prefix.empty())
        LOG_INFO(log, "Renaming {} to {} and forgetting it.", part_to_detach->relative_path, part_to_detach->name);
    else
        LOG_INFO(log, "Renaming {} to {}_{} and forgetting it.", part_to_detach->relative_path, prefix, part_to_detach->name);

    auto lock = lockParts();

    auto it_part = data_parts_by_info.find(part_to_detach->info);
    if (it_part == data_parts_by_info.end())
        throw Exception("No such data part " + part_to_detach->getNameWithState(), ErrorCodes::NO_SUCH_DATA_PART);

    /// What if part_to_detach is a reference to *it_part? Make a new owner just in case.
    DataPartPtr part = *it_part;

    if (part->getState() == DataPartState::Committed)
    {
        removePartContributionToDataVolume(part);
        removePartContributionToColumnAndSecondaryIndexSizes(part);
    }
    modifyPartState(it_part, DataPartState::Deleting);

    part->renameToDetached(prefix);

    data_parts_indexes.erase(it_part);

    if (restore_covered && part->info.level == 0)
    {
        LOG_WARNING(log, "Will not recover parts covered by zero-level part {}", part->name);
        return;
    }

    if (restore_covered)
    {
        Strings restored;
        bool error = false;
        String error_parts;

        Int64 pos = part->info.min_block;

        auto is_appropriate_state = [] (DataPartState state)
        {
            return state == DataPartState::Committed || state == DataPartState::Outdated;
        };

        auto update_error = [&] (DataPartIteratorByInfo it)
        {
            error = true;
            error_parts += (*it)->getNameWithState() + " ";
        };

        auto it_middle = data_parts_by_info.lower_bound(part->info);

        /// Restore the leftmost part covered by the part
        if (it_middle != data_parts_by_info.begin())
        {
            auto it = std::prev(it_middle);

            if (part->contains(**it) && is_appropriate_state((*it)->getState()))
            {
                /// Maybe, we must consider part level somehow
                if ((*it)->info.min_block != part->info.min_block)
                    update_error(it);

                if ((*it)->getState() != DataPartState::Committed)
                {
                    addPartContributionToColumnAndSecondaryIndexSizes(*it);
                    addPartContributionToDataVolume(*it);
                    modifyPartState(it, DataPartState::Committed); // iterator is not invalidated here
                }

                pos = (*it)->info.max_block + 1;
                restored.push_back((*it)->name);
            }
            else
                update_error(it);
        }
        else
            error = true;

        /// Restore "right" parts
        for (auto it = it_middle; it != data_parts_by_info.end() && part->contains(**it); ++it)
        {
            if ((*it)->info.min_block < pos)
                continue;

            if (!is_appropriate_state((*it)->getState()))
            {
                update_error(it);
                continue;
            }

            if ((*it)->info.min_block > pos)
                update_error(it);

            if ((*it)->getState() != DataPartState::Committed)
            {
                addPartContributionToColumnAndSecondaryIndexSizes(*it);
                addPartContributionToDataVolume(*it);
                modifyPartState(it, DataPartState::Committed);
            }

            pos = (*it)->info.max_block + 1;
            restored.push_back((*it)->name);
        }

        if (pos != part->info.max_block + 1)
            error = true;

        for (const String & name : restored)
        {
            LOG_INFO(log, "Activated part {}", name);
        }

        if (error)
        {
            LOG_ERROR(log, "The set of parts restored in place of {} looks incomplete. There might or might not be a data loss.{}", part->name, (error_parts.empty() ? "" : " Suspicious parts: " + error_parts));
        }
    }
}


void MergeTreeData::tryRemovePartImmediately(DataPartPtr && part)
{
    DataPartPtr part_to_delete;
    {
        auto lock = lockParts();

        LOG_TRACE(log, "Trying to immediately remove part {}", part->getNameWithState());

        if (part->getState() != DataPartState::Temporary)
        {
            auto it = data_parts_by_info.find(part->info);
            if (it == data_parts_by_info.end() || (*it).get() != part.get())
                throw Exception("Part " + part->name + " doesn't exist", ErrorCodes::LOGICAL_ERROR);

            part.reset();

            if (!((*it)->getState() == DataPartState::Outdated && it->unique()))
                return;

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
        part_to_delete->remove();
    }
    catch (...)
    {
        rollbackDeletingParts({part_to_delete});
        throw;
    }

    removePartsFinally({part_to_delete});
    LOG_TRACE(log, "Removed part {}", part_to_delete->name);
}


size_t MergeTreeData::getTotalActiveSizeInBytes() const
{
    return total_active_size_bytes.load(std::memory_order_acquire);
}


size_t MergeTreeData::getTotalActiveSizeInRows() const
{
    return total_active_size_rows.load(std::memory_order_acquire);
}


size_t MergeTreeData::getPartsCount() const
{
    return total_active_size_parts.load(std::memory_order_acquire);
}


size_t MergeTreeData::getMaxPartsCountForPartitionWithState(DataPartState state) const
{
    auto lock = lockParts();

    size_t res = 0;
    size_t cur_count = 0;
    const String * cur_partition_id = nullptr;

    for (const auto & part : getDataPartsStateRange(state))
    {
        if (cur_partition_id && part->info.partition_id == *cur_partition_id)
        {
            ++cur_count;
        }
        else
        {
            cur_partition_id = &part->info.partition_id;
            cur_count = 1;
        }

        res = std::max(res, cur_count);
    }

    return res;
}


size_t MergeTreeData::getMaxPartsCountForPartition() const
{
    return getMaxPartsCountForPartitionWithState(DataPartState::Committed);
}


size_t MergeTreeData::getMaxInactivePartsCountForPartition() const
{
    return getMaxPartsCountForPartitionWithState(DataPartState::Outdated);
}


std::optional<Int64> MergeTreeData::getMinPartDataVersion() const
{
    auto lock = lockParts();

    std::optional<Int64> result;
    for (const auto & part : getDataPartsStateRange(DataPartState::Committed))
    {
        if (!result || *result > part->info.getDataVersion())
            result = part->info.getDataVersion();
    }

    return result;
}


void MergeTreeData::delayInsertOrThrowIfNeeded(Poco::Event * until) const
{
    const auto settings = getSettings();
    const size_t parts_count_in_total = getPartsCount();
    if (parts_count_in_total >= settings->max_parts_in_total)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception("Too many parts (" + toString(parts_count_in_total) + ") in all partitions in total. This indicates wrong choice of partition key. The threshold can be modified with 'max_parts_in_total' setting in <merge_tree> element in config.xml or with per-table setting.", ErrorCodes::TOO_MANY_PARTS);
    }

    size_t parts_count_in_partition = getMaxPartsCountForPartition();
    ssize_t k_inactive = -1;
    if (settings->inactive_parts_to_throw_insert > 0 || settings->inactive_parts_to_delay_insert > 0)
    {
        size_t inactive_parts_count_in_partition = getMaxInactivePartsCountForPartition();
        if (settings->inactive_parts_to_throw_insert > 0 && inactive_parts_count_in_partition >= settings->inactive_parts_to_throw_insert)
        {
            ProfileEvents::increment(ProfileEvents::RejectedInserts);
            throw Exception(
                ErrorCodes::TOO_MANY_PARTS,
                "Too many inactive parts ({}). Parts cleaning are processing significantly slower than inserts",
                inactive_parts_count_in_partition);
        }
        k_inactive = ssize_t(inactive_parts_count_in_partition) - ssize_t(settings->inactive_parts_to_delay_insert);
    }

    if (parts_count_in_partition >= settings->parts_to_throw_insert)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception(
            ErrorCodes::TOO_MANY_PARTS,
            "Too many parts ({}). Merges are processing significantly slower than inserts",
            parts_count_in_partition);
    }

    if (k_inactive < 0 && parts_count_in_partition < settings->parts_to_delay_insert)
        return;

    const ssize_t k_active = ssize_t(parts_count_in_partition) - ssize_t(settings->parts_to_delay_insert);
    size_t max_k;
    size_t k;
    if (k_active > k_inactive)
    {
        max_k = settings->parts_to_throw_insert - settings->parts_to_delay_insert;
        k = k_active + 1;
    }
    else
    {
        max_k = settings->inactive_parts_to_throw_insert - settings->inactive_parts_to_delay_insert;
        k = k_inactive + 1;
    }
    const double delay_milliseconds = ::pow(settings->max_delay_to_insert * 1000, static_cast<double>(k) / max_k);

    ProfileEvents::increment(ProfileEvents::DelayedInserts);
    ProfileEvents::increment(ProfileEvents::DelayedInsertsMilliseconds, delay_milliseconds);

    CurrentMetrics::Increment metric_increment(CurrentMetrics::DelayedInserts);

    LOG_INFO(log, "Delaying inserting block by {} ms. because there are {} parts", delay_milliseconds, parts_count_in_partition);

    if (until)
        until->tryWait(delay_milliseconds);
    else
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<size_t>(delay_milliseconds)));
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


void MergeTreeData::swapActivePart(MergeTreeData::DataPartPtr part_copy)
{
    auto lock = lockParts();
    for (auto original_active_part : getDataPartsStateRange(DataPartState::Committed)) // NOLINT (copy is intended)
    {
        if (part_copy->name == original_active_part->name)
        {
            auto active_part_it = data_parts_by_info.find(original_active_part->info);
            if (active_part_it == data_parts_by_info.end())
                throw Exception("Cannot swap part '" + part_copy->name + "', no such active part.", ErrorCodes::NO_SUCH_DATA_PART);

            /// We do not check allow_remote_fs_zero_copy_replication here because data may be shared
            /// when allow_remote_fs_zero_copy_replication turned on and off again

            original_active_part->force_keep_shared_data = false;

            if (original_active_part->volume->getDisk()->supportZeroCopyReplication() &&
                part_copy->volume->getDisk()->supportZeroCopyReplication() &&
                original_active_part->getUniqueId() == part_copy->getUniqueId())
            {
                /// May be when several volumes use the same S3/HDFS storage
                original_active_part->force_keep_shared_data = true;
            }

            modifyPartState(original_active_part, DataPartState::DeleteOnDestroy);
            data_parts_indexes.erase(active_part_it);

            auto part_it = data_parts_indexes.insert(part_copy).first;
            modifyPartState(part_it, DataPartState::Committed);

            removePartContributionToDataVolume(original_active_part);
            addPartContributionToDataVolume(part_copy);

            auto disk = original_active_part->volume->getDisk();
            String marker_path = fs::path(original_active_part->getFullRelativePath()) / IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME;
            try
            {
                disk->createFile(marker_path);
            }
            catch (Poco::Exception & e)
            {
                LOG_ERROR(log, "{} (while creating DeleteOnDestroy marker: {})", e.what(), backQuote(fullPath(disk, marker_path)));
            }
            return;
        }
    }
    throw Exception("Cannot swap part '" + part_copy->name + "', no such active part.", ErrorCodes::NO_SUCH_DATA_PART);
}


MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(const MergeTreePartInfo & part_info) const
{
    auto lock = lockParts();
    return getActiveContainingPart(part_info, DataPartState::Committed, lock);
}

MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(const String & part_name) const
{
    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
    return getActiveContainingPart(part_info);
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorInPartition(MergeTreeData::DataPartState state, const String & partition_id) const
{
    DataPartStateAndPartitionID state_with_partition{state, partition_id};

    auto lock = lockParts();
    return DataPartsVector(
        data_parts_by_state_and_info.lower_bound(state_with_partition),
        data_parts_by_state_and_info.upper_bound(state_with_partition));
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorInPartitions(MergeTreeData::DataPartState state, const std::unordered_set<String> & partition_ids) const
{
    auto lock = lockParts();
    DataPartsVector res;
    for (const auto & partition_id : partition_ids)
    {
        DataPartStateAndPartitionID state_with_partition{state, partition_id};
        insertAtEnd(
            res,
            DataPartsVector(
                data_parts_by_state_and_info.lower_bound(state_with_partition),
                data_parts_by_state_and_info.upper_bound(state_with_partition)));
    }
    return res;
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExists(const MergeTreePartInfo & part_info, const MergeTreeData::DataPartStates & valid_states)
{
    auto lock = lockParts();

    auto it = data_parts_by_info.find(part_info);
    if (it == data_parts_by_info.end())
        return nullptr;

    for (auto state : valid_states)
        if ((*it)->getState() == state)
            return *it;

    return nullptr;
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExists(const String & part_name, const MergeTreeData::DataPartStates & valid_states)
{
    return getPartIfExists(MergeTreePartInfo::fromPartName(part_name, format_version), valid_states);
}


static void loadPartAndFixMetadataImpl(MergeTreeData::MutableDataPartPtr part)
{
    auto disk = part->volume->getDisk();
    String full_part_path = part->getFullRelativePath();

    part->loadColumnsChecksumsIndexes(false, true);
    part->modification_time = disk->getLastModified(full_part_path).epochTime();
}

void MergeTreeData::calculateColumnAndSecondaryIndexSizesImpl()
{
    column_sizes.clear();

    /// Take into account only committed parts
    auto committed_parts_range = getDataPartsStateRange(DataPartState::Committed);
    for (const auto & part : committed_parts_range)
        addPartContributionToColumnAndSecondaryIndexSizes(part);
}

void MergeTreeData::addPartContributionToColumnAndSecondaryIndexSizes(const DataPartPtr & part)
{
    for (const auto & column : part->getColumns())
    {
        ColumnSize & total_column_size = column_sizes[column.name];
        ColumnSize part_column_size = part->getColumnSize(column.name, *column.type);
        total_column_size.add(part_column_size);
    }

    auto indexes_descriptions = getInMemoryMetadataPtr()->secondary_indices;
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
        ColumnSize part_column_size = part->getColumnSize(column.name, *column.type);

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

    auto indexes_descriptions = getInMemoryMetadataPtr()->secondary_indices;
    for (const auto & index : indexes_descriptions)
    {
        IndexSize & total_secondary_index_size = secondary_index_sizes[index.name];
        IndexSize part_secondary_index_size = part->getSecondaryIndexSize(index.name);

        auto log_subtract = [&](size_t & from, size_t value, const char * field)
        {
            if (value > from)
                LOG_ERROR(log, "Possibly incorrect index size subtraction: {} - {} = {}, index: {}, field: {}",
                    from, value, from - value, index.name, field);

            from -= value;
        };

        log_subtract(total_secondary_index_size.data_compressed, part_secondary_index_size.data_compressed, ".data_compressed");
        log_subtract(total_secondary_index_size.data_uncompressed, part_secondary_index_size.data_uncompressed, ".data_uncompressed");
        log_subtract(total_secondary_index_size.marks, part_secondary_index_size.marks, ".marks");
    }
}

void MergeTreeData::checkAlterPartitionIsPossible(
    const PartitionCommands & commands, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & settings) const
{
    for (const auto & command : commands)
    {
        if (command.type == PartitionCommand::DROP_DETACHED_PARTITION
            && !settings.allow_drop_detached)
            throw DB::Exception("Cannot execute query: DROP DETACHED PART is disabled "
                                "(see allow_drop_detached setting)", ErrorCodes::SUPPORT_IS_DISABLED);

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
                getPartitionIDFromQuery(command.partition, getContext());
            }
        }
    }
}

void MergeTreeData::checkPartitionCanBeDropped(const ASTPtr & partition)
{
    const String partition_id = getPartitionIDFromQuery(partition, getContext());
    auto parts_to_remove = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);

    UInt64 partition_size = 0;

    for (const auto & part : parts_to_remove)
        partition_size += part->getBytesOnDisk();

    auto table_id = getStorageID();
    getContext()->checkPartitionCanBeDropped(table_id.database_name, table_id.table_name, partition_size);
}

void MergeTreeData::checkPartCanBeDropped(const String & part_name)
{
    auto part = getPartIfExists(part_name, {MergeTreeDataPartState::Committed});
    if (!part)
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No part {} in committed state", part_name);

    auto table_id = getStorageID();
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
        if (!parts.back() || parts.back()->name != part_info.getPartName())
            throw Exception("Part " + partition_id + " is not exists or not active", ErrorCodes::NO_SUCH_DATA_PART);
    }
    else
        parts = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);

    auto disk = getStoragePolicy()->getDiskByName(name);
    if (!disk)
        throw Exception("Disk " + name + " does not exists on policy " + getStoragePolicy()->getName(), ErrorCodes::UNKNOWN_DISK);

    parts.erase(std::remove_if(parts.begin(), parts.end(), [&](auto part_ptr)
        {
            return part_ptr->volume->getDisk()->getName() == disk->getName();
        }), parts.end());

    if (parts.empty())
    {
        String no_parts_to_move_message;
        if (moving_part)
            no_parts_to_move_message = "Part '" + partition_id + "' is already on disk '" + disk->getName() + "'";
        else
            no_parts_to_move_message = "All parts of partition '" + partition_id + "' are already on disk '" + disk->getName() + "'";

        throw Exception(no_parts_to_move_message, ErrorCodes::UNKNOWN_DISK);
    }

    if (!movePartsToSpace(parts, std::static_pointer_cast<Space>(disk)))
        throw Exception("Cannot move parts because moves are manually disabled", ErrorCodes::ABORTED);
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
        if (!parts.back() || parts.back()->name != part_info.getPartName())
            throw Exception("Part " + partition_id + " is not exists or not active", ErrorCodes::NO_SUCH_DATA_PART);
    }
    else
        parts = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);

    auto volume = getStoragePolicy()->getVolumeByName(name);
    if (!volume)
        throw Exception("Volume " + name + " does not exists on policy " + getStoragePolicy()->getName(), ErrorCodes::UNKNOWN_DISK);

    if (parts.empty())
        throw Exception("Nothing to move (сheck that the partition exists).", ErrorCodes::NO_SUCH_DATA_PART);

    parts.erase(std::remove_if(parts.begin(), parts.end(), [&](auto part_ptr)
        {
            for (const auto & disk : volume->getDisks())
            {
                if (part_ptr->volume->getDisk()->getName() == disk->getName())
                {
                    return true;
                }
            }
            return false;
        }), parts.end());

    if (parts.empty())
    {
        String no_parts_to_move_message;
        if (moving_part)
            no_parts_to_move_message = "Part '" + partition_id + "' is already on volume '" + volume->getName() + "'";
        else
            no_parts_to_move_message = "All parts of partition '" + partition_id + "' are already on volume '" + volume->getName() + "'";

        throw Exception(no_parts_to_move_message, ErrorCodes::UNKNOWN_DISK);
    }

    if (!movePartsToSpace(parts, std::static_pointer_cast<Space>(volume)))
        throw Exception("Cannot move parts because moves are manually disabled", ErrorCodes::ABORTED);
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

Pipe MergeTreeData::alterPartition(
    const StorageMetadataPtr & metadata_snapshot,
    const PartitionCommands & commands,
    ContextPtr query_context)
{
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
                    checkPartCanBeDropped(part_name);
                    dropPart(part_name, command.detach, query_context);
                }
                else
                {
                    checkPartitionCanBeDropped(command.partition);
                    dropPartition(command.partition, command.detach, query_context);
                }
            }
            break;

            case PartitionCommand::DROP_DETACHED_PARTITION:
                dropDetached(command.partition, command.part, query_context);
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
                    {
                        checkPartitionCanBeDropped(command.partition);
                        String dest_database = query_context->resolveDatabase(command.to_database);
                        auto dest_storage = DatabaseCatalog::instance().getTable({dest_database, command.to_table}, query_context);
                        movePartitionToTable(dest_storage, command.partition, query_context);
                    }
                    break;

                    case PartitionCommand::MoveDestinationType::SHARD:
                    {
                        if (!getSettings()->part_moves_between_shards_enable)
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
                checkPartitionCanBeDropped(command.partition);
                String from_database = query_context->resolveDatabase(command.from_database);
                auto from_storage = DatabaseCatalog::instance().getTable({from_database, command.from_table}, query_context);
                replacePartitionFrom(from_storage, command.partition, command.replace, query_context);
            }
            break;

            case PartitionCommand::FETCH_PARTITION:
                fetchPartition(command.partition, metadata_snapshot, command.from_zookeeper_path, command.part, query_context);
                break;

            case PartitionCommand::FREEZE_PARTITION:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
                current_command_results = freezePartition(command.partition, metadata_snapshot, command.with_name, query_context, lock);
            }
            break;

            case PartitionCommand::FREEZE_ALL_PARTITIONS:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
                current_command_results = freezeAll(command.with_name, metadata_snapshot, query_context, lock);
            }
            break;

            case PartitionCommand::UNFREEZE_PARTITION:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
                current_command_results = unfreezePartition(command.partition, command.with_name, query_context, lock);
            }
            break;

            case PartitionCommand::UNFREEZE_ALL_PARTITIONS:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
                current_command_results = unfreezeAll(command.with_name, query_context, lock);
            }

            break;

            default:
                throw Exception("Uninitialized partition command", ErrorCodes::LOGICAL_ERROR);
        }
        for (auto & command_result : current_command_results)
            command_result.command_type = command.typeToString();
        result.insert(result.end(), current_command_results.begin(), current_command_results.end());
    }

    if (query_context->getSettingsRef().alter_partition_verbose_result)
        return convertCommandsResultToSource(result);

    return {};
}


BackupEntries MergeTreeData::backup(const ASTs & partitions, ContextPtr local_context)
{
    DataPartsVector data_parts;
    if (partitions.empty())
        data_parts = getDataPartsVector();
    else
        data_parts = getDataPartsVectorInPartitions(MergeTreeDataPartState::Committed, getPartitionIDsFromQuery(partitions, local_context));
    return backupDataParts(data_parts);
}


BackupEntries MergeTreeData::backupDataParts(const DataPartsVector & data_parts)
{
    BackupEntries backup_entries;
    std::map<DiskPtr, std::shared_ptr<TemporaryFileOnDisk>> temp_dirs;

    for (const auto & part : data_parts)
    {
        auto disk = part->volume->getDisk();

        auto temp_dir_it = temp_dirs.find(disk);
        if (temp_dir_it == temp_dirs.end())
            temp_dir_it = temp_dirs.emplace(disk, std::make_shared<TemporaryFileOnDisk>(disk, "tmp/backup_")).first;
        auto temp_dir_owner = temp_dir_it->second;
        fs::path temp_dir = temp_dir_owner->getPath();

        fs::path part_dir = part->getFullRelativePath();
        fs::path temp_part_dir = temp_dir / part->relative_path;
        disk->createDirectories(temp_part_dir);

        for (const auto & [filepath, checksum] : part->checksums.files)
        {
            String relative_filepath = fs::path(part->relative_path) / filepath;
            String hardlink_filepath = temp_part_dir / filepath;
            disk->createHardLink(part_dir / filepath, hardlink_filepath);
            UInt128 file_hash{checksum.file_hash.first, checksum.file_hash.second};
            backup_entries.emplace_back(
                relative_filepath,
                std::make_unique<BackupEntryFromImmutableFile>(disk, hardlink_filepath, checksum.file_size, file_hash, temp_dir_owner));
        }

        for (const auto & filepath : part->getFileNamesWithoutChecksums())
        {
            String relative_filepath = fs::path(part->relative_path) / filepath;
            backup_entries.emplace_back(relative_filepath, std::make_unique<BackupEntryFromSmallFile>(disk, part_dir / filepath));
        }
    }

    return backup_entries;
}


RestoreDataTasks MergeTreeData::restoreDataPartsFromBackup(const BackupPtr & backup, const String & data_path_in_backup,
                                                           const std::unordered_set<String> & partition_ids,
                                                           SimpleIncrement * increment)
{
    RestoreDataTasks restore_tasks;

    Strings part_names = backup->list(data_path_in_backup);
    for (const String & part_name : part_names)
    {
        const auto part_info = MergeTreePartInfo::tryParsePartName(part_name, format_version);

        if (!part_info)
            continue;

        if (!partition_ids.empty() && !partition_ids.contains(part_info->partition_id))
            continue;

        UInt64 total_size_of_part = 0;
        Strings filenames = backup->list(data_path_in_backup + part_name + "/", "");
        for (const String & filename : filenames)
            total_size_of_part += backup->getSize(data_path_in_backup + part_name + "/" + filename);

        std::shared_ptr<IReservation> reservation = getStoragePolicy()->reserveAndCheck(total_size_of_part);

        auto restore_task = [this,
                             backup,
                             data_path_in_backup,
                             part_name,
                             part_info = std::move(part_info),
                             filenames = std::move(filenames),
                             reservation,
                             increment]()
        {
            auto disk = reservation->getDisk();

            auto temp_part_dir_owner = std::make_shared<TemporaryFileOnDisk>(disk, relative_data_path + "restoring_" + part_name + "_");
            String temp_part_dir = temp_part_dir_owner->getPath();
            disk->createDirectories(temp_part_dir);

            assert(temp_part_dir.starts_with(relative_data_path));
            String relative_temp_part_dir = temp_part_dir.substr(relative_data_path.size());

            for (const String & filename : filenames)
            {
                auto backup_entry = backup->read(data_path_in_backup + part_name + "/" + filename);
                auto read_buffer = backup_entry->getReadBuffer();
                auto write_buffer = disk->writeFile(temp_part_dir + "/" + filename);
                copyData(*read_buffer, *write_buffer);
            }

            auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);
            auto part = createPart(part_name, *part_info, single_disk_volume, relative_temp_part_dir);
            part->loadColumnsChecksumsIndexes(false, true);
            renameTempPartAndAdd(part, increment);
        };

        restore_tasks.emplace_back(std::move(restore_task));
    }

    return restore_tasks;
}


String MergeTreeData::getPartitionIDFromQuery(const ASTPtr & ast, ContextPtr local_context) const
{
    const auto & partition_ast = ast->as<ASTPartition &>();

    if (!partition_ast.value)
    {
        MergeTreePartInfo::validatePartitionID(partition_ast.id, format_version);
        return partition_ast.id;
    }

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// Month-partitioning specific - partition ID can be passed in the partition value.
        const auto * partition_lit = partition_ast.value->as<ASTLiteral>();
        if (partition_lit && partition_lit->value.getType() == Field::Types::String)
        {
            String partition_id = partition_lit->value.get<String>();
            MergeTreePartInfo::validatePartitionID(partition_id, format_version);
            return partition_id;
        }
    }

    /// Re-parse partition key fields using the information about expected field types.

    auto metadata_snapshot = getInMemoryMetadataPtr();
    size_t fields_count = metadata_snapshot->getPartitionKey().sample_block.columns();
    if (partition_ast.fields_count != fields_count)
        throw Exception(
            "Wrong number of fields in the partition expression: " + toString(partition_ast.fields_count) +
            ", must be: " + toString(fields_count),
            ErrorCodes::INVALID_PARTITION_VALUE);

    if (auto * f = partition_ast.value->as<ASTFunction>())
    {
        assert(f->name == "tuple");
        if (f->arguments && !f->arguments->as<ASTExpressionList>()->children.empty())
        {
            ASTPtr query = partition_ast.value->clone();
            auto syntax_analyzer_result
                = TreeRewriter(local_context)
                      .analyze(query, metadata_snapshot->getPartitionKey().sample_block.getNamesAndTypesList(), {}, {}, false, false);
            auto actions = ExpressionAnalyzer(query, syntax_analyzer_result, local_context).getActions(true);
            if (actions->hasArrayJoin())
                throw Exception("The partition expression cannot contain array joins", ErrorCodes::INVALID_PARTITION_VALUE);
        }
    }

    const FormatSettings format_settings;
    Row partition_row(fields_count);

    if (fields_count)
    {
        ConcatReadBuffer buf;
        buf.appendBuffer(std::make_unique<ReadBufferFromMemory>("(", 1));
        buf.appendBuffer(std::make_unique<ReadBufferFromMemory>(partition_ast.fields_str.data(), partition_ast.fields_str.size()));
        buf.appendBuffer(std::make_unique<ReadBufferFromMemory>(")", 1));

        auto input_format = local_context->getInputFormat(
            "Values",
            buf,
            metadata_snapshot->getPartitionKey().sample_block,
            local_context->getSettingsRef().max_block_size);
        QueryPipeline pipeline(std::move(input_format));
        PullingPipelineExecutor executor(pipeline);

        Block block;
        executor.pull(block);

        if (!block || !block.rows())
            throw Exception(
                "Could not parse partition value: `" + partition_ast.fields_str + "`",
                ErrorCodes::INVALID_PARTITION_VALUE);

        for (size_t i = 0; i < fields_count; ++i)
            block.getByPosition(i).column->get(0, partition_row[i]);
    }

    MergeTreePartition partition(std::move(partition_row));
    String partition_id = partition.getID(*this);

    {
        auto data_parts_lock = lockParts();
        DataPartPtr existing_part_in_partition = getAnyPartInPartition(partition_id, data_parts_lock);
        if (existing_part_in_partition && existing_part_in_partition->partition.value != partition.value)
        {
            WriteBufferFromOwnString buf;
            writeCString("Parsed partition value: ", buf);
            partition.serializeText(*this, buf, format_settings);
            writeCString(" doesn't match partition value for an existing part with the same partition ID: ", buf);
            writeString(existing_part_in_partition->name, buf);
            throw Exception(buf.str(), ErrorCodes::INVALID_PARTITION_VALUE);
        }
    }

    return partition_id;
}

std::unordered_set<String> MergeTreeData::getPartitionIDsFromQuery(const ASTs & asts, ContextPtr local_context) const
{
    std::unordered_set<String> partition_ids;
    for (const auto & ast : asts)
        partition_ids.emplace(getPartitionIDFromQuery(ast, local_context));
    return partition_ids;
}


MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVector(
    const DataPartStates & affordable_states, DataPartStateVector * out_states, bool require_projection_parts) const
{
    DataPartsVector res;
    DataPartsVector buf;
    {
        auto lock = lockParts();

        for (auto state : affordable_states)
        {
            auto range = getDataPartsStateRange(state);

            if (require_projection_parts)
            {
                for (const auto & part : range)
                {
                    for (const auto & [_, projection_part] : part->getProjectionParts())
                        res.push_back(projection_part);
                }
            }
            else
            {
                std::swap(buf, res);
                res.clear();
                std::merge(range.begin(), range.end(), buf.begin(), buf.end(), std::back_inserter(res), LessDataPart()); //-V783
            }
        }

        if (out_states != nullptr)
        {
            out_states->resize(res.size());
            if (require_projection_parts)
            {
                for (size_t i = 0; i < res.size(); ++i)
                    (*out_states)[i] = res[i]->getParentPart()->getState();
            }
            else
            {
                for (size_t i = 0; i < res.size(); ++i)
                    (*out_states)[i] = res[i]->getState();
            }
        }
    }

    return res;
}

MergeTreeData::DataPartsVector
MergeTreeData::getAllDataPartsVector(MergeTreeData::DataPartStateVector * out_states, bool require_projection_parts) const
{
    DataPartsVector res;
    if (require_projection_parts)
    {
        auto lock = lockParts();
        for (const auto & part : data_parts_by_info)
        {
            for (const auto & [p_name, projection_part] : part->getProjectionParts())
                res.push_back(projection_part);
        }

        if (out_states != nullptr)
        {
            out_states->resize(res.size());
            for (size_t i = 0; i < res.size(); ++i)
                (*out_states)[i] = res[i]->getParentPart()->getState();
        }
    }
    else
    {
        auto lock = lockParts();
        res.assign(data_parts_by_info.begin(), data_parts_by_info.end());

        if (out_states != nullptr)
        {
            out_states->resize(res.size());
            for (size_t i = 0; i < res.size(); ++i)
                (*out_states)[i] = res[i]->getState();
        }
    }

    return res;
}

std::vector<DetachedPartInfo> MergeTreeData::getDetachedParts() const
{
    std::vector<DetachedPartInfo> res;

    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
    {
        String detached_path = fs::path(path) / MergeTreeData::DETACHED_DIR_NAME;

        /// Note: we don't care about TOCTOU issue here.
        if (disk->exists(detached_path))
        {
            for (auto it = disk->iterateDirectory(detached_path); it->isValid(); it->next())
            {
                auto part = DetachedPartInfo::parseDetachedPartName(it->name(), format_version);
                part.disk = disk->getName();

                res.push_back(std::move(part));
            }
        }
    }
    return res;
}

void MergeTreeData::validateDetachedPartName(const String & name) const
{
    if (name.find('/') != std::string::npos || name == "." || name == "..")
        throw DB::Exception("Invalid part name '" + name + "'", ErrorCodes::INCORRECT_FILE_NAME);

    auto full_path = getFullRelativePathForPart(name, "detached/");

    if (!full_path)
        throw DB::Exception("Detached part \"" + name + "\" not found" , ErrorCodes::BAD_DATA_PART_NAME);

    if (startsWith(name, "attaching_") || startsWith(name, "deleting_"))
        throw DB::Exception("Cannot drop part " + name + ": "
                            "most likely it is used by another DROP or ATTACH query.",
                            ErrorCodes::BAD_DATA_PART_NAME);
}

void MergeTreeData::dropDetached(const ASTPtr & partition, bool part, ContextPtr local_context)
{
    PartsTemporaryRename renamed_parts(*this, "detached/");

    if (part)
    {
        String part_name = partition->as<ASTLiteral &>().value.safeGet<String>();
        validateDetachedPartName(part_name);
        renamed_parts.addPart(part_name, "deleting_" + part_name);
    }
    else
    {
        String partition_id = getPartitionIDFromQuery(partition, local_context);
        DetachedPartsInfo detached_parts = getDetachedParts();
        for (const auto & part_info : detached_parts)
            if (part_info.valid_name && part_info.partition_id == partition_id
                && part_info.prefix != "attaching" && part_info.prefix != "deleting")
                renamed_parts.addPart(part_info.dir_name, "deleting_" + part_info.dir_name);
    }

    LOG_DEBUG(log, "Will drop {} detached parts.", renamed_parts.old_and_new_names.size());

    renamed_parts.tryRenameAll();

    for (auto & [old_name, new_name] : renamed_parts.old_and_new_names)
    {
        const auto & [path, disk] = renamed_parts.old_part_name_to_path_and_disk[old_name];
        disk->removeRecursive(fs::path(path) / "detached" / new_name / "");
        LOG_DEBUG(log, "Dropped detached part {}", old_name);
        old_name.clear();
    }
}

MergeTreeData::MutableDataPartsVector MergeTreeData::tryLoadPartsToAttach(const ASTPtr & partition, bool attach_part,
        ContextPtr local_context, PartsTemporaryRename & renamed_parts)
{
    const String source_dir = "detached/";

    std::map<String, DiskPtr> name_to_disk;

    /// Let's compose a list of parts that should be added.
    if (attach_part)
    {
        const String part_id = partition->as<ASTLiteral &>().value.safeGet<String>();

        validateDetachedPartName(part_id);
        renamed_parts.addPart(part_id, "attaching_" + part_id);

        if (MergeTreePartInfo::tryParsePartName(part_id, format_version))
            name_to_disk[part_id] = getDiskForPart(part_id, source_dir);
    }
    else
    {
        String partition_id = getPartitionIDFromQuery(partition, local_context);
        LOG_DEBUG(log, "Looking for parts for partition {} in {}", partition_id, source_dir);
        ActiveDataPartSet active_parts(format_version);

        const auto disks = getStoragePolicy()->getDisks();

        for (const auto & disk : disks)
        {
            for (auto it = disk->iterateDirectory(relative_data_path + source_dir); it->isValid(); it->next())
            {
                const String & name = it->name();

                // TODO what if name contains "_tryN" suffix?
                /// Parts with prefix in name (e.g. attaching_1_3_3_0, deleting_1_3_3_0) will be ignored
                if (auto part_opt = MergeTreePartInfo::tryParsePartName(name, format_version);
                    !part_opt || part_opt->partition_id != partition_id)
                {
                    continue;
                }

                LOG_DEBUG(log, "Found part {}", name);
                active_parts.add(name);
                name_to_disk[name] = disk;
            }
        }
        LOG_DEBUG(log, "{} of them are active", active_parts.size());

        /// Inactive parts are renamed so they can not be attached in case of repeated ATTACH.
        for (const auto & [name, disk] : name_to_disk)
        {
            const String containing_part = active_parts.getContainingPart(name);

            if (!containing_part.empty() && containing_part != name)
                // TODO maybe use PartsTemporaryRename here?
                disk->moveDirectory(fs::path(relative_data_path) / source_dir / name,
                    fs::path(relative_data_path) / source_dir / ("inactive_" + name));
            else
                renamed_parts.addPart(name, "attaching_" + name);
        }
    }


    /// Try to rename all parts before attaching to prevent race with DROP DETACHED and another ATTACH.
    renamed_parts.tryRenameAll();

    /// Synchronously check that added parts exist and are not broken. We will write checksums.txt if it does not exist.
    LOG_DEBUG(log, "Checking parts");
    MutableDataPartsVector loaded_parts;
    loaded_parts.reserve(renamed_parts.old_and_new_names.size());

    for (const auto & [old_name, new_name] : renamed_parts.old_and_new_names)
    {
        LOG_DEBUG(log, "Checking part {}", new_name);

        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + old_name, name_to_disk[old_name]);
        MutableDataPartPtr part = createPart(old_name, single_disk_volume, source_dir + new_name);

        loadPartAndFixMetadataImpl(part);
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

    throw Exception(fmt::format("Cannot reserve {}, not enough space", ReadableSize(expected_size)), ErrorCodes::NOT_ENOUGH_SPACE);
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
        SpacePtr destination_ptr = getDestinationForMoveTTL(*move_ttl_entry, is_insert);
        if (!destination_ptr)
        {
            if (move_ttl_entry->destination_type == DataDestinationType::VOLUME)
                LOG_WARNING(log, "Would like to reserve space on volume '{}' by TTL rule of table '{}' but volume was not found or rule is not applicable at the moment",
                    move_ttl_entry->destination_name, log_name);
            else if (move_ttl_entry->destination_type == DataDestinationType::DISK)
                LOG_WARNING(log, "Would like to reserve space on disk '{}' by TTL rule of table '{}' but disk was not found or rule is not applicable at the moment",
                    move_ttl_entry->destination_name, log_name);
        }
        else
        {
            reservation = destination_ptr->reserve(expected_size);
            if (reservation)
                return reservation;
            else
                if (move_ttl_entry->destination_type == DataDestinationType::VOLUME)
                    LOG_WARNING(log, "Would like to reserve space on volume '{}' by TTL rule of table '{}' but there is not enough space",
                    move_ttl_entry->destination_name, log_name);
                else if (move_ttl_entry->destination_type == DataDestinationType::DISK)
                    LOG_WARNING(log, "Would like to reserve space on disk '{}' by TTL rule of table '{}' but there is not enough space",
                        move_ttl_entry->destination_name, log_name);
        }
    }

    // Prefer selected_disk
    if (selected_disk)
        reservation = selected_disk->reserve(expected_size);

    if (!reservation)
        reservation = getStoragePolicy()->reserve(expected_size, min_volume_index);

    return reservation;
}

SpacePtr MergeTreeData::getDestinationForMoveTTL(const TTLDescription & move_ttl, bool is_insert) const
{
    auto policy = getStoragePolicy();
    if (move_ttl.destination_type == DataDestinationType::VOLUME)
    {
        auto volume = policy->getVolumeByName(move_ttl.destination_name);

        if (!volume)
            return {};

        if (is_insert && !volume->perform_ttl_move_on_insert)
            return {};

        return volume;
    }
    else if (move_ttl.destination_type == DataDestinationType::DISK)
    {
        auto disk = policy->getDiskByName(move_ttl.destination_name);
        if (!disk)
            return {};

        auto volume = policy->getVolume(policy->getVolumeIndexByDisk(disk));
        if (!volume)
            return {};

        if (is_insert && !volume->perform_ttl_move_on_insert)
            return {};

        return disk;
    }
    else
        return {};
}

bool MergeTreeData::isPartInTTLDestination(const TTLDescription & ttl, const IMergeTreeDataPart & part) const
{
    auto policy = getStoragePolicy();
    if (ttl.destination_type == DataDestinationType::VOLUME)
    {
        for (const auto & disk : policy->getVolumeByName(ttl.destination_name)->getDisks())
            if (disk->getName() == part.volume->getDisk()->getName())
                return true;
    }
    else if (ttl.destination_type == DataDestinationType::DISK)
        return policy->getDiskByName(ttl.destination_name)->getName() == part.volume->getDisk()->getName();
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

MergeTreeData::DataParts MergeTreeData::getDataParts() const
{
    return getDataParts({DataPartState::Committed});
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVector() const
{
    return getDataPartsVector({DataPartState::Committed});
}

MergeTreeData::DataPartPtr MergeTreeData::getAnyPartInPartition(
    const String & partition_id, DataPartsLock & /*data_parts_lock*/) const
{
    auto it = data_parts_by_state_and_info.lower_bound(DataPartStateAndPartitionID{DataPartState::Committed, partition_id});

    if (it != data_parts_by_state_and_info.end() && (*it)->getState() == DataPartState::Committed && (*it)->info.partition_id == partition_id)
        return *it;

    return nullptr;
}


void MergeTreeData::Transaction::rollbackPartsToTemporaryState()
{
    if (!isEmpty())
    {
        WriteBufferFromOwnString buf;
        buf << " Rollbacking parts state to temporary and removing from working set:";
        for (const auto & part : precommitted_parts)
            buf << " " << part->relative_path;
        buf << ".";
        LOG_DEBUG(data.log, "Undoing transaction.{}", buf.str());

        data.removePartsFromWorkingSetImmediatelyAndSetTemporaryState(
            DataPartsVector(precommitted_parts.begin(), precommitted_parts.end()));
    }

    clear();
}

void MergeTreeData::Transaction::rollback()
{
    if (!isEmpty())
    {
        WriteBufferFromOwnString buf;
        buf << " Removing parts:";
        for (const auto & part : precommitted_parts)
            buf << " " << part->relative_path;
        buf << ".";
        LOG_DEBUG(data.log, "Undoing transaction.{}", buf.str());

        data.removePartsFromWorkingSet(
            DataPartsVector(precommitted_parts.begin(), precommitted_parts.end()),
            /* clear_without_timeout = */ true);
    }

    clear();
}

MergeTreeData::DataPartsVector MergeTreeData::Transaction::commit(MergeTreeData::DataPartsLock * acquired_parts_lock)
{
    DataPartsVector total_covered_parts;

    if (!isEmpty())
    {
        auto parts_lock = acquired_parts_lock ? MergeTreeData::DataPartsLock() : data.lockParts();
        auto * owing_parts_lock = acquired_parts_lock ? acquired_parts_lock : &parts_lock;

        auto current_time = time(nullptr);

        size_t add_bytes = 0;
        size_t add_rows = 0;
        size_t add_parts = 0;

        size_t reduce_bytes = 0;
        size_t reduce_rows = 0;
        size_t reduce_parts = 0;

        for (const DataPartPtr & part : precommitted_parts)
        {
            DataPartPtr covering_part;
            DataPartsVector covered_parts = data.getActivePartsToReplace(part->info, part->name, covering_part, *owing_parts_lock);
            if (covering_part)
            {
                LOG_WARNING(data.log, "Tried to commit obsolete part {} covered by {}", part->name, covering_part->getNameWithState());

                part->remove_time.store(0, std::memory_order_relaxed); /// The part will be removed without waiting for old_parts_lifetime seconds.
                data.modifyPartState(part, DataPartState::Outdated);
            }
            else
            {
                total_covered_parts.insert(total_covered_parts.end(), covered_parts.begin(), covered_parts.end());
                for (const DataPartPtr & covered_part : covered_parts)
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

                data.modifyPartState(part, DataPartState::Committed);
                data.addPartContributionToColumnAndSecondaryIndexSizes(part);
            }
        }
        data.decreaseDataVolume(reduce_bytes, reduce_rows, reduce_parts);
        data.increaseDataVolume(add_bytes, add_rows, add_parts);
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

bool MergeTreeData::mayBenefitFromIndexForIn(
    const ASTPtr & left_in_operand, ContextPtr, const StorageMetadataPtr & metadata_snapshot) const
{
    /// Make sure that the left side of the IN operator contain part of the key.
    /// If there is a tuple on the left side of the IN operator, at least one item of the tuple
    ///  must be part of the key (probably wrapped by a chain of some acceptable functions).
    const auto * left_in_operand_tuple = left_in_operand->as<ASTFunction>();
    const auto & index_wrapper_factory = MergeTreeIndexFactory::instance();
    if (left_in_operand_tuple && left_in_operand_tuple->name == "tuple")
    {
        for (const auto & item : left_in_operand_tuple->arguments->children)
        {
            if (isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(item, metadata_snapshot))
                return true;
            for (const auto & index : metadata_snapshot->getSecondaryIndices())
                if (index_wrapper_factory.get(index)->mayBenefitFromIndexForIn(item))
                    return true;
            for (const auto & projection : metadata_snapshot->getProjections())
            {
                if (projection.isPrimaryKeyColumnPossiblyWrappedInFunctions(item))
                    return true;
            }
        }
        /// The tuple itself may be part of the primary key, so check that as a last resort.
        if (isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(left_in_operand, metadata_snapshot))
            return true;
        for (const auto & projection : metadata_snapshot->getProjections())
        {
            if (projection.isPrimaryKeyColumnPossiblyWrappedInFunctions(left_in_operand))
                return true;
        }
        return false;
    }
    else
    {
        for (const auto & index : metadata_snapshot->getSecondaryIndices())
            if (index_wrapper_factory.get(index)->mayBenefitFromIndexForIn(left_in_operand))
                return true;

        for (const auto & projection : metadata_snapshot->getProjections())
        {
            if (projection.isPrimaryKeyColumnPossiblyWrappedInFunctions(left_in_operand))
                return true;
        }
        return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(left_in_operand, metadata_snapshot);
    }
}

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

static void selectBestProjection(
    const MergeTreeDataSelectExecutor & reader,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Names & required_columns,
    ProjectionCandidate & candidate,
    ContextPtr query_context,
    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks,
    const Settings & settings,
    const MergeTreeData::DataPartsVector & parts,
    ProjectionCandidate *& selected_candidate,
    size_t & min_sum_marks)
{
    MergeTreeData::DataPartsVector projection_parts;
    MergeTreeData::DataPartsVector normal_parts;
    for (const auto & part : parts)
    {
        const auto & projections = part->getProjectionParts();
        auto it = projections.find(candidate.desc->name);
        if (it != projections.end())
            projection_parts.push_back(it->second);
        else
            normal_parts.push_back(part);
    }

    if (projection_parts.empty())
        return;

    auto projection_result_ptr = reader.estimateNumMarksToRead(
        projection_parts,
        candidate.required_columns,
        metadata_snapshot,
        candidate.desc->metadata,
        query_info,
        query_context,
        settings.max_threads,
        max_added_blocks);

    if (projection_result_ptr->error())
        return;

    auto sum_marks = projection_result_ptr->marks();
    if (normal_parts.empty())
    {
        // All parts are projection parts which allows us to use in_order_optimization.
        // TODO It might be better to use a complete projection even with more marks to read.
        candidate.complete = true;
    }
    else
    {
        auto normal_result_ptr = reader.estimateNumMarksToRead(
            normal_parts,
            required_columns,
            metadata_snapshot,
            metadata_snapshot,
            query_info,
            query_context,
            settings.max_threads,
            max_added_blocks);

        if (normal_result_ptr->error())
            return;

        sum_marks += normal_result_ptr->marks();
        candidate.merge_tree_normal_select_result_ptr = normal_result_ptr;
    }
    candidate.merge_tree_projection_select_result_ptr = projection_result_ptr;

    // We choose the projection with least sum_marks to read.
    if (sum_marks < min_sum_marks)
    {
        selected_candidate = &candidate;
        min_sum_marks = sum_marks;
    }
}


Block MergeTreeData::getMinMaxCountProjectionBlock(
    const StorageMetadataPtr & metadata_snapshot,
    const Names & required_columns,
    const SelectQueryInfo & query_info,
    const DataPartsVector & parts,
    DataPartsVector & normal_parts,
    ContextPtr query_context) const
{
    if (!metadata_snapshot->minmax_count_projection)
        throw Exception(
            "Cannot find the definition of minmax_count projection but it's used in current query. It's a bug",
            ErrorCodes::LOGICAL_ERROR);

    auto block = metadata_snapshot->minmax_count_projection->sample_block.cloneEmpty();
    bool need_primary_key_max_column = false;
    const auto & primary_key_max_column_name = metadata_snapshot->minmax_count_projection->primary_key_max_column_name;
    if (!primary_key_max_column_name.empty())
    {
        need_primary_key_max_column = std::any_of(
            required_columns.begin(), required_columns.end(), [&](const auto & name) { return primary_key_max_column_name == name; });
    }

    auto minmax_count_columns = block.mutateColumns();
    auto insert = [](ColumnAggregateFunction & column, const Field & value)
    {
        auto func = column.getAggregateFunction();
        Arena & arena = column.createOrGetArena();
        size_t size_of_state = func->sizeOfData();
        size_t align_of_state = func->alignOfData();
        auto * place = arena.alignedAlloc(size_of_state, align_of_state);
        func->create(place);
        auto value_column = func->getReturnType()->createColumnConst(1, value)->convertToFullColumnIfConst();
        const auto * value_column_ptr = value_column.get();
        func->add(place, &value_column_ptr, 0, &arena);
        column.insertFrom(place);
    };

    ASTPtr expression_ast;
    Block virtual_columns_block = getBlockWithVirtualPartColumns(parts, false /* one_part */, true /* ignore_empty */);
    if (virtual_columns_block.rows() == 0)
        return {};

    // Generate valid expressions for filtering
    VirtualColumnUtils::prepareFilterBlockWithQuery(query_info.query, query_context, virtual_columns_block, expression_ast);
    if (expression_ast)
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, virtual_columns_block, query_context, expression_ast);

    size_t rows = virtual_columns_block.rows();
    const ColumnString & part_name_column = typeid_cast<const ColumnString &>(*virtual_columns_block.getByName("_part").column);
    size_t part_idx = 0;
    for (size_t row = 0; row < rows; ++row)
    {
        while (parts[part_idx]->name != part_name_column.getDataAt(row))
            ++part_idx;

        const auto & part = parts[part_idx];

        if (!part->minmax_idx->initialized)
            throw Exception("Found a non-empty part with uninitialized minmax_idx. It's a bug", ErrorCodes::LOGICAL_ERROR);

        if (need_primary_key_max_column && !part->index_granularity.hasFinalMark())
        {
            normal_parts.push_back(part);
            continue;
        }

        size_t pos = 0;
        size_t minmax_idx_size = part->minmax_idx->hyperrectangle.size();
        for (size_t i = 0; i < minmax_idx_size; ++i)
        {
            auto & min_column = assert_cast<ColumnAggregateFunction &>(*minmax_count_columns[pos++]);
            auto & max_column = assert_cast<ColumnAggregateFunction &>(*minmax_count_columns[pos++]);
            const auto & range = part->minmax_idx->hyperrectangle[i];
            insert(min_column, range.left);
            insert(max_column, range.right);
        }

        if (!primary_key_max_column_name.empty())
        {
            const auto & primary_key_column = *part->index[0];
            auto primary_key_column_size = primary_key_column.size();
            auto & min_column = assert_cast<ColumnAggregateFunction &>(*minmax_count_columns[pos++]);
            auto & max_column = assert_cast<ColumnAggregateFunction &>(*minmax_count_columns[pos++]);
            insert(min_column, primary_key_column[0]);
            insert(max_column, primary_key_column[primary_key_column_size - 1]);
        }

        {
            auto & column = assert_cast<ColumnAggregateFunction &>(*minmax_count_columns.back());
            auto func = column.getAggregateFunction();
            Arena & arena = column.createOrGetArena();
            size_t size_of_state = func->sizeOfData();
            size_t align_of_state = func->alignOfData();
            auto * place = arena.alignedAlloc(size_of_state, align_of_state);
            func->create(place);
            const AggregateFunctionCount & agg_count = assert_cast<const AggregateFunctionCount &>(*func);
            agg_count.set(place, part->rows_count);
            column.insertFrom(place);
        }
    }
    block.setColumns(std::move(minmax_count_columns));

    Block res;
    for (const auto & name : required_columns)
    {
        if (virtual_columns_block.has(name))
            res.insert(virtual_columns_block.getByName(name));
        else if (block.has(name))
            res.insert(block.getByName(name));
        else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot find column {} in minmax_count projection but query analysis still selects this projection. It's a bug",
                name);
    }
    return res;
}


bool MergeTreeData::getQueryProcessingStageWithAggregateProjection(
    ContextPtr query_context, const StorageMetadataPtr & metadata_snapshot, SelectQueryInfo & query_info) const
{
    const auto & settings = query_context->getSettingsRef();
    if (!settings.allow_experimental_projection_optimization || query_info.ignore_projections || query_info.is_projection_query)
        return false;

    const auto & query_ptr = query_info.original_query;

    if (auto * select = query_ptr->as<ASTSelectQuery>(); select)
    {
        // Currently projections don't support final yet.
        if (select->final())
            return false;

        // Currently projections don't support ARRAY JOIN yet.
        if (select->arrayJoinExpressionList().first)
            return false;
    }

    // Currently projections don't support sampling yet.
    if (settings.parallel_replicas_count > 1)
        return false;

    InterpreterSelectQuery select(
        query_ptr,
        query_context,
        SelectQueryOptions{QueryProcessingStage::WithMergeableState}.ignoreProjections().ignoreAlias(),
        query_info.sets /* prepared_sets */);
    const auto & analysis_result = select.getAnalysisResult();
    query_info.sets = std::move(select.getQueryAnalyzer()->getPreparedSets());

    bool can_use_aggregate_projection = true;
    /// If the first stage of the query pipeline is more complex than Aggregating - Expression - Filter - ReadFromStorage,
    /// we cannot use aggregate projection.
    if (analysis_result.join != nullptr || analysis_result.array_join != nullptr)
        can_use_aggregate_projection = false;

    /// Check if all needed columns can be provided by some aggregate projection. Here we also try
    /// to find expression matches. For example, suppose an aggregate projection contains a column
    /// named sum(x) and the given query also has an expression called sum(x), it's a match. This is
    /// why we need to ignore all aliases during projection creation and the above query planning.
    /// It's also worth noting that, sqrt(sum(x)) will also work because we can treat sum(x) as a
    /// required column.

    /// The ownership of ProjectionDescription is hold in metadata_snapshot which lives along with
    /// InterpreterSelect, thus we can store the raw pointer here.
    std::vector<ProjectionCandidate> candidates;
    NameSet keys;
    std::unordered_map<std::string_view, size_t> key_name_pos_map;
    size_t pos = 0;
    for (const auto & desc : select.getQueryAnalyzer()->aggregationKeys())
    {
        keys.insert(desc.name);
        key_name_pos_map.insert({desc.name, pos++});
    }
    auto actions_settings = ExpressionActionsSettings::fromSettings(settings);

    // All required columns should be provided by either current projection or previous actions
    // Let's traverse backward to finish the check.
    // TODO what if there is a column with name sum(x) and an aggregate sum(x)?
    auto rewrite_before_where =
        [&](ProjectionCandidate & candidate, const ProjectionDescription & projection,
            NameSet & required_columns, const Block & source_block, const Block & aggregates)
    {
        if (analysis_result.before_where)
        {
            candidate.where_column_name = analysis_result.where_column_name;
            candidate.remove_where_filter = analysis_result.remove_where_filter;
            candidate.before_where = analysis_result.before_where->clone();

            auto new_required_columns = candidate.before_where->foldActionsByProjection(
                required_columns,
                projection.sample_block_for_keys,
                candidate.where_column_name);
            if (new_required_columns.empty() && !required_columns.empty())
                return false;
            required_columns = std::move(new_required_columns);
            candidate.before_where->addAggregatesViaProjection(aggregates);
        }

        if (analysis_result.prewhere_info)
        {
            candidate.prewhere_info = analysis_result.prewhere_info;

            auto prewhere_actions = candidate.prewhere_info->prewhere_actions->clone();
            auto prewhere_required_columns = required_columns;
            // required_columns should not contain columns generated by prewhere
            for (const auto & column : prewhere_actions->getResultColumns())
                required_columns.erase(column.name);

            {
                // Prewhere_action should not add missing keys.
                auto new_prewhere_required_columns = prewhere_actions->foldActionsByProjection(
                        prewhere_required_columns, projection.sample_block_for_keys, candidate.prewhere_info->prewhere_column_name, false);
                if (new_prewhere_required_columns.empty() && !prewhere_required_columns.empty())
                    return false;
                prewhere_required_columns = std::move(new_prewhere_required_columns);
                candidate.prewhere_info->prewhere_actions = prewhere_actions;
            }

            if (candidate.prewhere_info->row_level_filter)
            {
                auto row_level_filter_actions = candidate.prewhere_info->row_level_filter->clone();
                auto new_prewhere_required_columns = row_level_filter_actions->foldActionsByProjection(
                    prewhere_required_columns, projection.sample_block_for_keys, candidate.prewhere_info->row_level_column_name, false);
                if (new_prewhere_required_columns.empty() && !prewhere_required_columns.empty())
                    return false;
                prewhere_required_columns = std::move(new_prewhere_required_columns);
                candidate.prewhere_info->row_level_filter = row_level_filter_actions;
            }

            if (candidate.prewhere_info->alias_actions)
            {
                auto alias_actions = candidate.prewhere_info->alias_actions->clone();
                auto new_prewhere_required_columns
                    = alias_actions->foldActionsByProjection(prewhere_required_columns, projection.sample_block_for_keys, {}, false);
                if (new_prewhere_required_columns.empty() && !prewhere_required_columns.empty())
                    return false;
                prewhere_required_columns = std::move(new_prewhere_required_columns);
                candidate.prewhere_info->alias_actions = alias_actions;
            }
            required_columns.insert(prewhere_required_columns.begin(), prewhere_required_columns.end());
        }

        bool match = true;
        for (const auto & column : required_columns)
        {
            /// There are still missing columns, fail to match
            if (!source_block.has(column))
            {
                match = false;
                break;
            }
        }
        return match;
    };

    auto virtual_block = getSampleBlockWithVirtualColumns();
    auto add_projection_candidate = [&](const ProjectionDescription & projection)
    {
        ProjectionCandidate candidate{};
        candidate.desc = &projection;

        auto sample_block = projection.sample_block;
        auto sample_block_for_keys = projection.sample_block_for_keys;
        for (const auto & column : virtual_block)
        {
            sample_block.insertUnique(column);
            sample_block_for_keys.insertUnique(column);
        }

        if (projection.type == ProjectionDescription::Type::Aggregate && analysis_result.need_aggregate && can_use_aggregate_projection)
        {
            bool match = true;
            Block aggregates;
            // Let's first check if all aggregates are provided by current projection
            for (const auto & aggregate : select.getQueryAnalyzer()->aggregates())
            {
                const auto * column = sample_block.findByName(aggregate.column_name);
                if (column)
                {
                    aggregates.insert(*column);
                }
                else
                {
                    match = false;
                    break;
                }
            }

            if (!match)
                return;

            // Check if all aggregation keys can be either provided by some action, or by current
            // projection directly. Reshape the `before_aggregation` action DAG so that it only
            // needs to provide aggregation keys, and the DAG of certain child might be substituted
            // by some keys in projection.
            candidate.before_aggregation = analysis_result.before_aggregation->clone();
            auto required_columns = candidate.before_aggregation->foldActionsByProjection(keys, sample_block_for_keys);

            // TODO Let's find out the exact required_columns for keys.
            if (required_columns.empty() && (!keys.empty() && !candidate.before_aggregation->getRequiredColumns().empty()))
                return;

            if (analysis_result.optimize_aggregation_in_order)
            {
                for (const auto & key : keys)
                {
                    auto actions_dag = analysis_result.before_aggregation->clone();
                    actions_dag->foldActionsByProjection({key}, sample_block_for_keys);
                    candidate.group_by_elements_actions.emplace_back(std::make_shared<ExpressionActions>(actions_dag, actions_settings));
                }
            }

            // Reorder aggregation keys and attach aggregates
            candidate.before_aggregation->reorderAggregationKeysForProjection(key_name_pos_map);
            candidate.before_aggregation->addAggregatesViaProjection(aggregates);

            if (rewrite_before_where(candidate, projection, required_columns, sample_block_for_keys, aggregates))
            {
                candidate.required_columns = {required_columns.begin(), required_columns.end()};
                for (const auto & aggregate : aggregates)
                    candidate.required_columns.push_back(aggregate.name);
                candidates.push_back(std::move(candidate));
            }
        }

        if (projection.type == ProjectionDescription::Type::Normal && (analysis_result.hasWhere() || analysis_result.hasPrewhere()))
        {
            const auto & actions
                = analysis_result.before_aggregation ? analysis_result.before_aggregation : analysis_result.before_order_by;
            NameSet required_columns;
            for (const auto & column : actions->getRequiredColumns())
                required_columns.insert(column.name);

            if (rewrite_before_where(candidate, projection, required_columns, sample_block, {}))
            {
                candidate.required_columns = {required_columns.begin(), required_columns.end()};
                candidates.push_back(std::move(candidate));
            }
        }
    };

    ProjectionCandidate * selected_candidate = nullptr;
    size_t min_sum_marks = std::numeric_limits<size_t>::max();
    if (metadata_snapshot->minmax_count_projection)
        add_projection_candidate(*metadata_snapshot->minmax_count_projection);
    std::optional<ProjectionCandidate> minmax_conut_projection_candidate;
    if (!candidates.empty())
    {
        minmax_conut_projection_candidate.emplace(std::move(candidates.front()));
        candidates.clear();
    }
    MergeTreeDataSelectExecutor reader(*this);
    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks;
    if (settings.select_sequential_consistency)
    {
        if (const StorageReplicatedMergeTree * replicated = dynamic_cast<const StorageReplicatedMergeTree *>(this))
            max_added_blocks = std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());
    }
    auto parts = getDataPartsVector();

    // If minmax_count_projection is a valid candidate, check its completeness.
    if (minmax_conut_projection_candidate)
    {
        DataPartsVector normal_parts;
        query_info.minmax_count_projection_block = getMinMaxCountProjectionBlock(
            metadata_snapshot, minmax_conut_projection_candidate->required_columns, query_info, parts, normal_parts, query_context);

        if (normal_parts.empty())
        {
            selected_candidate = &*minmax_conut_projection_candidate;
            selected_candidate->complete = true;
            min_sum_marks = query_info.minmax_count_projection_block.rows();
        }
        else
        {
            if (normal_parts.size() == parts.size())
            {
                // minmax_count_projection is useless.
            }
            else
            {
                auto normal_result_ptr = reader.estimateNumMarksToRead(
                    normal_parts,
                    analysis_result.required_columns,
                    metadata_snapshot,
                    metadata_snapshot,
                    query_info,
                    query_context,
                    settings.max_threads,
                    max_added_blocks);

                if (!normal_result_ptr->error())
                {
                    selected_candidate = &*minmax_conut_projection_candidate;
                    selected_candidate->merge_tree_normal_select_result_ptr = normal_result_ptr;
                    min_sum_marks = query_info.minmax_count_projection_block.rows() + normal_result_ptr->marks();
                }
            }

            // We cannot find a complete match of minmax_count_projection, add more projections to check.
            for (const auto & projection : metadata_snapshot->projections)
                add_projection_candidate(projection);
        }
    }
    else
    {
        for (const auto & projection : metadata_snapshot->projections)
            add_projection_candidate(projection);
    }

    // Let's select the best projection to execute the query.
    if (!candidates.empty())
    {
        query_info.merge_tree_select_result_ptr = reader.estimateNumMarksToRead(
            parts,
            analysis_result.required_columns,
            metadata_snapshot,
            metadata_snapshot,
            query_info,
            query_context,
            settings.max_threads,
            max_added_blocks);

        if (!query_info.merge_tree_select_result_ptr->error())
        {
            // Add 1 to base sum_marks so that we prefer projections even when they have equal number of marks to read.
            // NOTE: It is not clear if we need it. E.g. projections do not support skip index for now.
            auto sum_marks = query_info.merge_tree_select_result_ptr->marks() + 1;
            if (sum_marks < min_sum_marks)
            {
                selected_candidate = nullptr;
                min_sum_marks = sum_marks;
            }
        }

        /// Favor aggregate projections
        for (auto & candidate : candidates)
        {
            if (candidate.desc->type == ProjectionDescription::Type::Aggregate)
            {
                selectBestProjection(
                    reader,
                    metadata_snapshot,
                    query_info,
                    analysis_result.required_columns,
                    candidate,
                    query_context,
                    max_added_blocks,
                    settings,
                    parts,
                    selected_candidate,
                    min_sum_marks);
            }
        }

        /// Select the best normal projection.
        for (auto & candidate : candidates)
        {
            if (candidate.desc->type == ProjectionDescription::Type::Normal)
            {
                selectBestProjection(
                    reader,
                    metadata_snapshot,
                    query_info,
                    analysis_result.required_columns,
                    candidate,
                    query_context,
                    max_added_blocks,
                    settings,
                    parts,
                    selected_candidate,
                    min_sum_marks);
            }
        }
    }

    if (!selected_candidate)
        return false;
    else if (min_sum_marks == 0)
    {
        /// If selected_projection indicated an empty result set. Remember it in query_info but
        /// don't use projection to run the query, because projection pipeline with empty result
        /// set will not work correctly with empty_result_for_aggregation_by_empty_set.
        query_info.merge_tree_empty_result = true;
        return false;
    }

    if (selected_candidate->desc->type == ProjectionDescription::Type::Aggregate)
    {
        selected_candidate->aggregation_keys = select.getQueryAnalyzer()->aggregationKeys();
        selected_candidate->aggregate_descriptions = select.getQueryAnalyzer()->aggregates();
        selected_candidate->subqueries_for_sets
            = std::make_shared<SubqueriesForSets>(std::move(select.getQueryAnalyzer()->getSubqueriesForSets()));
    }

    query_info.projection = std::move(*selected_candidate);
    return true;
}


QueryProcessingStage::Enum MergeTreeData::getQueryProcessingStage(
    ContextPtr query_context,
    QueryProcessingStage::Enum to_stage,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info) const
{
    if (to_stage >= QueryProcessingStage::Enum::WithMergeableState)
    {
        if (getQueryProcessingStageWithAggregateProjection(query_context, metadata_snapshot, query_info))
        {
            if (query_info.projection->desc->type == ProjectionDescription::Type::Aggregate)
                return QueryProcessingStage::Enum::WithMergeableState;
        }
    }

    return QueryProcessingStage::Enum::FetchColumns;
}


MergeTreeData & MergeTreeData::checkStructureAndGetMergeTreeData(IStorage & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const
{
    MergeTreeData * src_data = dynamic_cast<MergeTreeData *>(&source_table);
    if (!src_data)
        throw Exception("Table " + source_table.getStorageID().getNameForLogs() +
                        " supports attachPartitionFrom only for MergeTree family of table engines."
                        " Got " + source_table.getName(), ErrorCodes::NOT_IMPLEMENTED);

    if (my_snapshot->getColumns().getAllPhysical().sizeOfDifference(src_snapshot->getColumns().getAllPhysical()))
        throw Exception("Tables have different structure", ErrorCodes::INCOMPATIBLE_COLUMNS);

    auto query_to_string = [] (const ASTPtr & ast)
    {
        return ast ? queryToString(ast) : "";
    };

    if (query_to_string(my_snapshot->getSortingKeyAST()) != query_to_string(src_snapshot->getSortingKeyAST()))
        throw Exception("Tables have different ordering", ErrorCodes::BAD_ARGUMENTS);

    if (query_to_string(my_snapshot->getPartitionKeyAST()) != query_to_string(src_snapshot->getPartitionKeyAST()))
        throw Exception("Tables have different partition key", ErrorCodes::BAD_ARGUMENTS);

    if (format_version != src_data->format_version)
        throw Exception("Tables have different format_version", ErrorCodes::BAD_ARGUMENTS);

    return *src_data;
}

MergeTreeData & MergeTreeData::checkStructureAndGetMergeTreeData(
    const StoragePtr & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const
{
    return checkStructureAndGetMergeTreeData(*source_table, src_snapshot, my_snapshot);
}

MergeTreeData::MutableDataPartPtr MergeTreeData::cloneAndLoadDataPartOnSameDisk(
    const MergeTreeData::DataPartPtr & src_part,
    const String & tmp_part_prefix,
    const MergeTreePartInfo & dst_part_info,
    const StorageMetadataPtr & metadata_snapshot)
{
    /// Check that the storage policy contains the disk where the src_part is located.
    bool does_storage_policy_allow_same_disk = false;
    for (const DiskPtr & disk : getStoragePolicy()->getDisks())
    {
        if (disk->getName() == src_part->volume->getDisk()->getName())
        {
            does_storage_policy_allow_same_disk = true;
            break;
        }
    }
    if (!does_storage_policy_allow_same_disk)
        throw Exception(
            "Could not clone and load part " + quoteString(src_part->getFullPath()) + " because disk does not belong to storage policy",
            ErrorCodes::BAD_ARGUMENTS);

    String dst_part_name = src_part->getNewName(dst_part_info);
    String tmp_dst_part_name = tmp_part_prefix + dst_part_name;

    auto reservation = reserveSpace(src_part->getBytesOnDisk(), src_part->volume->getDisk());
    auto disk = reservation->getDisk();
    String src_part_path = src_part->getFullRelativePath();
    String dst_part_path = relative_data_path + tmp_dst_part_name;

    if (disk->exists(dst_part_path))
        throw Exception("Part in " + fullPath(disk, dst_part_path) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    /// If source part is in memory, flush it to disk and clone it already in on-disk format
    if (auto src_part_in_memory = asInMemoryPart(src_part))
    {
        const auto & src_relative_data_path = src_part_in_memory->storage.relative_data_path;
        auto flushed_part_path = src_part_in_memory->getRelativePathForPrefix(tmp_part_prefix);
        src_part_in_memory->flushToDisk(src_relative_data_path, flushed_part_path, metadata_snapshot);
        src_part_path = fs::path(src_relative_data_path) / flushed_part_path / "";
    }

    LOG_DEBUG(log, "Cloning part {} to {}", fullPath(disk, src_part_path), fullPath(disk, dst_part_path));
    localBackup(disk, src_part_path, dst_part_path);
    disk->removeFileIfExists(fs::path(dst_part_path) / IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME);

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);
    auto dst_data_part = createPart(dst_part_name, dst_part_info, single_disk_volume, tmp_dst_part_name);

    dst_data_part->is_temp = true;

    dst_data_part->loadColumnsChecksumsIndexes(require_part_metadata, true);
    dst_data_part->modification_time = disk->getLastModified(dst_part_path).epochTime();
    return dst_data_part;
}

String MergeTreeData::getFullPathOnDisk(const DiskPtr & disk) const
{
    return disk->getPath() + relative_data_path;
}


DiskPtr MergeTreeData::getDiskForPart(const String & part_name, const String & additional_path) const
{
    const auto disks = getStoragePolicy()->getDisks();

    for (const DiskPtr & disk : disks)
        for (auto it = disk->iterateDirectory(relative_data_path + additional_path); it->isValid(); it->next())
            if (it->name() == part_name)
                return disk;

    return nullptr;
}


std::optional<String> MergeTreeData::getFullRelativePathForPart(const String & part_name, const String & additional_path) const
{
    auto disk = getDiskForPart(part_name, additional_path);
    if (disk)
        return relative_data_path + additional_path;
    return {};
}

Strings MergeTreeData::getDataPaths() const
{
    Strings res;
    auto disks = getStoragePolicy()->getDisks();
    for (const auto & disk : disks)
        res.push_back(getFullPathOnDisk(disk));
    return res;
}

MergeTreeData::PathsWithDisks MergeTreeData::getRelativeDataPathsWithDisks() const
{
    PathsWithDisks res;
    auto disks = getStoragePolicy()->getDisks();
    for (const auto & disk : disks)
        res.emplace_back(relative_data_path, disk);
    return res;
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
                 ? toString(partition_lit->value.get<UInt64>())
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
        else
            return id == partition_id;
    };
}

PartitionCommandsResultInfo MergeTreeData::freezePartition(
    const ASTPtr & partition_ast,
    const StorageMetadataPtr & metadata_snapshot,
    const String & with_name,
    ContextPtr local_context,
    TableLockHolder &)
{
    return freezePartitionsByMatcher(getPartitionMatcher(partition_ast, local_context), metadata_snapshot, with_name, local_context);
}

PartitionCommandsResultInfo MergeTreeData::freezeAll(
    const String & with_name,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    TableLockHolder &)
{
    return freezePartitionsByMatcher([] (const String &) { return true; }, metadata_snapshot, with_name, local_context);
}

PartitionCommandsResultInfo MergeTreeData::freezePartitionsByMatcher(
    MatcherFn matcher,
    const StorageMetadataPtr & metadata_snapshot,
    const String & with_name,
    ContextPtr local_context)
{
    String clickhouse_path = fs::canonical(local_context->getPath());
    String default_shadow_path = fs::path(clickhouse_path) / "shadow/";
    fs::create_directories(default_shadow_path);
    auto increment = Increment(fs::path(default_shadow_path) / "increment.txt").get(true);

    const String shadow_path = "shadow/";

    /// Acquire a snapshot of active data parts to prevent removing while doing backup.
    const auto data_parts = getDataParts();

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

        part->volume->getDisk()->createDirectories(backup_path);

        String src_part_path = part->getFullRelativePath();
        String backup_part_path = fs::path(backup_path) / relative_data_path / part->relative_path;
        if (auto part_in_memory = asInMemoryPart(part))
        {
            auto flushed_part_path = part_in_memory->getRelativePathForPrefix("tmp_freeze");
            part_in_memory->flushToDisk(relative_data_path, flushed_part_path, metadata_snapshot);
            src_part_path = fs::path(relative_data_path) / flushed_part_path / "";
        }

        localBackup(part->volume->getDisk(), src_part_path, backup_part_path);

        part->volume->getDisk()->removeFileIfExists(fs::path(backup_part_path) / IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME);

        part->is_frozen.store(true, std::memory_order_relaxed);
        result.push_back(PartitionCommandResultInfo{
            .partition_id = part->info.partition_id,
            .part_name = part->name,
            .backup_path = fs::path(part->volume->getDisk()->getPath()) / backup_path,
            .part_backup_path = fs::path(part->volume->getDisk()->getPath()) / backup_part_path,
            .backup_name = backup_name,
        });
        ++parts_processed;
    }

    LOG_DEBUG(log, "Freezed {} parts", parts_processed);
    return result;
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

PartitionCommandsResultInfo MergeTreeData::unfreezePartitionsByMatcher(MatcherFn matcher, const String & backup_name, ContextPtr)
{
    auto backup_path = fs::path("shadow") / escapeForFileName(backup_name) / relative_data_path;

    LOG_DEBUG(log, "Unfreezing parts by path {}", backup_path.generic_string());

    PartitionCommandsResultInfo result;

    for (const auto & disk : getStoragePolicy()->getDisks())
    {
        if (!disk->exists(backup_path))
            continue;

        for (auto it = disk->iterateDirectory(backup_path); it->isValid(); it->next())
        {
            const auto & partition_directory = it->name();

            /// Partition ID is prefix of part directory name: <partition id>_<rest of part directory name>
            auto found = partition_directory.find('_');
            if (found == std::string::npos)
                continue;
            auto partition_id = partition_directory.substr(0, found);

            if (!matcher(partition_id))
                continue;

            const auto & path = it->path();

            disk->removeRecursive(path);

            result.push_back(PartitionCommandResultInfo{
                .partition_id = partition_id,
                .part_name = partition_directory,
                .backup_path = disk->getPath() + backup_path.generic_string(),
                .part_backup_path = disk->getPath() + path,
                .backup_name = backup_name,
            });

            LOG_DEBUG(log, "Unfreezed part by path {}", disk->getPath() + path);
        }
    }

    LOG_DEBUG(log, "Unfreezed {} parts", result.size());

    return result;
}

bool MergeTreeData::canReplacePartition(const DataPartPtr & src_part) const
{
    const auto settings = getSettings();

    if (!settings->enable_mixed_granularity_parts || settings->index_granularity_bytes == 0)
    {
        if (!canUseAdaptiveGranularity() && src_part->index_granularity_info.is_adaptive)
            return false;
        if (canUseAdaptiveGranularity() && !src_part->index_granularity_info.is_adaptive)
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
    const MergeListEntry * merge_entry)
try
{
    auto table_id = getStorageID();
    auto part_log = getContext()->getPartLog(table_id.database_name);
    if (!part_log)
        return;

    PartLogElement part_log_elem;

    part_log_elem.event_type = type;

    part_log_elem.error = static_cast<UInt16>(execution_status.code);
    part_log_elem.exception = execution_status.message;

    // construct event_time and event_time_microseconds using the same time point
    // so that the two times will always be equal up to a precision of a second.
    const auto time_now = std::chrono::system_clock::now();
    part_log_elem.event_time = time_in_seconds(time_now);
    part_log_elem.event_time_microseconds = time_in_microseconds(time_now);

    /// TODO: Stop stopwatch in outer code to exclude ZK timings and so on
    part_log_elem.duration_ms = elapsed_ns / 1000000;

    part_log_elem.database_name = table_id.database_name;
    part_log_elem.table_name = table_id.table_name;
    part_log_elem.partition_id = MergeTreePartInfo::fromPartName(new_part_name, format_version).partition_id;
    part_log_elem.part_name = new_part_name;

    if (result_part)
    {
        part_log_elem.path_on_disk = result_part->getFullPath();
        part_log_elem.bytes_compressed_on_disk = result_part->getBytesOnDisk();
        part_log_elem.rows = result_part->rows_count;
    }

    part_log_elem.source_part_names.reserve(source_parts.size());
    for (const auto & source_part : source_parts)
        part_log_elem.source_part_names.push_back(source_part->name);

    if (merge_entry)
    {
        part_log_elem.rows_read = (*merge_entry)->rows_read;
        part_log_elem.bytes_read_uncompressed = (*merge_entry)->bytes_read_uncompressed;

        part_log_elem.rows = (*merge_entry)->rows_written;
        part_log_elem.bytes_uncompressed = (*merge_entry)->bytes_written_uncompressed;
        part_log_elem.peak_memory_usage = (*merge_entry)->memory_tracker.getPeak();
    }

    part_log->add(part_log_elem);
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
            throw Exception("Cannot move part '" + moving_part.part->name + "'. It's already moving.", ErrorCodes::LOGICAL_ERROR);
}

MergeTreeData::CurrentlyMovingPartsTagger::~CurrentlyMovingPartsTagger()
{
    std::lock_guard lock(data.moving_parts_mutex);
    for (const auto & moving_part : parts_to_move)
    {
        /// Something went completely wrong
        if (!data.currently_moving_parts.count(moving_part.part))
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

    assignee.scheduleMoveTask(ExecutableLambdaAdapter::create(
        [this, moving_tagger] () mutable
        {
            return moveParts(moving_tagger);
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

bool MergeTreeData::movePartsToSpace(const DataPartsVector & parts, SpacePtr space)
{
    if (parts_mover.moves_blocker.isCancelled())
        return false;

    auto moving_tagger = checkPartsForMove(parts, space);
    if (moving_tagger->parts_to_move.empty())
        return false;

    return moveParts(moving_tagger);
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
        if (currently_moving_parts.count(part))
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
            throw Exception("Move is not possible. Not enough space on '" + space->getName() + "'", ErrorCodes::NOT_ENOUGH_SPACE);

        auto reserved_disk = reservation->getDisk();

        if (reserved_disk->exists(relative_data_path + part->name))
            throw Exception(
                "Move is not possible: " + fullPath(reserved_disk, relative_data_path + part->name) + " already exists",
                ErrorCodes::DIRECTORY_ALREADY_EXISTS);

        if (currently_moving_parts.count(part) || partIsAssignedToBackgroundOperation(part))
            throw Exception(
                "Cannot move part '" + part->name + "' because it's participating in background process",
                ErrorCodes::PART_IS_TEMPORARILY_LOCKED);

        parts_to_move.emplace_back(part, std::move(reservation));
    }
    return std::make_shared<CurrentlyMovingPartsTagger>(std::move(parts_to_move), *this);
}

bool MergeTreeData::moveParts(const CurrentlyMovingPartsTaggerPtr & moving_tagger)
{
    LOG_INFO(log, "Got {} parts to move.", moving_tagger->parts_to_move.size());

    for (const auto & moving_part : moving_tagger->parts_to_move)
    {
        Stopwatch stopwatch;
        DataPartPtr cloned_part;

        auto write_part_log = [&](const ExecutionStatus & execution_status)
        {
            writePartLog(
                PartLogElement::Type::MOVE_PART,
                execution_status,
                stopwatch.elapsed(),
                moving_part.part->name,
                cloned_part,
                {moving_part.part},
                nullptr);
        };

        try
        {
            cloned_part = parts_mover.clonePart(moving_part);
            parts_mover.swapClonedPart(cloned_part);
            write_part_log({});
        }
        catch (...)
        {
            write_part_log(ExecutionStatus::fromCurrentException());
            if (cloned_part)
                cloned_part->remove();

            throw;
        }
    }
    return true;
}

bool MergeTreeData::partsContainSameProjections(const DataPartPtr & left, const DataPartPtr & right)
{
    if (left->getProjectionParts().size() != right->getProjectionParts().size())
        return false;
    for (const auto & [name, _] : left->getProjectionParts())
    {
        if (!right->hasProjection(name))
            return false;
    }
    return true;
}

bool MergeTreeData::canUsePolymorphicParts(const MergeTreeSettings & settings, String * out_reason) const
{
    if (!canUseAdaptiveGranularity())
    {
        if (out_reason && (settings.min_rows_for_wide_part != 0 || settings.min_bytes_for_wide_part != 0
            || settings.min_rows_for_compact_part != 0 || settings.min_bytes_for_compact_part != 0))
        {
            *out_reason = fmt::format(
                    "Table can't create parts with adaptive granularity, but settings"
                    " min_rows_for_wide_part = {}"
                    ", min_bytes_for_wide_part = {}"
                    ", min_rows_for_compact_part = {}"
                    ", min_bytes_for_compact_part = {}"
                    ". Parts with non-adaptive granularity can be stored only in Wide (default) format.",
                    settings.min_rows_for_wide_part,    settings.min_bytes_for_wide_part,
                    settings.min_rows_for_compact_part, settings.min_bytes_for_compact_part);
        }

        return false;
    }

    return true;
}

MergeTreeData::AlterConversions MergeTreeData::getAlterConversionsForPart(const MergeTreeDataPartPtr part) const
{
    MutationCommands commands = getFirstAlterMutationCommandsForPart(part);

    AlterConversions result{};
    for (const auto & command : commands)
        /// Currently we need explicit conversions only for RENAME alter
        /// all other conversions can be deduced from diff between part columns
        /// and columns in storage.
        if (command.type == MutationCommand::Type::RENAME_COLUMN)
            result.rename_map[command.rename_to] = command.column_name;

    return result;
}

MergeTreeData::WriteAheadLogPtr MergeTreeData::getWriteAheadLog()
{
    std::lock_guard lock(write_ahead_log_mutex);
    if (!write_ahead_log)
    {
        auto reservation = reserveSpace(getSettings()->write_ahead_log_max_bytes);
        write_ahead_log = std::make_shared<MergeTreeWriteAheadLog>(*this, reservation->getDisk());
    }

    return write_ahead_log;
}

NamesAndTypesList MergeTreeData::getVirtuals() const
{
    return NamesAndTypesList{
        NameAndTypePair("_part", std::make_shared<DataTypeString>()),
        NameAndTypePair("_part_index", std::make_shared<DataTypeUInt64>()),
        NameAndTypePair("_part_uuid", std::make_shared<DataTypeUUID>()),
        NameAndTypePair("_partition_id", std::make_shared<DataTypeString>()),
        NameAndTypePair("_partition_value", getPartitionValueType()),
        NameAndTypePair("_sample_factor", std::make_shared<DataTypeFloat64>()),
    };
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
    decreaseDataVolume(part->getBytesOnDisk(), part->rows_count, 1);
}

void MergeTreeData::increaseDataVolume(size_t bytes, size_t rows, size_t parts)
{
    total_active_size_bytes.fetch_add(bytes, std::memory_order_acq_rel);
    total_active_size_rows.fetch_add(rows, std::memory_order_acq_rel);
    total_active_size_parts.fetch_add(parts, std::memory_order_acq_rel);
}

void MergeTreeData::decreaseDataVolume(size_t bytes, size_t rows, size_t parts)
{
    total_active_size_bytes.fetch_sub(bytes, std::memory_order_acq_rel);
    total_active_size_rows.fetch_sub(rows, std::memory_order_acq_rel);
    total_active_size_parts.fetch_sub(parts, std::memory_order_acq_rel);
}

void MergeTreeData::setDataVolume(size_t bytes, size_t rows, size_t parts)
{
    total_active_size_bytes.store(bytes, std::memory_order_release);
    total_active_size_rows.store(rows, std::memory_order_release);
    total_active_size_parts.store(parts, std::memory_order_release);
}

bool MergeTreeData::insertQueryIdOrThrow(const String & query_id, size_t max_queries) const
{
    std::lock_guard lock(query_id_set_mutex);
    return insertQueryIdOrThrowNoLock(query_id, max_queries, lock);
}

bool MergeTreeData::insertQueryIdOrThrowNoLock(const String & query_id, size_t max_queries, const std::lock_guard<std::mutex> &) const
{
    if (query_id_set.find(query_id) != query_id_set.end())
        return false;
    if (query_id_set.size() >= max_queries)
        throw Exception(
            ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES, "Too many simultaneous queries for table {}. Maximum is: {}", log_name, max_queries);
    query_id_set.insert(query_id);
    return true;
}

void MergeTreeData::removeQueryId(const String & query_id) const
{
    std::lock_guard lock(query_id_set_mutex);
    removeQueryIdNoLock(query_id, lock);
}

void MergeTreeData::removeQueryIdNoLock(const String & query_id, const std::lock_guard<std::mutex> &) const
{
    if (query_id_set.find(query_id) == query_id_set.end())
        LOG_WARNING(log, "We have query_id removed but it's not recorded. This is a bug");
    else
        query_id_set.erase(query_id);
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
    auto min_bytes_to_rebalance_partition_over_jbod = getSettings()->min_bytes_to_rebalance_partition_over_jbod;
    if (tagger_ptr && min_bytes_to_rebalance_partition_over_jbod > 0 && part_size >= min_bytes_to_rebalance_partition_over_jbod)
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
            covered_parts.erase(
                std::remove_if(
                    covered_parts.begin(),
                    covered_parts.end(),
                    [min_bytes_to_rebalance_partition_over_jbod](const auto & part)
                    {
                        return !(part->isStoredOnDisk() && part->getBytesOnDisk() >= min_bytes_to_rebalance_partition_over_jbod);
                    }),
                covered_parts.end());

            // Include current submerging big parts which are not yet in `currently_submerging_big_parts`
            for (const auto & part : covered_parts)
                submerging_big_parts_from_partition.insert(part->name);

            for (const auto & part : getDataPartsStateRange(MergeTreeData::DataPartState::Committed))
            {
                if (part->isStoredOnDisk() && part->getBytesOnDisk() >= min_bytes_to_rebalance_partition_over_jbod
                    && part_info.partition_id == part->info.partition_id)
                {
                    auto name = part->volume->getDisk()->getName();
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
            LOG_DEBUG(log, log_str.str());

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
                    if (currently_submerging_big_parts.count(part))
                        LOG_WARNING(log, "currently_submerging_big_parts contains duplicates. JBOD might lose balance");
                    else
                        currently_submerging_big_parts.insert(part);
                }

                // Record submerging big parts in the tagger to clean them up.
                tagger_ptr->emplace(*this, part_name, std::move(covered_parts), log);
            }
        }
    }
    catch (...)
    {
        LOG_DEBUG(log, "JBOD balancer encounters an error. Fallback to random disk selection");
        tryLogCurrentException(log);
    }
    return reserved_space;
}

CurrentlySubmergingEmergingTagger::~CurrentlySubmergingEmergingTagger()
{
    std::lock_guard lock(storage.currently_submerging_emerging_mutex);

    for (const auto & part : submerging_parts)
    {
        if (!storage.currently_submerging_big_parts.count(part))
        {
            LOG_ERROR(log, "currently_submerging_big_parts doesn't contain part {} to erase. This is a bug", part->name);
            assert(false);
        }
        else
            storage.currently_submerging_big_parts.erase(part);
    }
    storage.currently_emerging_big_parts.erase(emerging_part_name);
}

}
