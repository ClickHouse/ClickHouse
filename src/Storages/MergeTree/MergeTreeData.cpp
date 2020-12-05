#include <Compression/CompressedReadBuffer.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/HexWriteBuffer.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/localBackup.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/Increment.h>
#include <Common/SimpleIncrement.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

#include <Poco/DirectoryIterator.h>

#include <boost/range/adaptor/filtered.hpp>
#include <boost/algorithm/string/join.hpp>

#include <algorithm>
#include <iomanip>
#include <optional>
#include <set>
#include <thread>
#include <typeinfo>
#include <typeindex>
#include <unordered_set>


namespace ProfileEvents
{
    extern const Event RejectedInserts;
    extern const Event DelayedInserts;
    extern const Event DelayedInsertsMilliseconds;
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
}


static void checkSampleExpression(const StorageInMemoryMetadata & metadata, bool allow_sampling_expression_not_in_primary_key)
{
    const auto & pk_sample_block = metadata.getPrimaryKey().sample_block;
    if (!pk_sample_block.has(metadata.sampling_key.column_names[0]) && !allow_sampling_expression_not_in_primary_key)
        throw Exception("Sampling expression must be present in the primary key", ErrorCodes::BAD_ARGUMENTS);
}

MergeTreeData::MergeTreeData(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    Context & context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool require_part_metadata_,
    bool attach,
    BrokenPartCallback broken_part_callback_)
    : IStorage(table_id_)
    , global_context(context_.getGlobalContext())
    , merging_params(merging_params_)
    , require_part_metadata(require_part_metadata_)
    , relative_data_path(relative_data_path_)
    , broken_part_callback(broken_part_callback_)
    , log_name(table_id_.getNameForLogs())
    , log(&Poco::Logger::get(log_name))
    , storage_settings(std::move(storage_settings_))
    , data_parts_by_info(data_parts_indexes.get<TagByInfo>())
    , data_parts_by_state_and_info(data_parts_indexes.get<TagByStateAndInfo>())
    , parts_mover(this)
{
    const auto settings = getSettings();
    allow_nullable_key = attach || settings->allow_nullable_key;

    if (relative_data_path.empty())
        throw Exception("MergeTree storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    /// Check sanity of MergeTreeSettings. Only when table is created.
    if (!attach)
        settings->sanityCheck(global_context.getSettingsRef());

    MergeTreeDataFormatVersion min_format_version(0);
    if (!date_column_name.empty())
    {
        try
        {
            checkPartitionKeyAndInitMinMax(metadata_.partition_key);
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
        checkSampleExpression(metadata_, attach || settings->compatibility_allow_sampling_expression_not_in_primary_key);
    }

    checkTTLExpressions(metadata_, metadata_);

    /// format_file always contained on any data path
    PathWithDisk version_file;
    /// Creating directories, if not exist.
    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
    {
        disk->createDirectories(path);
        disk->createDirectories(path + "detached");
        auto current_version_file_path = path + "format_version.txt";
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
        version_file = {relative_data_path + "format_version.txt", getStoragePolicy()->getAnyDisk()};

    bool version_file_exists = version_file.second->exists(version_file.first);

    // When data path or file not exists, ignore the format_version check
    if (!attach || !version_file_exists)
    {
        format_version = min_format_version;
        auto buf = version_file.second->writeFile(version_file.first);
        writeIntText(format_version.toUnderType(), *buf);
        if (global_context.getSettingsRef().fsync_metadata)
            buf->sync();
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
}

StoragePolicyPtr MergeTreeData::getStoragePolicy() const
{
    return global_context.getStoragePolicy(getSettings()->storage_policy);
}

static void checkKeyExpression(const ExpressionActions & expr, const Block & sample_block, const String & key_name, bool allow_nullable_key)
{
    for (const auto & action : expr.getActions())
    {
        if (action.node->type == ActionsDAG::ActionType::ARRAY_JOIN)
            throw Exception(key_name + " key cannot contain array joins", ErrorCodes::ILLEGAL_COLUMN);

        if (action.node->type == ActionsDAG::ActionType::FUNCTION)
        {
            IFunctionBase & func = *action.node->function_base;
            if (!func.isDeterministic())
                throw Exception(key_name + " key cannot contain non-deterministic functions, "
                    "but contains function " + func.getName(),
                    ErrorCodes::BAD_ARGUMENTS);
        }
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
                throw Exception("Primary key must be a prefix of the sorting key, but in position "
                    + toString(i) + " its column is " + pk_column + ", not " + sorting_key_column,
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
            auto syntax = TreeRewriter(global_context).analyze(added_key_column_expr_list, all_columns);
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
    const Context & context)
{
    ASTPtr combined_expr_list = key.expression_list_ast->clone();

    for (const auto & index : indices)
        for (const auto & index_expr : index.expression_list_ast->children)
            combined_expr_list->children.push_back(index_expr->clone());

    auto syntax_result = TreeRewriter(context).analyze(combined_expr_list, columns.getAllPhysical());
    return ExpressionAnalyzer(combined_expr_list, syntax_result, context).getActions(false);
}

}

ExpressionActionsPtr MergeTreeData::getPrimaryKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot) const
{
    return getCombinedIndicesExpression(metadata_snapshot->getPrimaryKey(), metadata_snapshot->getSecondaryIndices(), metadata_snapshot->getColumns(), global_context);
}

ExpressionActionsPtr MergeTreeData::getSortingKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot) const
{
    return getCombinedIndicesExpression(metadata_snapshot->getSortingKey(), metadata_snapshot->getSecondaryIndices(), metadata_snapshot->getColumns(), global_context);
}


void MergeTreeData::checkPartitionKeyAndInitMinMax(const KeyDescription & new_partition_key)
{
    if (new_partition_key.expression_list_ast->children.empty())
        return;

    checkKeyExpression(*new_partition_key.expression, new_partition_key.sample_block, "Partition", allow_nullable_key);

    /// Add all columns used in the partition key to the min-max index.
    const NamesAndTypesList & minmax_idx_columns_with_types = new_partition_key.expression->getRequiredColumnsWithTypes();
    minmax_idx_expr = std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(minmax_idx_columns_with_types));
    for (const NameAndTypePair & column : minmax_idx_columns_with_types)
    {
        minmax_idx_columns.emplace_back(column.name);
        minmax_idx_column_types.emplace_back(column.type);
    }

    /// Try to find the date column in columns used by the partition key (a common case).
    bool encountered_date_column = false;
    for (size_t i = 0; i < minmax_idx_column_types.size(); ++i)
    {
        if (typeid_cast<const DataTypeDate *>(minmax_idx_column_types[i].get()))
        {
            if (!encountered_date_column)
            {
                minmax_idx_date_column_pos = i;
                encountered_date_column = true;
            }
            else
            {
                /// There is more than one Date column in partition key and we don't know which one to choose.
                minmax_idx_date_column_pos = -1;
            }
        }
    }
    if (!encountered_date_column)
    {
        for (size_t i = 0; i < minmax_idx_column_types.size(); ++i)
        {
            if (typeid_cast<const DataTypeDateTime *>(minmax_idx_column_types[i].get()))
            {
                if (!encountered_date_column)
                {
                    minmax_idx_time_column_pos = i;
                    encountered_date_column = true;
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
                        " (must be of an integer type or of type Date or DateTime)", ErrorCodes::BAD_TYPE_OF_FIELD);
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


void MergeTreeData::loadDataParts(bool skip_sanity_checks)
{
    LOG_DEBUG(log, "Loading data parts");

    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto settings = getSettings();
    std::vector<std::pair<String, DiskPtr>> part_names_with_disks;
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

        for (const auto & [disk_name, disk] : global_context.getDisksMap())
        {
            if (defined_disk_names.count(disk_name) == 0 && disk->exists(relative_data_path))
            {
                for (const auto it = disk->iterateDirectory(relative_data_path); it->isValid(); it->next())
                {
                    MergeTreePartInfo part_info;
                    if (MergeTreePartInfo::tryParsePartName(it->name(), &part_info, format_version))
                        throw Exception("Part " + backQuote(it->name()) + " was found on disk " + backQuote(disk_name) + " which is not defined in the storage policy", ErrorCodes::UNKNOWN_DISK);
                }
            }
        }
    }

    /// Reversed order to load part from low priority disks firstly.
    /// Used for keep part on low priority disk if duplication found
    for (auto disk_it = disks.rbegin(); disk_it != disks.rend(); ++disk_it)
    {
        auto disk_ptr = *disk_it;
        for (auto it = disk_ptr->iterateDirectory(relative_data_path); it->isValid(); it->next())
        {
            /// Skip temporary directories.
            if (startsWith(it->name(), "tmp"))
                continue;

            part_names_with_disks.emplace_back(it->name(), disk_ptr);

            /// Create and correctly initialize global WAL object, if it's needed
            if (it->name() == MergeTreeWriteAheadLog::DEFAULT_WAL_FILE_NAME && settings->in_memory_parts_enable_wal)
            {
                write_ahead_log = std::make_shared<MergeTreeWriteAheadLog>(*this, disk_ptr, it->name());
                for (auto && part : write_ahead_log->restore(metadata_snapshot))
                    parts_from_wal.push_back(std::move(part));
            }
            else if (startsWith(it->name(), MergeTreeWriteAheadLog::WAL_FILE_NAME) && settings->in_memory_parts_enable_wal)
            {
                MergeTreeWriteAheadLog wal(*this, disk_ptr, it->name());
                for (auto && part : wal.restore(metadata_snapshot))
                    parts_from_wal.push_back(std::move(part));
            }
        }
    }

    auto part_lock = lockParts();
    data_parts_indexes.clear();

    if (part_names_with_disks.empty() && parts_from_wal.empty())
    {
        LOG_DEBUG(log, "There is no data parts");
        return;
    }

    /// Parallel loading of data parts.
    size_t num_threads = std::min(size_t(settings->max_part_loading_threads), part_names_with_disks.size());

    std::mutex mutex;

    DataPartsVector broken_parts_to_remove;
    DataPartsVector broken_parts_to_detach;
    size_t suspicious_broken_parts = 0;

    std::atomic<bool> has_adaptive_parts = false;
    std::atomic<bool> has_non_adaptive_parts = false;

    ThreadPool pool(num_threads);

    for (size_t i = 0; i < part_names_with_disks.size(); ++i)
    {
        pool.scheduleOrThrowOnError([&, i]
        {
            const auto & part_name = part_names_with_disks[i].first;
            const auto part_disk_ptr = part_names_with_disks[i].second;

            MergeTreePartInfo part_info;
            if (!MergeTreePartInfo::tryParsePartName(part_name, &part_info, format_version))
                return;

            auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, part_disk_ptr, 0);
            auto part = createPart(part_name, part_info, single_disk_volume, part_name);
            bool broken = false;

            String part_path = relative_data_path + "/" + part_name;
            String marker_path = part_path + "/" + IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME;
            if (part_disk_ptr->exists(marker_path))
            {
                LOG_WARNING(log, "Detaching stale part {}{}, which should have been deleted after a move. That can only happen after unclean restart of ClickHouse after move of a part having an operation blocking that stale copy of part.", getFullPathOnDisk(part_disk_ptr), part_name);
                std::lock_guard loading_lock(mutex);
                broken_parts_to_detach.push_back(part);
                ++suspicious_broken_parts;
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

            /// Ignore and possibly delete broken parts that can appear as a result of hard server restart.
            if (broken)
            {
                if (part->info.level == 0)
                {
                    /// It is impossible to restore level 0 parts.
                    LOG_ERROR(log, "Considering to remove broken part {}{} because it's impossible to repair.", getFullPathOnDisk(part_disk_ptr), part_name);
                    std::lock_guard loading_lock(mutex);
                    broken_parts_to_remove.push_back(part);
                }
                else
                {
                    /// Count the number of parts covered by the broken part. If it is at least two, assume that
                    /// the broken part was created as a result of merging them and we won't lose data if we
                    /// delete it.
                    size_t contained_parts = 0;

                    LOG_ERROR(log, "Part {}{} is broken. Looking for parts to replace it.", getFullPathOnDisk(part_disk_ptr), part_name);

                    for (const auto & [contained_name, contained_disk_ptr] : part_names_with_disks)
                    {
                        if (contained_name == part_name)
                            continue;

                        MergeTreePartInfo contained_part_info;
                        if (!MergeTreePartInfo::tryParsePartName(contained_name, &contained_part_info, format_version))
                            continue;

                        if (part->info.contains(contained_part_info))
                        {
                            LOG_ERROR(log, "Found part {}{}", getFullPathOnDisk(contained_disk_ptr), contained_name);
                            ++contained_parts;
                        }
                    }

                    if (contained_parts >= 2)
                    {
                        LOG_ERROR(log, "Considering to remove broken part {}{} because it covers at least 2 other parts", getFullPathOnDisk(part_disk_ptr), part_name);
                        std::lock_guard loading_lock(mutex);
                        broken_parts_to_remove.push_back(part);
                    }
                    else
                    {
                        LOG_ERROR(log, "Detaching broken part {}{} because it covers less than 2 parts. You need to resolve this manually", getFullPathOnDisk(part_disk_ptr), part_name);
                        std::lock_guard loading_lock(mutex);
                        broken_parts_to_detach.push_back(part);
                        ++suspicious_broken_parts;
                    }
                }

                return;
            }
            if (!part->index_granularity_info.is_adaptive)
                has_non_adaptive_parts.store(true, std::memory_order_relaxed);
            else
                has_adaptive_parts.store(true, std::memory_order_relaxed);

            part->modification_time = part_disk_ptr->getLastModified(relative_data_path + part_name).epochTime();
            /// Assume that all parts are Committed, covered parts will be detected and marked as Outdated later
            part->state = DataPartState::Committed;

            std::lock_guard loading_lock(mutex);
            if (!data_parts_indexes.insert(part).second)
                throw Exception("Part " + part->name + " already exists", ErrorCodes::DUPLICATE_DATA_PART);
        });
    }

    pool.wait();

    for (auto & part : parts_from_wal)
    {
        if (getActiveContainingPart(part->info, DataPartState::Committed, part_lock))
            continue;

        part->modification_time = time(nullptr);
        /// Assume that all parts are Committed, covered parts will be detected and marked as Outdated later
        part->state = DataPartState::Committed;

        if (!data_parts_indexes.insert(part).second)
            throw Exception("Part " + part->name + " already exists", ErrorCodes::DUPLICATE_DATA_PART);
    }

    if (has_non_adaptive_parts && has_adaptive_parts && !settings->enable_mixed_granularity_parts)
        throw Exception("Table contains parts with adaptive and non adaptive marks, but `setting enable_mixed_granularity_parts` is disabled", ErrorCodes::LOGICAL_ERROR);

    has_non_adaptive_index_granularity_parts = has_non_adaptive_parts;

    if (suspicious_broken_parts > settings->max_suspicious_broken_parts && !skip_sanity_checks)
        throw Exception("Suspiciously many (" + toString(suspicious_broken_parts) + ") broken parts to remove.",
            ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);

    for (auto & part : broken_parts_to_remove)
        part->remove();
    for (auto & part : broken_parts_to_detach)
        part->renameToDetached("");


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
        };

        (*prev_jt)->assertState({DataPartState::Committed});

        while (curr_jt != data_parts_by_state_and_info.end() && (*curr_jt)->state == DataPartState::Committed)
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

    calculateColumnSizesImpl();


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


void MergeTreeData::clearOldTemporaryDirectories(ssize_t custom_directories_lifetime_seconds)
{
    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock lock(clear_old_temporary_directories_mutex, std::defer_lock);
    if (!lock.try_lock())
        return;

    const auto settings = getSettings();
    time_t current_time = time(nullptr);
    ssize_t deadline = (custom_directories_lifetime_seconds >= 0)
        ? current_time - custom_directories_lifetime_seconds
        : current_time - settings->temporary_directories_lifetime.totalSeconds();

    /// Delete temporary directories older than a day.
    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
    {
        for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
        {
            if (startsWith(it->name(), "tmp_"))
            {
                try
                {
                    if (disk->isDirectory(it->path()) && isOldPartDirectory(disk, it->path(), deadline))
                    {
                        LOG_WARNING(log, "Removing temporary directory {}", fullPath(disk, it->path()));
                        disk->removeRecursive(it->path());
                    }
                }
                catch (const Poco::FileNotFoundException &)
                {
                    /// If the file is already deleted, do nothing.
                }
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
    if (auto part_log = global_context.getPartLog(table_id.database_name))
    {
        PartLogElement part_log_elem;

        part_log_elem.event_type = PartLogElement::REMOVE_PART;
        part_log_elem.event_time = time(nullptr);
        part_log_elem.duration_ms = 0;

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
}

void MergeTreeData::clearPartsFromFilesystem(const DataPartsVector & parts_to_remove)
{
    const auto settings = getSettings();
    if (parts_to_remove.size() > 1 && settings->max_part_removal_threads > 1 && parts_to_remove.size() > settings->concurrent_part_removal_threshold)
    {
        /// Parallel parts removal.

        size_t num_threads = std::min(size_t(settings->max_part_removal_threads), parts_to_remove.size());
        ThreadPool pool(num_threads);

        /// NOTE: Under heavy system load you may get "Cannot schedule a task" from ThreadPool.
        for (const DataPartPtr & part : parts_to_remove)
        {
            pool.scheduleOrThrowOnError([&]
            {
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
                LOG_DEBUG(log, "Removing from filesystem outdated WAL file " + it->name());
                disk_ptr->remove(relative_data_path + it->name());
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
        {
            ASTPtr literal = std::make_shared<ASTLiteral>(part->name);
            /// If another replica has already started drop, it's ok, no need to throw.
            dropPartition(literal, /* detach = */ false, /*drop_part = */ true, global_context, /* throw_if_noop = */ false);
        }
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
        global_context.dropCaches();

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
        global_context.dropCaches();

    LOG_TRACE(log, "dropAllData: removing data from filesystem.");

    /// Removing of each data part before recursive removal of directory is to speed-up removal, because there will be less number of syscalls.
    clearPartsFromFilesystem(all_parts);

    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
    {
        try
        {
            disk->removeRecursive(path);
        }
        catch (const Poco::FileNotFoundException &)
        {
            /// If the file is already deleted, log the error message and do nothing.
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    LOG_TRACE(log, "dropAllData: done.");
}

void MergeTreeData::dropIfEmpty()
{
    LOG_TRACE(log, "dropIfEmpty");

    auto lock = lockParts();

    if (!data_parts_by_info.empty())
        return;

    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
    {
        /// Non recursive, exception is thrown if there are more files.
        disk->remove(path + "format_version.txt");
        disk->remove(path + "detached");
        disk->remove(path);
    }
}

namespace
{

/// Conversion that is allowed for partition key.
/// Partition key should be serialized in the same way after conversion.
/// NOTE: The list is not complete.
bool isSafeForPartitionKeyConversion(const IDataType * from, const IDataType * to)
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

void MergeTreeData::checkAlterIsPossible(const AlterCommands & commands, const Settings & settings) const
{
    /// Check that needed transformations can be applied to the list of columns without considering type conversions.
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    if (!settings.allow_non_metadata_alters)
    {

        auto mutation_commands = commands.getMutationCommands(new_metadata, settings.materialize_ttl_after_modify, global_context);

        if (!mutation_commands.empty())
            throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "The following alter commands: '{}' will modify data on disk, but setting `allow_non_metadata_alters` is disabled", queryToString(mutation_commands.ast()));
    }
    commands.apply(new_metadata, global_context);

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

    for (const AlterCommand & command : commands)
    {
        /// Just validate partition expression
        if (command.partition)
        {
            getPartitionIDFromQuery(command.partition, global_context);
        }

        /// Some type changes for version column is allowed despite it's a part of sorting key
        if (command.type == AlterCommand::MODIFY_COLUMN && command.column_name == merging_params.version_column)
        {
            const IDataType * new_type = command.data_type.get();
            const IDataType * old_type = old_types[command.column_name];

            checkVersionColumnTypesConversion(old_type, new_type, command.column_name);

            /// No other checks required
            continue;
        }

        if (command.type == AlterCommand::MODIFY_ORDER_BY && !is_custom_partitioned)
        {
            throw Exception(
                "ALTER MODIFY ORDER BY is not supported for default-partitioned tables created with the old syntax",
                ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::MODIFY_SAMPLE_BY)
        {
            if (!is_custom_partitioned)
                throw Exception(
                    "ALTER MODIFY SAMPLE BY is not supported for default-partitioned tables created with the old syntax",
                    ErrorCodes::BAD_ARGUMENTS);

            checkSampleExpression(new_metadata, getSettings()->compatibility_allow_sampling_expression_not_in_primary_key);
        }
        if (command.type == AlterCommand::ADD_INDEX && !is_custom_partitioned)
        {
            throw Exception(
                "ALTER ADD INDEX is not supported for tables with the old syntax",
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
            dropped_columns.emplace(command.column_name);
        }
        else if (command.isRequireMutationStage(getInMemoryMetadata()))
        {
            /// This alter will override data on disk. Let's check that it doesn't
            /// modify immutable column.
            if (columns_alter_type_forbidden.count(command.column_name))
                throw Exception("ALTER of key column " + backQuoteIfNeed(command.column_name) + " is forbidden",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);

            if (columns_alter_type_check_safe_for_partition.count(command.column_name))
            {
                if (command.type == AlterCommand::MODIFY_COLUMN)
                {
                    auto it = old_types.find(command.column_name);

                    assert(it != old_types.end());
                    if (!isSafeForPartitionKeyConversion(it->second, command.data_type.get()))
                        throw Exception("ALTER of partition key column " + backQuoteIfNeed(command.column_name) + " from type "
                                + it->second->getName() + " to type " + command.data_type->getName()
                                + " is not safe because it can change the representation of partition key",
                            ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
                }
            }

            if (columns_alter_type_metadata_only.count(command.column_name))
            {
                if (command.type == AlterCommand::MODIFY_COLUMN)
                {
                    auto it = old_types.find(command.column_name);
                    assert(it != old_types.end());
                    throw Exception("ALTER of key column " + backQuoteIfNeed(command.column_name) + " from type "
                        + it->second->getName() + " to type " + command.data_type->getName() + " must be metadata-only",
                        ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
                }
            }
        }
    }

    checkProperties(new_metadata, old_metadata);
    checkTTLExpressions(new_metadata, old_metadata);

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
                checkStoragePolicy(global_context.getStoragePolicy(new_value.safeGet<String>()));
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
    const VolumePtr & volume, const String & relative_path) const
{
    if (type == MergeTreeDataPartType::COMPACT)
        return std::make_shared<MergeTreeDataPartCompact>(*this, name, part_info, volume, relative_path);
    else if (type == MergeTreeDataPartType::WIDE)
        return std::make_shared<MergeTreeDataPartWide>(*this, name, part_info, volume, relative_path);
    else if (type == MergeTreeDataPartType::IN_MEMORY)
        return std::make_shared<MergeTreeDataPartInMemory>(*this, name, part_info, volume, relative_path);
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
    const String & name, const VolumePtr & volume, const String & relative_path) const
{
    return createPart(name, MergeTreePartInfo::fromPartName(name, format_version), volume, relative_path);
}

MergeTreeData::MutableDataPartPtr MergeTreeData::createPart(
    const String & name, const MergeTreePartInfo & part_info,
    const VolumePtr & volume, const String & relative_path) const
{
    MergeTreeDataPartType type;
    auto full_path = relative_data_path + relative_path + "/";
    auto mrk_ext = MergeTreeIndexGranularityInfo::getMarksExtensionFromFilesystem(volume->getDisk(), full_path);

    if (mrk_ext)
        type = getPartTypeFromMarkExtension(*mrk_ext);
    else
    {
        /// Didn't find any mark file, suppose that part is empty.
        type = choosePartTypeOnDisk(0, 0);
    }

    return createPart(name, type, part_info, volume, relative_path);
}

void MergeTreeData::changeSettings(
        const ASTPtr & new_settings,
        TableLockHolder & /* table_lock_holder */)
{
    if (new_settings)
    {
        bool has_storage_policy_changed = false;

        const auto & new_changes = new_settings->as<const ASTSetQuery &>().changes;

        for (const auto & change : new_changes)
        {
            if (change.name == "storage_policy")
            {
                StoragePolicyPtr new_storage_policy = global_context.getStoragePolicy(change.value.safeGet<String>());
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
                        disk->createDirectories(relative_data_path + "detached");
                    }
                    /// FIXME how would that be done while reloading configuration???

                    has_storage_policy_changed = true;
                }
            }
        }

        MergeTreeSettings copy = *getSettings();
        copy.applyChanges(new_changes);

        copy.sanityCheck(global_context.getSettingsRef());

        storage_settings.set(std::make_unique<const MergeTreeSettings>(copy));
        StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
        new_metadata.setSettingsChanges(new_settings);
        setInMemoryMetadata(new_metadata);

        if (has_storage_policy_changed)
            startBackgroundMovesIfNeeded();
    }
}

PartitionCommandsResultInfo MergeTreeData::freezeAll(const String & with_name, const StorageMetadataPtr & metadata_snapshot, const Context & context, TableLockHolder &)
{
    return freezePartitionsByMatcher([] (const DataPartPtr &) { return true; }, metadata_snapshot, with_name, context);
}

void MergeTreeData::PartsTemporaryRename::addPart(const String & old_name, const String & new_name)
{
    old_and_new_names.push_back({old_name, new_name});
    for (const auto & [path, disk] : storage.getRelativeDataPathsWithDisks())
    {
        for (auto it = disk->iterateDirectory(path + source_dir); it->isValid(); it->next())
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
            const auto full_path = path + source_dir; /// for old_name
            disk->moveFile(full_path + old_name, full_path + new_name);
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
            const auto full_path = path + source_dir; /// for old_name
            disk->moveFile(full_path + new_name, full_path + old_name);
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
                throw Exception("Part " + new_part_name + " intersects previous part " + (*prev)->getNameWithState() +
                    ". It is a bug.", ErrorCodes::LOGICAL_ERROR);

            break;
        }

        begin = prev;
    }

    /// Go to the right.
    DataPartIteratorByStateAndInfo end = it_middle;
    while (end != committed_parts_range.end())
    {
        if ((*end)->info == new_part_info)
            throw Exception("Unexpected duplicate part " + (*end)->getNameWithState() + ". It is a bug.", ErrorCodes::LOGICAL_ERROR);

        if (!new_part_info.contains((*end)->info))
        {
            if ((*end)->info.contains(new_part_info))
            {
                out_covering_part = *end;
                return {};
            }

            if (!new_part_info.isDisjoint((*end)->info))
                throw Exception("Part " + new_part_name + " intersects next part " + (*end)->getNameWithState() +
                    ". It is a bug.", ErrorCodes::LOGICAL_ERROR);

            break;
        }

        ++end;
    }

    return DataPartsVector{begin, end};
}


bool MergeTreeData::renameTempPartAndAdd(MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction)
{
    if (out_transaction && &out_transaction->data != this)
        throw Exception("MergeTreeData::Transaction for one table cannot be used with another. It is a bug.",
            ErrorCodes::LOGICAL_ERROR);

    DataPartsVector covered_parts;
    {
        auto lock = lockParts();
        if (!renameTempPartAndReplace(part, increment, out_transaction, lock, &covered_parts))
            return false;
    }
    if (!covered_parts.empty())
        throw Exception("Added part " + part->name + " covers " + toString(covered_parts.size())
            + " existing part(s) (including " + covered_parts[0]->name + ")", ErrorCodes::LOGICAL_ERROR);

    return true;
}


bool MergeTreeData::renameTempPartAndReplace(
    MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction,
    std::unique_lock<std::mutex> & lock, DataPartsVector * out_covered_parts)
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

    auto it_duplicate = data_parts_by_info.find(part_info);
    if (it_duplicate != data_parts_by_info.end())
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

    /// All checks are passed. Now we can rename the part on disk.
    /// So, we maintain invariant: if a non-temporary part in filesystem then it is in data_parts
    ///
    /// If out_transaction is null, we commit the part to the active set immediately, else add it to the transaction.
    part->name = part_name;
    part->info = part_info;
    part->is_temp = false;
    part->state = DataPartState::PreCommitted;
    part->renameTo(part_name, true);

    auto part_it = data_parts_indexes.insert(part).first;

    if (out_transaction)
    {
        out_transaction->precommitted_parts.insert(part);
    }
    else
    {
        auto current_time = time(nullptr);
        for (const DataPartPtr & covered_part : covered_parts)
        {
            covered_part->remove_time.store(current_time, std::memory_order_relaxed);
            modifyPartState(covered_part, DataPartState::Outdated);
            removePartContributionToColumnSizes(covered_part);
        }

        modifyPartState(part_it, DataPartState::Committed);
        addPartContributionToColumnSizes(part);
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
    MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction)
{
    if (out_transaction && &out_transaction->data != this)
        throw Exception("MergeTreeData::Transaction for one table cannot be used with another. It is a bug.",
            ErrorCodes::LOGICAL_ERROR);

    DataPartsVector covered_parts;
    {
        auto lock = lockParts();
        renameTempPartAndReplace(part, increment, out_transaction, lock, &covered_parts);
    }
    return covered_parts;
}

void MergeTreeData::removePartsFromWorkingSet(const MergeTreeData::DataPartsVector & remove, bool clear_without_timeout, DataPartsLock & /*acquired_lock*/)
{
    auto remove_time = clear_without_timeout ? 0 : time(nullptr);

    for (const DataPartPtr & part : remove)
    {
        if (part->state == IMergeTreeDataPart::State::Committed)
            removePartContributionToColumnSizes(part);

        if (part->state == IMergeTreeDataPart::State::Committed || clear_without_timeout)
            part->remove_time.store(remove_time, std::memory_order_relaxed);

        if (part->state != IMergeTreeDataPart::State::Outdated)
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
                                                                               bool skip_intersecting_parts, DataPartsLock & lock)
{
    DataPartsVector parts_to_remove;

    if (drop_range.min_block > drop_range.max_block)
        return parts_to_remove;

    auto partition_range = getDataPartsPartitionRange(drop_range.partition_id);

    for (const DataPartPtr & part : partition_range)
    {
        if (part->info.partition_id != drop_range.partition_id)
            throw Exception("Unexpected partition_id of part " + part->name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);

        if (part->info.min_block < drop_range.min_block)
        {
            if (drop_range.min_block <= part->info.max_block)
            {
                /// Intersect left border
                String error = "Unexpected merged part " + part->name + " intersecting drop range " + drop_range.getPartName();
                if (!skip_intersecting_parts)
                    throw Exception(error, ErrorCodes::LOGICAL_ERROR);

                LOG_WARNING(log, error);
            }

            continue;
        }

        /// Stop on new parts
        if (part->info.min_block > drop_range.max_block)
            break;

        if (part->info.min_block <= drop_range.max_block && drop_range.max_block < part->info.max_block)
        {
            /// Intersect right border
            String error = "Unexpected merged part " + part->name + " intersecting drop range " + drop_range.getPartName();
            if (!skip_intersecting_parts)
                throw Exception(error, ErrorCodes::LOGICAL_ERROR);

            LOG_WARNING(log, error);
            continue;
        }

        if (part->state != DataPartState::Deleting)
            parts_to_remove.emplace_back(part);
    }

    removePartsFromWorkingSet(parts_to_remove, clear_without_timeout, lock);

    return parts_to_remove;
}

void MergeTreeData::forgetPartAndMoveToDetached(const MergeTreeData::DataPartPtr & part_to_detach, const String & prefix, bool
restore_covered)
{
    LOG_INFO(log, "Renaming {} to {}{} and forgiving it.", part_to_detach->relative_path, prefix, part_to_detach->name);

    auto lock = lockParts();

    auto it_part = data_parts_by_info.find(part_to_detach->info);
    if (it_part == data_parts_by_info.end())
        throw Exception("No such data part " + part_to_detach->getNameWithState(), ErrorCodes::NO_SUCH_DATA_PART);

    /// What if part_to_detach is a reference to *it_part? Make a new owner just in case.
    DataPartPtr part = *it_part;

    if (part->state == DataPartState::Committed)
        removePartContributionToColumnSizes(part);
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

            if (part->contains(**it) && is_appropriate_state((*it)->state))
            {
                /// Maybe, we must consider part level somehow
                if ((*it)->info.min_block != part->info.min_block)
                    update_error(it);

                if ((*it)->state != DataPartState::Committed)
                {
                    addPartContributionToColumnSizes(*it);
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

            if (!is_appropriate_state((*it)->state))
            {
                update_error(it);
                continue;
            }

            if ((*it)->info.min_block > pos)
                update_error(it);

            if ((*it)->state != DataPartState::Committed)
            {
                addPartContributionToColumnSizes(*it);
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

        auto it = data_parts_by_info.find(part->info);
        if (it == data_parts_by_info.end() || (*it).get() != part.get())
            throw Exception("Part " + part->name + " doesn't exist", ErrorCodes::LOGICAL_ERROR);

        part.reset();

        if (!((*it)->state == DataPartState::Outdated && it->unique()))
            return;

        modifyPartState(it, DataPartState::Deleting);
        part_to_delete = *it;
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
    size_t res = 0;
    {
        auto lock = lockParts();

        for (const auto & part : getDataPartsStateRange(DataPartState::Committed))
            res += part->getBytesOnDisk();
    }

    return res;
}


size_t MergeTreeData::getTotalActiveSizeInRows() const
{
    size_t res = 0;
    {
        auto lock = lockParts();

        for (const auto & part : getDataPartsStateRange(DataPartState::Committed))
            res += part->rows_count;
    }

    return res;
}


size_t MergeTreeData::getPartsCount() const
{
    auto lock = lockParts();

    size_t res = 0;
    for (const auto & part [[maybe_unused]] : getDataPartsStateRange(DataPartState::Committed))
        ++res;

    return res;
}


size_t MergeTreeData::getMaxPartsCountForPartition() const
{
    auto lock = lockParts();

    size_t res = 0;
    size_t cur_count = 0;
    const String * cur_partition_id = nullptr;

    for (const auto & part : getDataPartsStateRange(DataPartState::Committed))
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

    const size_t parts_count_in_partition = getMaxPartsCountForPartition();
    if (parts_count_in_partition < settings->parts_to_delay_insert)
        return;

    if (parts_count_in_partition >= settings->parts_to_throw_insert)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception("Too many parts (" + toString(parts_count_in_partition) + "). Merges are processing significantly slower than inserts.", ErrorCodes::TOO_MANY_PARTS);
    }

    const size_t max_k = settings->parts_to_throw_insert - settings->parts_to_delay_insert; /// always > 0
    const size_t k = 1 + parts_count_in_partition - settings->parts_to_delay_insert; /// from 1 to max_k
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

void MergeTreeData::throwInsertIfNeeded() const
{
    const auto settings = getSettings();
    const size_t parts_count_in_total = getPartsCount();
    if (parts_count_in_total >= settings->max_parts_in_total)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception("Too many parts (" + toString(parts_count_in_total) + ") in all partitions in total. This indicates wrong choice of partition key. The threshold can be modified with 'max_parts_in_total' setting in <merge_tree> element in config.xml or with per-table setting.", ErrorCodes::TOO_MANY_PARTS);
    }

    const size_t parts_count_in_partition = getMaxPartsCountForPartition();

    if (parts_count_in_partition >= settings->parts_to_throw_insert)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception("Too many parts (" + toString(parts_count_in_partition) + "). Merges are processing significantly slower than inserts.", ErrorCodes::TOO_MANY_PARTS);
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

            modifyPartState(original_active_part, DataPartState::DeleteOnDestroy);
            data_parts_indexes.erase(active_part_it);

            auto part_it = data_parts_indexes.insert(part_copy).first;
            modifyPartState(part_it, DataPartState::Committed);

            auto disk = original_active_part->volume->getDisk();
            String marker_path = original_active_part->getFullRelativePath() + IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME;
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

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorInPartition(MergeTreeData::DataPartState state, const String & partition_id)
{
    DataPartStateAndPartitionID state_with_partition{state, partition_id};

    auto lock = lockParts();
    return DataPartsVector(
        data_parts_by_state_and_info.lower_bound(state_with_partition),
        data_parts_by_state_and_info.upper_bound(state_with_partition));
}


MergeTreeData::DataPartPtr MergeTreeData::getPartIfExists(const MergeTreePartInfo & part_info, const MergeTreeData::DataPartStates & valid_states)
{
    auto lock = lockParts();

    auto it = data_parts_by_info.find(part_info);
    if (it == data_parts_by_info.end())
        return nullptr;

    for (auto state : valid_states)
    {
        if ((*it)->state == state)
            return *it;
    }

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

void MergeTreeData::calculateColumnSizesImpl()
{
    column_sizes.clear();

    /// Take into account only committed parts
    auto committed_parts_range = getDataPartsStateRange(DataPartState::Committed);
    for (const auto & part : committed_parts_range)
        addPartContributionToColumnSizes(part);
}

void MergeTreeData::addPartContributionToColumnSizes(const DataPartPtr & part)
{
    for (const auto & column : part->getColumns())
    {
        ColumnSize & total_column_size = column_sizes[column.name];
        ColumnSize part_column_size = part->getColumnSize(column.name, *column.type);
        total_column_size.add(part_column_size);
    }
}

void MergeTreeData::removePartContributionToColumnSizes(const DataPartPtr & part)
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
}


PartitionCommandsResultInfo MergeTreeData::freezePartition(const ASTPtr & partition_ast, const StorageMetadataPtr & metadata_snapshot, const String & with_name, const Context & context, TableLockHolder &)
{
    std::optional<String> prefix;
    String partition_id;

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// Month-partitioning specific - partition value can represent a prefix of the partition to freeze.
        if (const auto * partition_lit = partition_ast->as<ASTPartition &>().value->as<ASTLiteral>())
            prefix = partition_lit->value.getType() == Field::Types::UInt64
                ? toString(partition_lit->value.get<UInt64>())
                : partition_lit->value.safeGet<String>();
        else
            partition_id = getPartitionIDFromQuery(partition_ast, context);
    }
    else
        partition_id = getPartitionIDFromQuery(partition_ast, context);

    if (prefix)
        LOG_DEBUG(log, "Freezing parts with prefix {}", *prefix);
    else
        LOG_DEBUG(log, "Freezing parts with partition ID {}", partition_id);


    return freezePartitionsByMatcher(
        [&prefix, &partition_id](const DataPartPtr & part)
        {
            if (prefix)
                return startsWith(part->info.partition_id, *prefix);
            else
                return part->info.partition_id == partition_id;
        },
        metadata_snapshot,
        with_name,
        context);
}

void MergeTreeData::checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & settings) const
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
                /// We able to parse it
                MergeTreePartInfo::fromPartName(part_name, format_version);
            }
            else
            {
                /// We able to parse it
                getPartitionIDFromQuery(command.partition, global_context);
            }
        }
    }
}

void MergeTreeData::checkPartitionCanBeDropped(const ASTPtr & partition)
{
    const String partition_id = getPartitionIDFromQuery(partition, global_context);
    auto parts_to_remove = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);

    UInt64 partition_size = 0;

    for (const auto & part : parts_to_remove)
        partition_size += part->getBytesOnDisk();

    auto table_id = getStorageID();
    global_context.checkPartitionCanBeDropped(table_id.database_name, table_id.table_name, partition_size);
}

void MergeTreeData::checkPartCanBeDropped(const ASTPtr & part_ast)
{
    String part_name = part_ast->as<ASTLiteral &>().value.safeGet<String>();
    auto part = getPartIfExists(part_name, {MergeTreeDataPartState::Committed});
    if (!part)
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No part {} in committed state", part_name);

    auto table_id = getStorageID();
    global_context.checkPartitionCanBeDropped(table_id.database_name, table_id.table_name, part->getBytesOnDisk());
}

void MergeTreeData::movePartitionToDisk(const ASTPtr & partition, const String & name, bool moving_part, const Context & context)
{
    String partition_id;

    if (moving_part)
        partition_id = partition->as<ASTLiteral &>().value.safeGet<String>();
    else
        partition_id = getPartitionIDFromQuery(partition, context);

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


void MergeTreeData::movePartitionToVolume(const ASTPtr & partition, const String & name, bool moving_part, const Context & context)
{
    String partition_id;

    if (moving_part)
        partition_id = partition->as<ASTLiteral &>().value.safeGet<String>();
    else
        partition_id = getPartitionIDFromQuery(partition, context);

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
        throw Exception("Nothing to move", ErrorCodes::NO_SUCH_DATA_PART);

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

void MergeTreeData::fetchPartition(const ASTPtr & /*partition*/, const StorageMetadataPtr & /*metadata_snapshot*/, const String & /*from*/, const Context & /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "FETCH PARTITION is not supported by storage {}", getName());
}

Pipe MergeTreeData::alterPartition(
    const StorageMetadataPtr & metadata_snapshot,
    const PartitionCommands & commands,
    const Context & query_context)
{
    PartitionCommandsResultInfo result;
    for (const PartitionCommand & command : commands)
    {
        PartitionCommandsResultInfo current_command_results;
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
                if (command.part)
                    checkPartCanBeDropped(command.partition);
                else
                    checkPartitionCanBeDropped(command.partition);
                dropPartition(command.partition, command.detach, command.part, query_context);
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
                        checkPartitionCanBeDropped(command.partition);
                        String dest_database = query_context.resolveDatabase(command.to_database);
                        auto dest_storage = DatabaseCatalog::instance().getTable({dest_database, command.to_table}, query_context);
                        movePartitionToTable(dest_storage, command.partition, query_context);
                        break;
                }
            }
            break;

            case PartitionCommand::REPLACE_PARTITION:
            {
                checkPartitionCanBeDropped(command.partition);
                String from_database = query_context.resolveDatabase(command.from_database);
                auto from_storage = DatabaseCatalog::instance().getTable({from_database, command.from_table}, query_context);
                replacePartitionFrom(from_storage, command.partition, command.replace, query_context);
            }
            break;

            case PartitionCommand::FETCH_PARTITION:
                fetchPartition(command.partition, metadata_snapshot, command.from_zookeeper_path, query_context);
                break;

            case PartitionCommand::FREEZE_PARTITION:
            {
                auto lock = lockForShare(query_context.getCurrentQueryId(), query_context.getSettingsRef().lock_acquire_timeout);
                current_command_results = freezePartition(command.partition, metadata_snapshot, command.with_name, query_context, lock);
            }
            break;

            case PartitionCommand::FREEZE_ALL_PARTITIONS:
            {
                auto lock = lockForShare(query_context.getCurrentQueryId(), query_context.getSettingsRef().lock_acquire_timeout);
                current_command_results = freezeAll(command.with_name, metadata_snapshot, query_context, lock);
            }
            break;
        }
        for (auto & command_result : current_command_results)
            command_result.command_type = command.typeToString();
        result.insert(result.end(), current_command_results.begin(), current_command_results.end());
    }

    if (query_context.getSettingsRef().alter_partition_verbose_result)
        return convertCommandsResultToSource(result);

    return {};
}

String MergeTreeData::getPartitionIDFromQuery(const ASTPtr & ast, const Context & context) const
{
    const auto & partition_ast = ast->as<ASTPartition &>();

    if (!partition_ast.value)
        return partition_ast.id;

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// Month-partitioning specific - partition ID can be passed in the partition value.
        const auto * partition_lit = partition_ast.value->as<ASTLiteral>();
        if (partition_lit && partition_lit->value.getType() == Field::Types::String)
        {
            String partition_id = partition_lit->value.get<String>();
            if (partition_id.size() != 6 || !std::all_of(partition_id.begin(), partition_id.end(), isNumericASCII))
                throw Exception(
                    "Invalid partition format: " + partition_id + ". Partition should consist of 6 digits: YYYYMM",
                    ErrorCodes::INVALID_PARTITION_VALUE);
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

    const FormatSettings format_settings;
    Row partition_row(fields_count);

    if (fields_count)
    {
        ReadBufferFromMemory left_paren_buf("(", 1);
        ReadBufferFromMemory fields_buf(partition_ast.fields_str.data(), partition_ast.fields_str.size());
        ReadBufferFromMemory right_paren_buf(")", 1);
        ConcatReadBuffer buf({&left_paren_buf, &fields_buf, &right_paren_buf});

        auto input_stream = FormatFactory::instance().getInput("Values", buf, metadata_snapshot->getPartitionKey().sample_block, context, context.getSettingsRef().max_block_size);

        auto block = input_stream->read();
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

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVector(const DataPartStates & affordable_states, DataPartStateVector * out_states) const
{
    DataPartsVector res;
    DataPartsVector buf;
    {
        auto lock = lockParts();

        for (auto state : affordable_states)
        {
            std::swap(buf, res);
            res.clear();

            auto range = getDataPartsStateRange(state);
            std::merge(range.begin(), range.end(), buf.begin(), buf.end(), std::back_inserter(res), LessDataPart());
        }

        if (out_states != nullptr)
        {
            out_states->resize(res.size());
            for (size_t i = 0; i < res.size(); ++i)
                (*out_states)[i] = res[i]->state;
        }
    }

    return res;
}

MergeTreeData::DataPartsVector MergeTreeData::getAllDataPartsVector(MergeTreeData::DataPartStateVector * out_states) const
{
    DataPartsVector res;
    {
        auto lock = lockParts();
        res.assign(data_parts_by_info.begin(), data_parts_by_info.end());

        if (out_states != nullptr)
        {
            out_states->resize(res.size());
            for (size_t i = 0; i < res.size(); ++i)
                (*out_states)[i] = res[i]->state;
        }
    }

    return res;
}

std::vector<DetachedPartInfo>
MergeTreeData::getDetachedParts() const
{
    std::vector<DetachedPartInfo> res;

    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
    {
        for (auto it = disk->iterateDirectory(path + "detached"); it->isValid(); it->next())
        {
            res.emplace_back();
            auto & part = res.back();

            DetachedPartInfo::tryParseDetachedPartName(it->name(), part, format_version);
            part.disk = disk->getName();
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

void MergeTreeData::dropDetached(const ASTPtr & partition, bool part, const Context & context)
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
        String partition_id = getPartitionIDFromQuery(partition, context);
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
        disk->removeRecursive(path + "detached/" + new_name + "/");
        LOG_DEBUG(log, "Dropped detached part {}", old_name);
        old_name.clear();
    }
}

MergeTreeData::MutableDataPartsVector MergeTreeData::tryLoadPartsToAttach(const ASTPtr & partition, bool attach_part,
        const Context & context, PartsTemporaryRename & renamed_parts)
{
    String source_dir = "detached/";

    std::map<String, DiskPtr> name_to_disk;
    /// Let's compose a list of parts that should be added.
    if (attach_part)
    {
        String part_id = partition->as<ASTLiteral &>().value.safeGet<String>();
        validateDetachedPartName(part_id);
        renamed_parts.addPart(part_id, "attaching_" + part_id);
        if (MergeTreePartInfo::tryParsePartName(part_id, nullptr, format_version))
            name_to_disk[part_id] = getDiskForPart(part_id, source_dir);
    }
    else
    {
        String partition_id = getPartitionIDFromQuery(partition, context);
        LOG_DEBUG(log, "Looking for parts for partition {} in {}", partition_id, source_dir);
        ActiveDataPartSet active_parts(format_version);

        const auto disks = getStoragePolicy()->getDisks();
        for (const auto & disk : disks)
        {
            for (auto it = disk->iterateDirectory(relative_data_path + source_dir); it->isValid(); it->next())
            {
                const String & name = it->name();
                MergeTreePartInfo part_info;
                // TODO what if name contains "_tryN" suffix?
                /// Parts with prefix in name (e.g. attaching_1_3_3_0, deleting_1_3_3_0) will be ignored
                if (!MergeTreePartInfo::tryParsePartName(name, &part_info, format_version)
                    || part_info.partition_id != partition_id)
                {
                    continue;
                }
                LOG_DEBUG(log, "Found part {}", name);
                active_parts.add(name);
                name_to_disk[name] = disk;
            }
        }
        LOG_DEBUG(log, "{} of them are active", active_parts.size());
        /// Inactive parts rename so they can not be attached in case of repeated ATTACH.
        for (const auto & [name, disk] : name_to_disk)
        {
            String containing_part = active_parts.getContainingPart(name);
            if (!containing_part.empty() && containing_part != name)
            {
                // TODO maybe use PartsTemporaryRename here?
                disk->moveDirectory(relative_data_path + source_dir + name, relative_data_path + source_dir + "inactive_" + name);
            }
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

    for (const auto & part_names : renamed_parts.old_and_new_names)
    {
        LOG_DEBUG(log, "Checking part {}", part_names.second);
        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_names.first, name_to_disk[part_names.first], 0);
        MutableDataPartPtr part = createPart(part_names.first, single_disk_volume, source_dir + part_names.second);
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
    auto reservation = getStoragePolicy()->reserve(expected_size);
    return checkAndReturnReservation(expected_size, std::move(reservation));
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
    bool is_insert) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    ReservationPtr reservation = tryReserveSpacePreferringTTLRules(metadata_snapshot, expected_size, ttl_infos, time_of_move, min_volume_index, is_insert);

    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeData::tryReserveSpacePreferringTTLRules(
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 expected_size,
    const IMergeTreeDataPart::TTLInfos & ttl_infos,
    time_t time_of_move,
    size_t min_volume_index,
    bool is_insert) const
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

    return global_context.chooseCompressionCodec(
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

    if (it != data_parts_by_state_and_info.end() && (*it)->state == DataPartState::Committed && (*it)->info.partition_id == partition_id)
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
                    data.modifyPartState(covered_part, DataPartState::Outdated);
                    data.removePartContributionToColumnSizes(covered_part);
                }

                data.modifyPartState(part, DataPartState::Committed);
                data.addPartContributionToColumnSizes(part);
            }
        }
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

    for (const auto & name : minmax_idx_columns)
        if (column_name == name)
            return true;

    if (const auto * func = node->as<ASTFunction>())
        if (func->arguments->children.size() == 1)
            return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(func->arguments->children.front(), metadata_snapshot);

    return false;
}

bool MergeTreeData::mayBenefitFromIndexForIn(
    const ASTPtr & left_in_operand, const Context &, const StorageMetadataPtr & metadata_snapshot) const
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
        }
        /// The tuple itself may be part of the primary key, so check that as a last resort.
        return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(left_in_operand, metadata_snapshot);
    }
    else
    {
        for (const auto & index : metadata_snapshot->getSecondaryIndices())
            if (index_wrapper_factory.get(index)->mayBenefitFromIndexForIn(left_in_operand))
                return true;

        return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(left_in_operand, metadata_snapshot);
    }
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
        src_part_path = src_relative_data_path + flushed_part_path + "/";
    }

    LOG_DEBUG(log, "Cloning part {} to {}", fullPath(disk, src_part_path), fullPath(disk, dst_part_path));
    localBackup(disk, src_part_path, dst_part_path);
    disk->removeIfExists(dst_part_path + "/" + IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME);

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

PartitionCommandsResultInfo MergeTreeData::freezePartitionsByMatcher(MatcherFn matcher, const StorageMetadataPtr & metadata_snapshot, const String & with_name, const Context & context)
{
    String clickhouse_path = Poco::Path(context.getPath()).makeAbsolute().toString();
    String default_shadow_path = clickhouse_path + "shadow/";
    Poco::File(default_shadow_path).createDirectories();
    auto increment = Increment(default_shadow_path + "increment.txt").get(true);

    const String shadow_path = "shadow/";

    /// Acquire a snapshot of active data parts to prevent removing while doing backup.
    const auto data_parts = getDataParts();

    String backup_name = (!with_name.empty() ? escapeForFileName(with_name) : toString(increment));

    PartitionCommandsResultInfo result;

    size_t parts_processed = 0;
    for (const auto & part : data_parts)
    {
        if (!matcher(part))
            continue;

        part->volume->getDisk()->createDirectories(shadow_path);

        String backup_path = shadow_path + backup_name + "/";

        LOG_DEBUG(log, "Freezing part {} snapshot will be placed at {}", part->name, backup_path);

        String backup_part_path = backup_path + relative_data_path + part->relative_path;
        if (auto part_in_memory = asInMemoryPart(part))
            part_in_memory->flushToDisk(backup_path + relative_data_path, part->relative_path, metadata_snapshot);
        else
            localBackup(part->volume->getDisk(), part->getFullRelativePath(), backup_part_path);

        part->volume->getDisk()->removeIfExists(backup_part_path + "/" + IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME);

        part->is_frozen.store(true, std::memory_order_relaxed);
        result.push_back(PartitionCommandResultInfo{
            .partition_id = part->info.partition_id,
            .part_name = part->name,
            .backup_path = part->volume->getDisk()->getPath() + backup_path,
            .part_backup_path = part->volume->getDisk()->getPath() + backup_part_path,
            .backup_name = backup_name,
        });
        ++parts_processed;
    }

    LOG_DEBUG(log, "Freezed {} parts", parts_processed);
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
    auto part_log = global_context.getPartLog(table_id.database_name);
    if (!part_log)
        return;

    PartLogElement part_log_elem;

    part_log_elem.event_type = type;

    part_log_elem.error = static_cast<UInt16>(execution_status.code);
    part_log_elem.exception = execution_status.message;

    part_log_elem.event_time = time(nullptr);
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

bool MergeTreeData::selectPartsAndMove()
{
    if (parts_mover.moves_blocker.isCancelled())
        return false;

    auto moving_tagger = selectPartsForMove();
    if (moving_tagger->parts_to_move.empty())
        return false;

    return moveParts(std::move(moving_tagger));
}

std::optional<JobAndPool> MergeTreeData::getDataMovingJob()
{
    if (parts_mover.moves_blocker.isCancelled())
        return {};

    auto moving_tagger = selectPartsForMove();
    if (moving_tagger->parts_to_move.empty())
        return {};

    return JobAndPool{[this, moving_tagger] () mutable
    {
        moveParts(moving_tagger);
    }, PoolType::MOVE};
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
        NameAndTypePair("_partition_id", std::make_shared<DataTypeString>()),
        NameAndTypePair("_sample_factor", std::make_shared<DataTypeFloat64>()),
    };
}

size_t MergeTreeData::getTotalMergesWithTTLInMergeList() const
{
    return global_context.getMergeList().getExecutingMergesWithTTLCount();
}

}
