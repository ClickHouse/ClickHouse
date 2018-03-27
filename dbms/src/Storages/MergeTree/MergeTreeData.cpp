#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/AlterCommands.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/ValuesRowInputStream.h>
#include <DataStreams/copyData.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/HexWriteBuffer.h>
#include <IO/Operators.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/Increment.h>
#include <Common/SimpleIncrement.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Stopwatch.h>
#include <Common/typeid_cast.h>
#include <Common/localBackup.h>

#include <Poco/DirectoryIterator.h>

#include <boost/range/adaptor/filtered.hpp>

#include <algorithm>
#include <iomanip>
#include <thread>
#include <typeinfo>
#include <typeindex>
#include <optional>
#include <Interpreters/PartLog.h>


namespace ProfileEvents
{
    extern const Event RejectedInserts;
    extern const Event DelayedInserts;
    extern const Event DelayedInsertsMilliseconds;
}

namespace CurrentMetrics
{
    extern const Metric DelayedInserts;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int SYNTAX_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int INVALID_PARTITION_VALUE;
    extern const int METADATA_MISMATCH;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int TOO_MANY_PARTS;
}


MergeTreeData::MergeTreeData(
    const String & database_, const String & table_,
    const String & full_path_, const ColumnsDescription & columns_,
    Context & context_,
    const ASTPtr & primary_expr_ast_,
    const ASTPtr & secondary_sort_expr_ast_,
    const String & date_column_name,
    const ASTPtr & partition_expr_ast_,
    const ASTPtr & sampling_expression_,
    const MergingParams & merging_params_,
    const MergeTreeSettings & settings_,
    bool require_part_metadata_,
    bool attach,
    BrokenPartCallback broken_part_callback_)
    : ITableDeclaration{columns_},
    context(context_),
    sampling_expression(sampling_expression_),
    index_granularity(settings_.index_granularity),
    merging_params(merging_params_),
    settings(settings_),
    primary_expr_ast(primary_expr_ast_),
    secondary_sort_expr_ast(secondary_sort_expr_ast_),
    partition_expr_ast(partition_expr_ast_),
    require_part_metadata(require_part_metadata_),
    database_name(database_), table_name(table_),
    full_path(full_path_),
    broken_part_callback(broken_part_callback_),
    log_name(database_name + "." + table_name), log(&Logger::get(log_name + " (Data)")),
    data_parts_by_info(data_parts_indexes.get<TagByInfo>()),
    data_parts_by_state_and_info(data_parts_indexes.get<TagByStateAndInfo>())
{
    /// NOTE: using the same columns list as is read when performing actual merges.
    merging_params.check(getColumns().getAllPhysical());

    if (!primary_expr_ast)
        throw Exception("Primary key cannot be empty", ErrorCodes::BAD_ARGUMENTS);

    initPrimaryKey();

    if (sampling_expression && (!primary_key_sample.has(sampling_expression->getColumnName()))
        && !attach && !settings.compatibility_allow_sampling_expression_not_in_primary_key) /// This is for backward compatibility.
        throw Exception("Sampling expression must be present in the primary key", ErrorCodes::BAD_ARGUMENTS);

    MergeTreeDataFormatVersion min_format_version(0);
    if (!date_column_name.empty())
    {
        try
        {
            String partition_expr_str = "toYYYYMM(" + backQuoteIfNeed(date_column_name) + ")";
            ParserNotEmptyExpressionList parser(/* allow_alias_without_as_keyword = */ false);
            partition_expr_ast = parseQuery(
                parser, partition_expr_str.data(), partition_expr_str.data() + partition_expr_str.length(), "partition expression");

            initPartitionKey();

            if (minmax_idx_date_column_pos == -1)
                throw Exception("Could not find Date column", ErrorCodes::BAD_TYPE_OF_FIELD);
        }
        catch (Exception & e)
        {
            /// Better error message.
            e.addMessage("(while initializing MergeTree partition key from date column `" + date_column_name + "`)");
            throw;
        }
    }
    else
    {
        initPartitionKey();
        min_format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    }

    /// Creating directories, if not exist.
    Poco::File(full_path).createDirectories();
    Poco::File(full_path + "detached").createDirectory();

    String version_file_path = full_path + "format_version.txt";
    if (!attach)
    {
        format_version = min_format_version;
        WriteBufferFromFile buf(version_file_path);
        writeIntText(format_version.toUnderType(), buf);
    }
    else if (Poco::File(version_file_path).exists())
    {
        ReadBufferFromFile buf(version_file_path);
        readIntText(format_version, buf);
        if (!buf.eof())
            throw Exception("Bad version file: " + version_file_path, ErrorCodes::CORRUPTED_DATA);
    }
    else
        format_version = 0;

    if (format_version < min_format_version)
        throw Exception(
            "MergeTree data format version on disk doesn't support custom partitioning",
            ErrorCodes::METADATA_MISMATCH);
}


static void checkKeyExpression(const ExpressionActions & expr, const Block & sample_block, const String & key_name)
{
    for (const ExpressionAction & action : expr.getActions())
    {
        if (action.type == ExpressionAction::ARRAY_JOIN)
            throw Exception(key_name + " key cannot contain array joins");

        if (action.type == ExpressionAction::APPLY_FUNCTION)
        {
            IFunctionBase & func = *action.function;
            if (!func.isDeterministic())
                throw Exception(key_name + " key cannot contain non-deterministic functions, "
                    "but contains function " + func.getName(),
                    ErrorCodes::BAD_ARGUMENTS);
        }
    }

    for (const ColumnWithTypeAndName & element : sample_block)
    {
        const ColumnPtr & column = element.column;
        if (column && (column->isColumnConst() || column->isDummy()))
            throw Exception{key_name + " key cannot contain constants", ErrorCodes::ILLEGAL_COLUMN};

        if (element.type->isNullable())
            throw Exception{key_name + " key cannot contain nullable columns", ErrorCodes::ILLEGAL_COLUMN};
    }
}


void MergeTreeData::initPrimaryKey()
{
    auto addSortDescription = [](SortDescription & descr, const ASTPtr & expr_ast)
    {
        descr.reserve(descr.size() + expr_ast->children.size());
        for (const ASTPtr & ast : expr_ast->children)
        {
            String name = ast->getColumnName();
            descr.emplace_back(name, 1, 1);
        }
    };

    /// Initialize description of sorting for primary key.
    primary_sort_descr.clear();
    addSortDescription(primary_sort_descr, primary_expr_ast);

    primary_expr = ExpressionAnalyzer(primary_expr_ast, context, nullptr, getColumns().getAllPhysical()).getActions(false);

    {
        ExpressionActionsPtr projected_expr =
            ExpressionAnalyzer(primary_expr_ast, context, nullptr, getColumns().getAllPhysical()).getActions(true);
        primary_key_sample = projected_expr->getSampleBlock();
    }

    checkKeyExpression(*primary_expr, primary_key_sample, "Primary");

    size_t primary_key_size = primary_key_sample.columns();
    primary_key_data_types.resize(primary_key_size);
    for (size_t i = 0; i < primary_key_size; ++i)
        primary_key_data_types[i] = primary_key_sample.getByPosition(i).type;

    sort_descr = primary_sort_descr;
    if (secondary_sort_expr_ast)
    {
        addSortDescription(sort_descr, secondary_sort_expr_ast);
        secondary_sort_expr = ExpressionAnalyzer(secondary_sort_expr_ast, context, nullptr, getColumns().getAllPhysical()).getActions(false);

        ExpressionActionsPtr projected_expr =
            ExpressionAnalyzer(secondary_sort_expr_ast, context, nullptr, getColumns().getAllPhysical()).getActions(true);
        auto secondary_key_sample = projected_expr->getSampleBlock();

        checkKeyExpression(*secondary_sort_expr, secondary_key_sample, "Secondary");
    }
}


void MergeTreeData::initPartitionKey()
{
    if (!partition_expr_ast || partition_expr_ast->children.empty())
        return;

    partition_expr = ExpressionAnalyzer(partition_expr_ast, context, nullptr, getColumns().getAllPhysical()).getActions(false);
    for (const ASTPtr & ast : partition_expr_ast->children)
    {
        String col_name = ast->getColumnName();
        partition_key_sample.insert(partition_expr->getSampleBlock().getByName(col_name));
    }

    checkKeyExpression(*partition_expr, partition_key_sample, "Partition");

    /// Add all columns used in the partition key to the min-max index.
    const NamesAndTypesList & minmax_idx_columns_with_types = partition_expr->getRequiredColumnsWithTypes();
    minmax_idx_expr = std::make_shared<ExpressionActions>(minmax_idx_columns_with_types, context.getSettingsRef());
    for (const NameAndTypePair & column : minmax_idx_columns_with_types)
    {
        minmax_idx_columns.emplace_back(column.name);
        minmax_idx_column_types.emplace_back(column.type);
        minmax_idx_sort_descr.emplace_back(column.name, 1, 1);
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
}


void MergeTreeData::MergingParams::check(const NamesAndTypesList & columns) const
{
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
            throw Exception("Sign column " + sign_column + " does not exist in table declaration.");
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
            throw Exception("Version column " + version_column + " does not exist in table declaration.");
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
                        "Column " + column_to_sum + " listed in columns to sum does not exist in table declaration.");
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
        case VersionedCollapsing:  return "VersionedCollapsing";

        default:
            throw Exception("Unknown mode of operation for MergeTreeData: " + toString<int>(mode), ErrorCodes::LOGICAL_ERROR);
    }
}


Int64 MergeTreeData::getMaxDataPartIndex()
{
    std::lock_guard<std::mutex> lock_all(data_parts_mutex);

    Int64 max_block_id = 0;
    for (const DataPartPtr & part : data_parts_by_info)
        max_block_id = std::max(max_block_id, part->info.max_block);

    return max_block_id;
}


void MergeTreeData::loadDataParts(bool skip_sanity_checks)
{
    LOG_DEBUG(log, "Loading data parts");

    Strings part_file_names;
    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator it(full_path); it != end; ++it)
    {
        /// Skip temporary directories.
        if (startsWith(it.name(), "tmp"))
            continue;

        part_file_names.push_back(it.name());
    }

    DataPartsVector broken_parts_to_remove;
    DataPartsVector broken_parts_to_detach;
    size_t suspicious_broken_parts = 0;

    std::lock_guard<std::mutex> lock(data_parts_mutex);
    data_parts_indexes.clear();

    for (const String & file_name : part_file_names)
    {
        MergeTreePartInfo part_info;
        if (!MergeTreePartInfo::tryParsePartName(file_name, &part_info, format_version))
            continue;

        MutableDataPartPtr part = std::make_shared<DataPart>(*this, file_name, part_info);
        part->relative_path = file_name;
        bool broken = false;

        try
        {
            part->loadColumnsChecksumsIndexes(require_part_metadata, true);
        }
        catch (const Exception & e)
        {
            /// Don't count the part as broken if there is not enough memory to load it.
            /// In fact, there can be many similar situations.
            /// But it is OK, because there is a safety guard against deleting too many parts.
            if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
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
                LOG_ERROR(log, "Considering to remove broken part " << full_path + file_name << " because it's impossible to repair.");
                broken_parts_to_remove.push_back(part);
            }
            else
            {
                /// Count the number of parts covered by the broken part. If it is at least two, assume that
                /// the broken part was created as a result of merging them and we won't lose data if we
                /// delete it.
                int contained_parts = 0;

                LOG_ERROR(log, "Part " << full_path + file_name << " is broken. Looking for parts to replace it.");

                for (const String & contained_name : part_file_names)
                {
                    if (contained_name == file_name)
                        continue;

                    MergeTreePartInfo contained_part_info;
                    if (!MergeTreePartInfo::tryParsePartName(contained_name, &contained_part_info, format_version))
                        continue;

                    if (part->info.contains(contained_part_info))
                    {
                        LOG_ERROR(log, "Found part " << full_path + contained_name);
                        ++contained_parts;
                    }
                }

                if (contained_parts >= 2)
                {
                    LOG_ERROR(log, "Considering to remove broken part " << full_path + file_name << " because it covers at least 2 other parts");
                    broken_parts_to_remove.push_back(part);
                }
                else
                {
                    LOG_ERROR(log, "Detaching broken part " << full_path + file_name
                        << " because it covers less than 2 parts. You need to resolve this manually");
                    broken_parts_to_detach.push_back(part);
                    ++suspicious_broken_parts;
                }
            }

            continue;
        }

        part->modification_time = Poco::File(full_path + file_name).getLastModified().epochTime();
        /// Assume that all parts are Committed, covered parts will be detected and marked as Outdated later
        part->state = DataPartState::Committed;

        if (!data_parts_indexes.insert(part).second)
            throw Exception("Part " + part->name + " already exists", ErrorCodes::DUPLICATE_DATA_PART);
    }

    if (suspicious_broken_parts > settings.max_suspicious_broken_parts && !skip_sanity_checks)
        throw Exception("Suspiciously many (" + toString(suspicious_broken_parts) + ") broken parts to remove.",
            ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);

    for (auto & part : broken_parts_to_remove)
        part->remove();
    for (auto & part : broken_parts_to_detach)
        part->renameAddPrefix(true, "");

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

    LOG_DEBUG(log, "Loaded data parts (" << data_parts_indexes.size() << " items)");
}


/// Is the part directory old.
/// True if its modification time and the modification time of all files inside it is less then threshold.
/// (Only files on the first level of nesting are considered).
static bool isOldPartDirectory(Poco::File & directory, time_t threshold)
{
    if (directory.getLastModified().epochTime() >= threshold)
        return false;

    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator it(directory); it != end; ++it)
        if (it->getLastModified().epochTime() >= threshold)
            return false;

    return true;
}


void MergeTreeData::clearOldTemporaryDirectories(ssize_t custom_directories_lifetime_seconds)
{
    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock<std::mutex> lock(clear_old_temporary_directories_mutex, std::defer_lock);
    if (!lock.try_lock())
        return;

    time_t current_time = time(nullptr);
    ssize_t deadline = (custom_directories_lifetime_seconds >= 0)
        ? current_time - custom_directories_lifetime_seconds
        : current_time - settings.temporary_directories_lifetime.totalSeconds();

    /// Delete temporary directories older than a day.
    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator it{full_path}; it != end; ++it)
    {
        if (startsWith(it.name(), "tmp"))
        {
            Poco::File tmp_dir(full_path + it.name());

            try
            {
                if (tmp_dir.isDirectory() && isOldPartDirectory(tmp_dir, deadline))
                {
                    LOG_WARNING(log, "Removing temporary directory " << full_path << it.name());
                    Poco::File(full_path + it.name()).remove(true);
                }
            }
            catch (const Poco::FileNotFoundException &)
            {
                /// If the file is already deleted, do nothing.
            }
        }
    }
}


MergeTreeData::DataPartsVector MergeTreeData::grabOldParts()
{
    DataPartsVector res;

    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock<std::mutex> lock(grab_old_parts_mutex, std::defer_lock);
    if (!lock.try_lock())
        return res;

    time_t now = time(nullptr);
    std::vector<DataPartIteratorByStateAndInfo> parts_to_delete;

    {
        std::lock_guard<std::mutex> lock_parts(data_parts_mutex);

        auto outdated_parts_range = getDataPartsStateRange(DataPartState::Outdated);
        for (auto it = outdated_parts_range.begin(); it != outdated_parts_range.end(); ++it)
        {
            const DataPartPtr & part = *it;

            auto part_remove_time = part->remove_time.load(std::memory_order_relaxed);

            if (part.unique() && /// Grab only parts that are not used by anyone (SELECTs for example).
                part_remove_time < now &&
                now - part_remove_time > settings.old_parts_lifetime.totalSeconds())
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
        LOG_TRACE(log, "Found " << res.size() << " old parts to remove.");

    return res;
}


void MergeTreeData::rollbackDeletingParts(const MergeTreeData::DataPartsVector & parts)
{
    std::lock_guard<std::mutex> lock(data_parts_mutex);
    for (auto & part : parts)
    {
        /// We should modify it under data_parts_mutex
        part->assertState({DataPartState::Deleting});
        modifyPartState(part, DataPartState::Outdated);
    }
}

void MergeTreeData::removePartsFinally(const MergeTreeData::DataPartsVector & parts)
{
    {
        std::lock_guard<std::mutex> lock(data_parts_mutex);

        /// TODO: use data_parts iterators instead of pointers
        for (auto & part : parts)
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
    if (auto part_log = context.getPartLog(database_name))
    {
        PartLogElement part_log_elem;

        part_log_elem.event_type = PartLogElement::REMOVE_PART;
        part_log_elem.event_time = time(nullptr);
        part_log_elem.duration_ms = 0;

        part_log_elem.database_name = database_name;
        part_log_elem.table_name = table_name;

        for (auto & part : parts)
        {
            part_log_elem.part_name = part->name;
            part_log_elem.bytes_compressed_on_disk = part->bytes_on_disk;
            part_log_elem.rows = part->rows_count;

            part_log->add(part_log_elem);
        }
    }
}

void MergeTreeData::clearOldPartsFromFilesystem()
{
    auto parts_to_remove = grabOldParts();

    for (const DataPartPtr & part : parts_to_remove)
    {
        LOG_DEBUG(log, "Removing part from filesystem " << part->name);
        part->remove();
    }

    removePartsFinally(parts_to_remove);
}

void MergeTreeData::setPath(const String & new_full_path)
{
    if (Poco::File{new_full_path}.exists())
        throw Exception{
            "Target path already exists: " + new_full_path,
            /// @todo existing target can also be a file, not directory
            ErrorCodes::DIRECTORY_ALREADY_EXISTS};

    Poco::File(full_path).renameTo(new_full_path);

    context.dropCaches();
    full_path = new_full_path;
}

void MergeTreeData::dropAllData()
{
    LOG_TRACE(log, "dropAllData: waiting for locks.");

    std::lock_guard<std::mutex> lock(data_parts_mutex);

    LOG_TRACE(log, "dropAllData: removing data from memory.");

    data_parts_indexes.clear();
    column_sizes.clear();

    context.dropCaches();

    LOG_TRACE(log, "dropAllData: removing data from filesystem.");

    Poco::File(full_path).remove(true);

    LOG_TRACE(log, "dropAllData: done.");
}

namespace
{

/// If true, then in order to ALTER the type of the column from the type from to the type to
/// we don't need to rewrite the data, we only need to update metadata and columns.txt in part directories.
/// The function works for Arrays and Nullables of the same structure.
bool isMetadataOnlyConversion(const IDataType * from, const IDataType * to)
{
    if (from->getName() == to->getName())
        return true;

    static const std::unordered_multimap<std::type_index, const std::type_info &> ALLOWED_CONVERSIONS =
        {
            { typeid(DataTypeEnum8),    typeid(DataTypeEnum8)    },
            { typeid(DataTypeEnum8),    typeid(DataTypeInt8)     },
            { typeid(DataTypeEnum16),   typeid(DataTypeEnum16)   },
            { typeid(DataTypeEnum16),   typeid(DataTypeInt16)    },
            { typeid(DataTypeDateTime), typeid(DataTypeUInt32)   },
            { typeid(DataTypeUInt32),   typeid(DataTypeDateTime) },
            { typeid(DataTypeDate),     typeid(DataTypeUInt16)   },
            { typeid(DataTypeUInt16),   typeid(DataTypeDate)     },
        };

    while (true)
    {
        auto it_range = ALLOWED_CONVERSIONS.equal_range(typeid(*from));
        for (auto it = it_range.first; it != it_range.second; ++it)
        {
            if (it->second == typeid(*to))
                return true;
        }

        const auto * arr_from = typeid_cast<const DataTypeArray *>(from);
        const auto * arr_to = typeid_cast<const DataTypeArray *>(to);
        if (arr_from && arr_to)
        {
            from = arr_from->getNestedType().get();
            to = arr_to->getNestedType().get();
            continue;
        }

        const auto * nullable_from = typeid_cast<const DataTypeNullable *>(from);
        const auto * nullable_to = typeid_cast<const DataTypeNullable *>(to);
        if (nullable_from && nullable_to)
        {
            from = nullable_from->getNestedType().get();
            to = nullable_to->getNestedType().get();
            continue;
        }

        return false;
    }
}

}

void MergeTreeData::checkAlter(const AlterCommands & commands)
{
    /// Check that needed transformations can be applied to the list of columns without considering type conversions.
    auto new_columns = getColumns();
    commands.apply(new_columns);

    /// Set of columns that shouldn't be altered.
    NameSet columns_alter_forbidden;

    /// Primary key columns can be ALTERed only if they are used in the key as-is
    /// (and not as a part of some expression) and if the ALTER only affects column metadata.
    NameSet columns_alter_metadata_only;

    if (partition_expr)
    {
        /// Forbid altering partition key columns because it can change partition ID format.
        /// TODO: in some cases (e.g. adding an Enum value) a partition key column can still be ALTERed.
        /// We should allow it.
        for (const String & col : partition_expr->getRequiredColumns())
            columns_alter_forbidden.insert(col);
    }

    auto processSortingColumns =
            [&columns_alter_forbidden, &columns_alter_metadata_only] (const ExpressionActionsPtr & expression)
    {
        for (const ExpressionAction & action : expression->getActions())
        {
            auto action_columns = action.getNeededColumns();
            columns_alter_forbidden.insert(action_columns.begin(), action_columns.end());
        }
        for (const String & col : expression->getRequiredColumns())
            columns_alter_metadata_only.insert(col);
    };

    if (primary_expr)
        processSortingColumns(primary_expr);
    /// We don't process sampling_expression separately because it must be among the primary key columns.

    if (secondary_sort_expr)
        processSortingColumns(secondary_sort_expr);

    if (!merging_params.sign_column.empty())
        columns_alter_forbidden.insert(merging_params.sign_column);

    std::map<String, const IDataType *> old_types;
    for (const auto & column : getColumns().getAllPhysical())
        old_types.emplace(column.name, column.type.get());

    for (const AlterCommand & command : commands)
    {
        if (columns_alter_forbidden.count(command.column_name))
            throw Exception("trying to ALTER key column " + command.column_name, ErrorCodes::ILLEGAL_COLUMN);

        if (columns_alter_metadata_only.count(command.column_name))
        {
            if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                auto it = old_types.find(command.column_name);
                if (it != old_types.end() && isMetadataOnlyConversion(it->second, command.data_type.get()))
                    continue;
            }

            throw Exception(
                    "ALTER of key column " + command.column_name + " must be metadata-only",
                    ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    /// Check that type conversions are possible.
    ExpressionActionsPtr unused_expression;
    NameToNameMap unused_map;
    bool unused_bool;

    createConvertExpression(nullptr, getColumns().getAllPhysical(), new_columns.getAllPhysical(), unused_expression, unused_map, unused_bool);
}

void MergeTreeData::createConvertExpression(const DataPartPtr & part, const NamesAndTypesList & old_columns, const NamesAndTypesList & new_columns,
    ExpressionActionsPtr & out_expression, NameToNameMap & out_rename_map, bool & out_force_update_metadata) const
{
    out_expression = nullptr;
    out_rename_map = {};
    out_force_update_metadata = false;

    using NameToType = std::map<String, const IDataType *>;
    NameToType new_types;
    for (const NameAndTypePair & column : new_columns)
        new_types.emplace(column.name, column.type.get());

    /// For every column that need to be converted: source column name, column name of calculated expression for conversion.
    std::vector<std::pair<String, String>> conversions;

    /// Collect counts for shared streams of different columns. As an example, Nested columns have shared stream with array sizes.
    std::map<String, size_t> stream_counts;
    for (const NameAndTypePair & column : old_columns)
    {
        column.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
        {
            ++stream_counts[IDataType::getFileNameForStream(column.name, substream_path)];
        }, {});
    }

    for (const NameAndTypePair & column : old_columns)
    {
        if (!new_types.count(column.name))
        {
            /// The column was deleted.
            if (!part || part->hasColumnFiles(column.name))
            {
                column.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
                {
                    String file_name = IDataType::getFileNameForStream(column.name, substream_path);

                    /// Delete files if they are no longer shared with another column.
                    if (--stream_counts[file_name] == 0)
                    {
                        out_rename_map[file_name + ".bin"] = "";
                        out_rename_map[file_name + ".mrk"] = "";
                    }
                }, {});
            }
        }
        else
        {
            /// The column was converted. Collect conversions.
            const auto * new_type = new_types[column.name];
            const String new_type_name = new_type->getName();
            const auto * old_type = column.type.get();

            if (!new_type->equals(*old_type) && (!part || part->hasColumnFiles(column.name)))
            {
                if (isMetadataOnlyConversion(old_type, new_type))
                {
                    out_force_update_metadata = true;
                    continue;
                }

                /// Need to modify column type.
                if (!out_expression)
                    out_expression = std::make_shared<ExpressionActions>(NamesAndTypesList(), context.getSettingsRef());

                out_expression->addInput(ColumnWithTypeAndName(nullptr, column.type, column.name));

                Names out_names;

                /// This is temporary name for expression. TODO Invent the name more safely.
                const String new_type_name_column = '#' + new_type_name + "_column";
                out_expression->add(ExpressionAction::addColumn(
                    { DataTypeString().createColumnConst(1, new_type_name), std::make_shared<DataTypeString>(), new_type_name_column }));

                const auto & function = FunctionFactory::instance().get("CAST", context);
                out_expression->add(ExpressionAction::applyFunction(
                    function, Names{column.name, new_type_name_column}), out_names);

                out_expression->add(ExpressionAction::removeColumn(new_type_name_column));
                out_expression->add(ExpressionAction::removeColumn(column.name));

                conversions.emplace_back(column.name, out_names.at(0));

            }
        }
    }

    if (!conversions.empty())
    {
        /// Give proper names for temporary columns with conversion results.

        NamesWithAliases projection;
        projection.reserve(conversions.size());

        for (const auto & source_and_expression : conversions)
        {
            /// Column name for temporary filenames before renaming. NOTE The is unnecessarily tricky.

            String original_column_name = source_and_expression.first;
            String temporary_column_name = original_column_name + " converting";

            projection.emplace_back(source_and_expression.second, temporary_column_name);

            /// After conversion, we need to rename temporary files into original.

            new_types[source_and_expression.first]->enumerateStreams(
                [&](const IDataType::SubstreamPath & substream_path)
                {
                    /// Skip array sizes, because they cannot be modified in ALTER.
                    if (!substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes)
                        return;

                    String original_file_name = IDataType::getFileNameForStream(original_column_name, substream_path);
                    String temporary_file_name = IDataType::getFileNameForStream(temporary_column_name, substream_path);

                    out_rename_map[temporary_file_name + ".bin"] = original_file_name + ".bin";
                    out_rename_map[temporary_file_name + ".mrk"] = original_file_name + ".mrk";
                }, {});
        }

        out_expression->add(ExpressionAction::project(projection));
    }

    if (part && !out_rename_map.empty())
    {
        WriteBufferFromOwnString out;
        out << "Will ";
        bool first = true;
        for (const auto & from_to : out_rename_map)
        {
            if (!first)
                out << ", ";
            first = false;
            if (from_to.second.empty())
                out << "remove " << from_to.first;
            else
                out << "rename " << from_to.first << " to " << from_to.second;
        }
        out << " in part " << part->name;
        LOG_DEBUG(log, out.str());
    }
}

MergeTreeData::AlterDataPartTransactionPtr MergeTreeData::alterDataPart(
    const DataPartPtr & part,
    const NamesAndTypesList & new_columns,
    const ASTPtr & new_primary_key,
    bool skip_sanity_checks)
{
    ExpressionActionsPtr expression;
    AlterDataPartTransactionPtr transaction(new AlterDataPartTransaction(part)); /// Blocks changes to the part.
    bool force_update_metadata;
    createConvertExpression(part, part->columns, new_columns, expression, transaction->rename_map, force_update_metadata);

    size_t num_files_to_modify = transaction->rename_map.size();
    size_t num_files_to_remove = 0;

    for (const auto & from_to : transaction->rename_map)
        if (from_to.second.empty())
            ++num_files_to_remove;

    if (!skip_sanity_checks
        && (num_files_to_modify > settings.max_files_to_modify_in_alter_columns
            || num_files_to_remove > settings.max_files_to_remove_in_alter_columns))
    {
        transaction->clear();

        const bool forbidden_because_of_modify = num_files_to_modify > settings.max_files_to_modify_in_alter_columns;

        std::stringstream exception_message;
        exception_message
            << "Suspiciously many ("
            << (forbidden_because_of_modify ? num_files_to_modify : num_files_to_remove)
            << ") files (";

        bool first = true;
        for (const auto & from_to : transaction->rename_map)
        {
            if (!first)
                exception_message << ", ";
            if (forbidden_because_of_modify)
            {
                exception_message << "from `" << from_to.first << "' to `" << from_to.second << "'";
                first = false;
            }
            else if (from_to.second.empty())
            {
                exception_message << "`" << from_to.first << "'";
                first = false;
            }
        }

        exception_message
            << ") need to be "
            << (forbidden_because_of_modify ? "modified" : "removed")
            << " in part " << part->name << " of table at " << full_path << ". Aborting just in case."
            << " If it is not an error, you could increase merge_tree/"
            << (forbidden_because_of_modify ? "max_files_to_modify_in_alter_columns" : "max_files_to_remove_in_alter_columns")
            << " parameter in configuration file (current value: "
            << (forbidden_because_of_modify ? settings.max_files_to_modify_in_alter_columns : settings.max_files_to_remove_in_alter_columns)
            << ")";

        throw Exception(exception_message.str(), ErrorCodes::TABLE_DIFFERS_TOO_MUCH);
    }

    DataPart::Checksums add_checksums;

    /// Update primary key if needed.
    size_t new_primary_key_file_size{};
    MergeTreeDataPartChecksum::uint128 new_primary_key_hash{};

    /// TODO: Check the order of secondary sorting key columns.
    if (new_primary_key.get() != primary_expr_ast.get())
    {
        ExpressionActionsPtr new_primary_expr = ExpressionAnalyzer(new_primary_key, context, nullptr, new_columns).getActions(true);
        Block new_primary_key_sample = new_primary_expr->getSampleBlock();
        size_t new_key_size = new_primary_key_sample.columns();

        Columns new_index(new_key_size);

        /// Copy the existing primary key columns. Fill new columns with default values.
        /// NOTE default expressions are not supported.

        ssize_t prev_position_of_existing_column = -1;
        for (size_t i = 0; i < new_key_size; ++i)
        {
            const String & column_name = new_primary_key_sample.safeGetByPosition(i).name;

            if (primary_key_sample.has(column_name))
            {
                ssize_t position_of_existing_column = primary_key_sample.getPositionByName(column_name);

                if (position_of_existing_column < prev_position_of_existing_column)
                    throw Exception("Permuting of columns of primary key is not supported", ErrorCodes::BAD_ARGUMENTS);

                new_index[i] = part->index.at(position_of_existing_column);
                prev_position_of_existing_column = position_of_existing_column;
            }
            else
            {
                const IDataType & type = *new_primary_key_sample.safeGetByPosition(i).type;
                new_index[i] = type.createColumnConstWithDefaultValue(part->marks_count)->convertToFullColumnIfConst();
            }
        }

        if (prev_position_of_existing_column == -1)
            throw Exception("No common columns while modifying primary key", ErrorCodes::BAD_ARGUMENTS);

        String index_tmp_path = full_path + part->name + "/primary.idx.tmp";
        WriteBufferFromFile index_file(index_tmp_path);
        HashingWriteBuffer index_stream(index_file);

        for (size_t i = 0, marks_count = part->marks_count; i < marks_count; ++i)
            for (size_t j = 0; j < new_key_size; ++j)
                new_primary_key_sample.getByPosition(j).type->serializeBinary(*new_index[j].get(), i, index_stream);

        transaction->rename_map["primary.idx.tmp"] = "primary.idx";

        index_stream.next();
        new_primary_key_file_size = index_stream.count();
        new_primary_key_hash = index_stream.getHash();
    }

    if (transaction->rename_map.empty() && !force_update_metadata)
    {
        transaction->clear();
        return nullptr;
    }

    /// Apply the expression and write the result to temporary files.
    if (expression)
    {
        MarkRanges ranges{MarkRange(0, part->marks_count)};
        BlockInputStreamPtr part_in = std::make_shared<MergeTreeBlockInputStream>(
            *this, part, DEFAULT_MERGE_BLOCK_SIZE, 0, 0, expression->getRequiredColumns(), ranges,
            false, nullptr, "", false, 0, DBMS_DEFAULT_BUFFER_SIZE, false);

        auto compression_settings = this->context.chooseCompressionSettings(
            part->bytes_on_disk,
            static_cast<double>(part->bytes_on_disk) / this->getTotalActiveSizeInBytes());
        ExpressionBlockInputStream in(part_in, expression);

        /** Don't write offsets for arrays, because ALTER never change them
         *  (MODIFY COLUMN could only change types of elements but never modify array sizes).
          * Also note that they does not participate in 'rename_map'.
          * Also note, that for columns, that are parts of Nested,
          *  temporary column name ('converting_column_name') created in 'createConvertExpression' method
          *  will have old name of shared offsets for arrays.
          */
        MergedColumnOnlyOutputStream out(*this, in.getHeader(), full_path + part->name + '/', true /* sync */, compression_settings, true /* skip_offsets */);

        in.readPrefix();
        out.writePrefix();

        while (Block b = in.read())
            out.write(b);

        in.readSuffix();
        add_checksums = out.writeSuffixAndGetChecksums();
    }

    /// Update the checksums.
    DataPart::Checksums new_checksums = part->checksums;
    for (auto it : transaction->rename_map)
    {
        if (it.second.empty())
            new_checksums.files.erase(it.first);
        else
            new_checksums.files[it.second] = add_checksums.files[it.first];
    }

    if (new_primary_key_file_size)
    {
        new_checksums.files["primary.idx"].file_size = new_primary_key_file_size;
        new_checksums.files["primary.idx"].file_hash = new_primary_key_hash;
    }

    /// Write the checksums to the temporary file.
    if (!part->checksums.empty())
    {
        transaction->new_checksums = new_checksums;
        WriteBufferFromFile checksums_file(full_path + part->name + "/checksums.txt.tmp", 4096);
        new_checksums.write(checksums_file);
        transaction->rename_map["checksums.txt.tmp"] = "checksums.txt";
    }

    /// Write the new column list to the temporary file.
    {
        transaction->new_columns = new_columns.filter(part->columns.getNames());
        WriteBufferFromFile columns_file(full_path + part->name + "/columns.txt.tmp", 4096);
        transaction->new_columns.writeText(columns_file);
        transaction->rename_map["columns.txt.tmp"] = "columns.txt";
    }

    return transaction;
}

void MergeTreeData::AlterDataPartTransaction::commit()
{
    if (!data_part)
        return;
    try
    {
        std::unique_lock<std::shared_mutex> lock(data_part->columns_lock);

        String path = data_part->storage.full_path + data_part->name + "/";

        /// NOTE: checking that a file exists before renaming or deleting it
        /// is justified by the fact that, when converting an ordinary column
        /// to a nullable column, new files are created which did not exist
        /// before, i.e. they do not have older versions.

        /// 1) Rename the old files.
        for (const auto & from_to : rename_map)
        {
            String name = from_to.second.empty() ? from_to.first : from_to.second;
            Poco::File file{path + name};
            if (file.exists())
                file.renameTo(path + name + ".tmp2");
        }

        /// 2) Move new files in the place of old and update the metadata in memory.
        for (const auto & from_to : rename_map)
        {
            if (!from_to.second.empty())
                Poco::File{path + from_to.first}.renameTo(path + from_to.second);
        }

        auto & mutable_part = const_cast<DataPart &>(*data_part);
        mutable_part.checksums = new_checksums;
        mutable_part.columns = new_columns;

        /// 3) Delete the old files.
        for (const auto & from_to : rename_map)
        {
            String name = from_to.second.empty() ? from_to.first : from_to.second;
            Poco::File file{path + name + ".tmp2"};
            if (file.exists())
                file.remove();
        }

        mutable_part.bytes_on_disk = MergeTreeData::DataPart::calculateTotalSizeOnDisk(path);

        /// TODO: we can skip resetting caches when the column is added.
        data_part->storage.context.dropCaches();

        clear();
    }
    catch (...)
    {
        /// Don't delete temporary files in the destructor in case something went wrong.
        clear();
        throw;
    }
}

MergeTreeData::AlterDataPartTransaction::~AlterDataPartTransaction()
{
    try
    {
        if (!data_part)
            return;

        LOG_WARNING(data_part->storage.log, "Aborting ALTER of part " << data_part->relative_path);

        String path = data_part->getFullPath();
        for (const auto & from_to : rename_map)
        {
            if (!from_to.second.empty())
            {
                try
                {
                    Poco::File file(path + from_to.first);
                    if (file.exists())
                        file.remove();
                }
                catch (Poco::Exception & e)
                {
                    LOG_WARNING(data_part->storage.log, "Can't remove " << path + from_to.first << ": " << e.displayText());
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


MergeTreeData::DataPartsVector MergeTreeData::getActivePartsToReplace(
    const MergeTreePartInfo & new_part_info,
    const String & new_part_name,
    DataPartPtr & out_covering_part,
    std::lock_guard<std::mutex> & /* data_parts_lock */) const
{
    /// Parts contained in the part are consecutive in data_parts, intersecting the insertion place for the part itself.
    auto it_middle = data_parts_by_state_and_info.lower_bound(DataPartStateAndInfo(DataPartState::Committed, new_part_info));
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


void MergeTreeData::renameTempPartAndAdd(MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction)
{
    auto removed = renameTempPartAndReplace(part, increment, out_transaction);
    if (!removed.empty())
        throw Exception("Added part " + part->name + " covers " + toString(removed.size())
            + " existing part(s) (including " + removed[0]->name + ")", ErrorCodes::LOGICAL_ERROR);
}



MergeTreeData::DataPartsVector MergeTreeData::renameTempPartAndReplace(
    MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction)
{
    if (out_transaction && out_transaction->data && out_transaction->data != this)
        throw Exception("The same MergeTreeData::Transaction cannot be used for different tables",
            ErrorCodes::LOGICAL_ERROR);

    std::lock_guard<std::mutex> lock(data_parts_mutex);

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
        part_info.min_block = part_info.max_block = increment->get();

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        part_name = part_info.getPartNameV0(part->getMinDate(), part->getMaxDate());
    else
        part_name = part_info.getPartName();

    LOG_TRACE(log, "Renaming temporary part " << part->relative_path << " to " << part_name << ".");

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

    if (covering_part)
    {
        LOG_WARNING(log, "Tried to add obsolete part " << part_name << " covered by " << covering_part->getNameWithState());
        return {};
    }

    /// All checks are passed. Now we can rename the part on disk.
    /// So, we maintain invariant: if a non-temporary part in filesystem then it is in data_parts
    ///
    /// If out_transaction is null, we commit the part to the active set immediately, else add it to the transaction.
    part->name = part_name;
    part->info = part_info;
    part->is_temp = false;
    part->state = DataPartState::PreCommitted;
    part->renameTo(part_name);

    auto part_it = data_parts_indexes.insert(part).first;

    if (out_transaction)
    {
        out_transaction->data = this;
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

    return covered_parts;
}

void MergeTreeData::removePartsFromWorkingSet(const DataPartsVector & remove, bool clear_without_timeout)
{
    std::lock_guard<std::mutex> lock(data_parts_mutex);

    for (auto & part : remove)
    {
        if (!data_parts_by_info.count(part->info))
            throw Exception("Part " + part->getNameWithState() + " not found in data_parts", ErrorCodes::LOGICAL_ERROR);

        part->assertState({DataPartState::PreCommitted, DataPartState::Committed, DataPartState::Outdated});
    }

    auto remove_time = clear_without_timeout ? 0 : time(nullptr);
    for (const DataPartPtr & part : remove)
    {
        if (part->state == DataPartState::Committed)
            removePartContributionToColumnSizes(part);

        modifyPartState(part, DataPartState::Outdated);
        part->remove_time.store(remove_time, std::memory_order_relaxed);
    }
}


void MergeTreeData::renameAndDetachPart(const DataPartPtr & part_to_detach, const String & prefix, bool restore_covered,
                                        bool move_to_detached)
{
    LOG_INFO(log, "Renaming " << part_to_detach->relative_path << " to " << prefix << part_to_detach->name << " and detaching it.");

    std::lock_guard<std::mutex> lock(data_parts_mutex);

    auto it_part = data_parts_by_info.find(part_to_detach->info);
    if (it_part == data_parts_by_info.end())
        throw Exception("No such data part " + part_to_detach->getNameWithState(), ErrorCodes::NO_SUCH_DATA_PART);

    /// What if part_to_detach is reference to *it_part? Make a new owner just in case.
    DataPartPtr part = *it_part;

    if (part->state == DataPartState::Committed)
        removePartContributionToColumnSizes(part);
    modifyPartState(it_part, DataPartState::Deleting);
    if (move_to_detached || !prefix.empty())
        part->renameAddPrefix(move_to_detached, prefix);
    data_parts_indexes.erase(it_part);

    if (restore_covered && part->info.level == 0)
    {
        LOG_WARNING(log, "Will not recover parts covered by zero-level part " << part->name);
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
            LOG_INFO(log, "Activated part " << name);
        }

        if (error)
        {
            LOG_ERROR(log, "The set of parts restored in place of " << part->name << " looks incomplete."
                           << " There might or might not be a data loss."
                           << (error_parts.empty() ? "" : " Suspicious parts: " + error_parts));
        }
    }
}


size_t MergeTreeData::getTotalActiveSizeInBytes() const
{
    size_t res = 0;
    {
        std::lock_guard<std::mutex> lock(data_parts_mutex);

        for (auto & part : getDataPartsStateRange(DataPartState::Committed))
            res += part->bytes_on_disk;
    }

    return res;
}


size_t MergeTreeData::getMaxPartsCountForPartition() const
{
    std::lock_guard<std::mutex> lock(data_parts_mutex);

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


void MergeTreeData::delayInsertIfNeeded(Poco::Event * until)
{
    const size_t parts_count = getMaxPartsCountForPartition();
    if (parts_count < settings.parts_to_delay_insert)
        return;

    if (parts_count >= settings.parts_to_throw_insert)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception("Too many parts (" + toString(parts_count) + "). Merges are processing significantly slower than inserts.", ErrorCodes::TOO_MANY_PARTS);
    }

    const size_t max_k = settings.parts_to_throw_insert - settings.parts_to_delay_insert; /// always > 0
    const size_t k = 1 + parts_count - settings.parts_to_delay_insert; /// from 1 to max_k
    const double delay_milliseconds = ::pow(settings.max_delay_to_insert * 1000, static_cast<double>(k) / max_k);

    ProfileEvents::increment(ProfileEvents::DelayedInserts);
    ProfileEvents::increment(ProfileEvents::DelayedInsertsMilliseconds, delay_milliseconds);

    CurrentMetrics::Increment metric_increment(CurrentMetrics::DelayedInserts);

    LOG_INFO(log, "Delaying inserting block by "
        << std::fixed << std::setprecision(4) << delay_milliseconds << " ms. because there are " << parts_count << " parts");

    if (until)
        until->tryWait(delay_milliseconds);
    else
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<size_t>(delay_milliseconds)));
}

MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(const String & part_name)
{
    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

    std::lock_guard<std::mutex> lock(data_parts_mutex);

    auto committed_parts_range = getDataPartsStateRange(DataPartState::Committed);

    /// The part can be covered only by the previous or the next one in data_parts.
    auto it = data_parts_by_state_and_info.lower_bound(DataPartStateAndInfo(DataPartState::Committed, part_info));

    if (it != committed_parts_range.end())
    {
        if ((*it)->name == part_name)
            return *it;
        if ((*it)->info.contains(part_info))
            return *it;
    }

    if (it != committed_parts_range.begin())
    {
        --it;
        if ((*it)->info.contains(part_info))
            return *it;
    }

    return nullptr;
}


MergeTreeData::DataPartPtr MergeTreeData::getPartIfExists(const String & part_name, const MergeTreeData::DataPartStates & valid_states)
{
    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

    std::lock_guard<std::mutex> lock(data_parts_mutex);

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


MergeTreeData::MutableDataPartPtr MergeTreeData::loadPartAndFixMetadata(const String & relative_path)
{
    MutableDataPartPtr part = std::make_shared<DataPart>(*this, Poco::Path(relative_path).getFileName());
    part->relative_path = relative_path;
    String full_part_path = part->getFullPath();

    /// Earlier the list of columns was written incorrectly. Delete it and re-create.
    if (Poco::File(full_part_path + "columns.txt").exists())
        Poco::File(full_part_path + "columns.txt").remove();

    part->loadColumnsChecksumsIndexes(false, true);
    part->modification_time = Poco::File(full_part_path).getLastModified().epochTime();

    /// If the checksums file is not present, calculate the checksums and write them to disk.
    /// Check the data while we are at it.
    if (part->checksums.empty())
    {
        part->checksums = checkDataPart(full_part_path, index_granularity, false, primary_key_data_types);

        {
            WriteBufferFromFile out(full_part_path + "checksums.txt.tmp", 4096);
            part->checksums.write(out);
        }

        Poco::File(full_part_path + "checksums.txt.tmp").renameTo(full_part_path + "checksums.txt");
    }

    return part;
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
    std::shared_lock<std::shared_mutex> lock(part->columns_lock);

    for (const auto & column : part->columns)
    {
        DataPart::ColumnSize & total_column_size = column_sizes[column.name];
        DataPart::ColumnSize part_column_size = part->getColumnSize(column.name, *column.type);
        total_column_size.add(part_column_size);
    }
}

void MergeTreeData::removePartContributionToColumnSizes(const DataPartPtr & part)
{
    std::shared_lock<std::shared_mutex> lock(part->columns_lock);

    for (const auto & column : part->columns)
    {
        DataPart::ColumnSize & total_column_size = column_sizes[column.name];
        DataPart::ColumnSize part_column_size = part->getColumnSize(column.name, *column.type);

        auto log_subtract = [&](size_t & from, size_t value, const char * field)
        {
            if (value > from)
                LOG_ERROR(log, "Possibly incorrect column size subtraction: "
                    << from << " - " << value << " = " << from - value
                    << ", column: " << column.name << ", field: " << field);

            from -= value;
        };

        log_subtract(total_column_size.data_compressed, part_column_size.data_compressed, ".data_compressed");
        log_subtract(total_column_size.data_uncompressed, part_column_size.data_uncompressed, ".data_uncompressed");
        log_subtract(total_column_size.marks, part_column_size.marks, ".marks");
    }
}


void MergeTreeData::freezePartition(const ASTPtr & partition_ast, const String & with_name, const Context & context)
{
    std::optional<String> prefix;
    String partition_id;

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        const auto & partition = dynamic_cast<const ASTPartition &>(*partition_ast);
        /// Month-partitioning specific - partition value can represent a prefix of the partition to freeze.
        if (const auto * partition_lit = dynamic_cast<const ASTLiteral *>(partition.value.get()))
            prefix = partition_lit->value.getType() == Field::Types::UInt64
                ? toString(partition_lit->value.get<UInt64>())
                : partition_lit->value.safeGet<String>();
        else
            partition_id = getPartitionIDFromQuery(partition_ast, context);
    }
    else
        partition_id = getPartitionIDFromQuery(partition_ast, context);

    if (prefix)
        LOG_DEBUG(log, "Freezing parts with prefix " + *prefix);
    else
        LOG_DEBUG(log, "Freezing parts with partition ID " + partition_id);

    String clickhouse_path = Poco::Path(context.getPath()).makeAbsolute().toString();
    String shadow_path = clickhouse_path + "shadow/";
    Poco::File(shadow_path).createDirectories();
    String backup_path = shadow_path
        + (!with_name.empty()
            ? escapeForFileName(with_name)
            : toString(Increment(shadow_path + "increment.txt").get(true)))
        + "/";

    LOG_DEBUG(log, "Snapshot will be placed at " + backup_path);

    /// Acquire a snapshot of active data parts to prevent removing while doing backup.
    const auto data_parts = getDataParts();

    size_t parts_processed = 0;
    for (const auto & part : data_parts)
    {
        if (prefix)
        {
            if (!startsWith(part->info.partition_id, *prefix))
                continue;
        }
        else if (part->info.partition_id != partition_id)
            continue;

        LOG_DEBUG(log, "Freezing part " << part->name);

        String part_absolute_path = Poco::Path(part->getFullPath()).absolute().toString();
        if (!startsWith(part_absolute_path, clickhouse_path))
            throw Exception("Part path " + part_absolute_path + " is not inside " + clickhouse_path, ErrorCodes::LOGICAL_ERROR);

        String backup_part_absolute_path = part_absolute_path;
        backup_part_absolute_path.replace(0, clickhouse_path.size(), backup_path);
        localBackup(part_absolute_path, backup_part_absolute_path);
        ++parts_processed;
    }

    LOG_DEBUG(log, "Freezed " << parts_processed << " parts");
}

size_t MergeTreeData::getPartitionSize(const std::string & partition_id) const
{
    size_t size = 0;

    Poco::DirectoryIterator end;

    for (Poco::DirectoryIterator it(full_path); it != end; ++it)
    {
        MergeTreePartInfo part_info;
        if (!MergeTreePartInfo::tryParsePartName(it.name(), &part_info, format_version))
            continue;
        if (part_info.partition_id != partition_id)
            continue;

        const auto part_path = it.path().absolute().toString();
        for (Poco::DirectoryIterator it2(part_path); it2 != end; ++it2)
        {
            const auto part_file_path = it2.path().absolute().toString();
            size += Poco::File(part_file_path).getSize();
        }
    }

    return size;
}

String MergeTreeData::getPartitionIDFromQuery(const ASTPtr & ast, const Context & context)
{
    const auto & partition_ast = typeid_cast<const ASTPartition &>(*ast);

    if (!partition_ast.value)
        return partition_ast.id;

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// Month-partitioning specific - partition ID can be passed in the partition value.
        const auto * partition_lit = typeid_cast<const ASTLiteral *>(partition_ast.value.get());
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

    size_t fields_count = partition_key_sample.columns();
    if (partition_ast.fields_count != fields_count)
        throw Exception(
            "Wrong number of fields in the partition expression: " + toString(partition_ast.fields_count) +
            ", must be: " + toString(fields_count),
            ErrorCodes::INVALID_PARTITION_VALUE);

    Row partition_row(fields_count);

    if (fields_count)
    {
        ReadBufferFromMemory left_paren_buf("(", 1);
        ReadBufferFromMemory fields_buf(partition_ast.fields_str.data, partition_ast.fields_str.size);
        ReadBufferFromMemory right_paren_buf(")", 1);
        ConcatReadBuffer buf({&left_paren_buf, &fields_buf, &right_paren_buf});

        ValuesRowInputStream input_stream(buf, partition_key_sample, context, /* interpret_expressions = */true);
        MutableColumns columns = partition_key_sample.cloneEmptyColumns();

        if (!input_stream.read(columns))
            throw Exception(
                "Could not parse partition value: `" + partition_ast.fields_str.toString() + "`",
                ErrorCodes::INVALID_PARTITION_VALUE);

        for (size_t i = 0; i < fields_count; ++i)
            columns[i]->get(0, partition_row[i]);
    }

    MergeTreePartition partition(std::move(partition_row));
    String partition_id = partition.getID(*this);

    {
        std::lock_guard<std::mutex> data_parts_lock(data_parts_mutex);
        DataPartPtr existing_part_in_partition = getAnyPartInPartition(partition_id, data_parts_lock);
        if (existing_part_in_partition && existing_part_in_partition->partition.value != partition.value)
        {
            WriteBufferFromOwnString buf;
            writeCString("Parsed partition value: ", buf);
            partition.serializeTextQuoted(*this, buf);
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
        std::lock_guard<std::mutex> lock(data_parts_mutex);

        for (auto state : affordable_states)
        {
            buf = std::move(res);
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
        std::lock_guard<std::mutex> lock(data_parts_mutex);
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

MergeTreeData::DataParts MergeTreeData::getDataParts(const DataPartStates & affordable_states) const
{
    DataParts res;
    {
        std::lock_guard<std::mutex> lock(data_parts_mutex);
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
    const String & partition_id, std::lock_guard<std::mutex> & /*data_parts_lock*/)
{
    auto min_block = std::numeric_limits<Int64>::min();
    MergeTreePartInfo dummy_part_info(partition_id, min_block, min_block, 0);

    auto it = data_parts_by_state_and_info.lower_bound(DataPartStateAndInfo(DataPartState::Committed, dummy_part_info));

    if (it != data_parts_by_state_and_info.end() && (*it)->state == DataPartState::Committed && (*it)->info.partition_id == partition_id)
        return *it;

    return nullptr;
}

void MergeTreeData::Transaction::rollback()
{
    if (!isEmpty())
    {
        std::stringstream ss;
        ss << " Removing parts:";
        for (const auto & part : precommitted_parts)
            ss << " " << part->relative_path;
        ss << ".";
        LOG_DEBUG(data->log, "Undoing transaction." << ss.str());

        data->removePartsFromWorkingSet(
            DataPartsVector(precommitted_parts.begin(), precommitted_parts.end()),
            /* clear_without_timeout = */ true);
    }

    clear();
}

MergeTreeData::DataPartsVector MergeTreeData::Transaction::commit()
{
    DataPartsVector total_covered_parts;

    if (!isEmpty())
    {
        std::lock_guard<std::mutex> data_parts_lock(data->data_parts_mutex);

        auto current_time = time(nullptr);
        for (const DataPartPtr & part : precommitted_parts)
        {
            DataPartPtr covering_part;
            DataPartsVector covered_parts = data->getActivePartsToReplace(part->info, part->name, covering_part, data_parts_lock);
            if (covering_part)
            {
                LOG_WARNING(data->log, "Tried to commit obsolete part " << part->name
                    << " covered by " << covering_part->getNameWithState());

                part->remove_time.store(0, std::memory_order_relaxed); /// The part will be removed without waiting for old_parts_lifetime seconds.
                data->modifyPartState(part, DataPartState::Outdated);
            }
            else
            {
                total_covered_parts.insert(total_covered_parts.end(), covered_parts.begin(), covered_parts.end());
                for (const DataPartPtr & covered_part : covered_parts)
                {
                    covered_part->remove_time.store(current_time, std::memory_order_relaxed);
                    data->modifyPartState(covered_part, DataPartState::Outdated);
                    data->removePartContributionToColumnSizes(covered_part);
                }

                data->modifyPartState(part, DataPartState::Committed);
                data->addPartContributionToColumnSizes(part);
            }
        }
    }

    clear();

    return total_covered_parts;
}

bool MergeTreeData::isPrimaryKeyColumnPossiblyWrappedInFunctions(const ASTPtr & node) const
{
    String column_name = node->getColumnName();

    for (const auto & column : primary_sort_descr)
        if (column_name == column.column_name)
            return true;

    if (const ASTFunction * func = typeid_cast<const ASTFunction *>(node.get()))
        if (func->arguments->children.size() == 1)
            return isPrimaryKeyColumnPossiblyWrappedInFunctions(func->arguments->children.front());

    return false;
}

bool MergeTreeData::mayBenefitFromIndexForIn(const ASTPtr & left_in_operand) const
{
    /// Make sure that the left side of the IN operator is part of the primary key.
    /// If there is a tuple on the left side of the IN operator, each item of the tuple must be part of the primary key.
    const ASTFunction * left_in_operand_tuple = typeid_cast<const ASTFunction *>(left_in_operand.get());
    if (left_in_operand_tuple && left_in_operand_tuple->name == "tuple")
    {
        for (const auto & item : left_in_operand_tuple->arguments->children)
            if (!isPrimaryKeyColumnPossiblyWrappedInFunctions(item))
                /// The tuple itself may be part of the primary key, so check that as a last resort.
                return isPrimaryKeyColumnPossiblyWrappedInFunctions(left_in_operand);

        /// tuple() is invalid but can still be found here since this method may be called before the arguments are validated.
        return !left_in_operand_tuple->arguments->children.empty();
    }
    else
    {
        return isPrimaryKeyColumnPossiblyWrappedInFunctions(left_in_operand);
    }
}

}
