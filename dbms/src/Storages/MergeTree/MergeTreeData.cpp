#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreePartChecker.h>
#include <Storages/AlterCommands.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTNameTypePair.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/HexWriteBuffer.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/localBackup.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Poco/DirectoryIterator.h>
#include <Common/Increment.h>
#include <Common/SimpleIncrement.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils.h>
#include <Common/Stopwatch.h>
#include <IO/Operators.h>

#include <algorithm>
#include <iomanip>
#include <thread>
#include <typeinfo>
#include <typeindex>


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
}


MergeTreeData::MergeTreeData(
    const String & database_, const String & table_,
    const String & full_path_, NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    Context & context_,
    ASTPtr & primary_expr_ast_,
    const String & date_column_name_, const ASTPtr & sampling_expression_,
    size_t index_granularity_,
    const MergingParams & merging_params_,
    const MergeTreeSettings & settings_,
    const String & log_name_,
    bool require_part_metadata_,
    bool attach,
    BrokenPartCallback broken_part_callback_)
    : ITableDeclaration{materialized_columns_, alias_columns_, column_defaults_}, context(context_),
    date_column_name(date_column_name_), sampling_expression(sampling_expression_),
    index_granularity(index_granularity_),
    merging_params(merging_params_),
    settings(settings_), primary_expr_ast(primary_expr_ast_ ? primary_expr_ast_->clone() : nullptr),
    require_part_metadata(require_part_metadata_),
    database_name(database_), table_name(table_),
    full_path(full_path_), columns(columns_),
    broken_part_callback(broken_part_callback_),
    log_name(log_name_), log(&Logger::get(log_name + " (Data)"))
{
    /// Check that the date column exists and is of type Date.
    const auto check_date_exists = [this] (const NamesAndTypesList & columns)
    {
        for (const auto & column : columns)
        {
            if (column.name == date_column_name)
            {
                if (!typeid_cast<const DataTypeDate *>(column.type.get()))
                    throw Exception("Date column (" + date_column_name + ") for storage of MergeTree family must have type Date."
                        " Provided column of type " + column.type->getName() + "."
                        " You may have separate column with type " + column.type->getName() + ".", ErrorCodes::BAD_TYPE_OF_FIELD);
                return true;
            }
        }

        return false;
    };

    if (!check_date_exists(*columns) && !check_date_exists(materialized_columns))
        throw Exception{
            "Date column (" + date_column_name + ") does not exist in table declaration.",
            ErrorCodes::NO_SUCH_COLUMN_IN_TABLE};

    checkNoMultidimensionalArrays(*columns, attach);
    checkNoMultidimensionalArrays(materialized_columns, attach);

    merging_params.check(*columns);

    if (!primary_expr_ast && merging_params.mode != MergingParams::Unsorted)
        throw Exception("Primary key could be empty only for UnsortedMergeTree", ErrorCodes::BAD_ARGUMENTS);

    initPrimaryKey();

    /// Creating directories, if not exist.
    Poco::File(full_path).createDirectories();
    Poco::File(full_path + "detached").createDirectory();
}


void MergeTreeData::checkNoMultidimensionalArrays(const NamesAndTypesList & columns, bool attach) const
{
    for (const auto & column : columns)
    {
        if (const auto * array = dynamic_cast<const DataTypeArray *>(column.type.get()))
        {
            if (dynamic_cast<const DataTypeArray *>(array->getNestedType().get()))
            {
                std::string message =
                    "Column " + column.name + " is a multidimensional array. "
                    + "Multidimensional arrays are not supported in MergeTree engines.";
                if (!attach)
                    throw Exception(message, ErrorCodes::BAD_TYPE_OF_FIELD);
                else
                    LOG_ERROR(log, message);
            }
        }
    }
}


void MergeTreeData::initPrimaryKey()
{
    if (!primary_expr_ast)
        return;

    /// Initialize description of sorting.
    sort_descr.clear();
    sort_descr.reserve(primary_expr_ast->children.size());
    for (const ASTPtr & ast : primary_expr_ast->children)
    {
        String name = ast->getColumnName();
        sort_descr.emplace_back(name, 1, 1);
    }

    primary_expr = ExpressionAnalyzer(primary_expr_ast, context, nullptr, getColumnsList()).getActions(false);

    ExpressionActionsPtr projected_expr = ExpressionAnalyzer(primary_expr_ast, context, nullptr, getColumnsList()).getActions(true);
    primary_key_sample = projected_expr->getSampleBlock();

    size_t primary_key_size = primary_key_sample.columns();

    /// A primary key cannot contain constants. It is meaningless.
    ///  (And also couldn't work because primary key is serialized with method of IDataType that doesn't support constants).
    /// Also a primary key must not contain any nullable column.
    for (size_t i = 0; i < primary_key_size; ++i)
    {
        const auto & element = primary_key_sample.getByPosition(i);

        const ColumnPtr & column = element.column;
        if (column && column->isConst())
                throw Exception{"Primary key cannot contain constants", ErrorCodes::ILLEGAL_COLUMN};

        if (element.type->isNullable())
            throw Exception{"Primary key cannot contain nullable columns", ErrorCodes::ILLEGAL_COLUMN};
    }

    primary_key_data_types.resize(primary_key_size);
    for (size_t i = 0; i < primary_key_size; ++i)
        primary_key_data_types[i] = primary_key_sample.getByPosition(i).type;
}


void MergeTreeData::MergingParams::check(const NamesAndTypesList & columns) const
{
    /// Check that if the sign column is needed, it exists and is of type Int8.
    if (mode == MergingParams::Collapsing)
    {
        if (sign_column.empty())
            throw Exception("Logical error: Sign column for storage CollapsingMergeTree is empty", ErrorCodes::LOGICAL_ERROR);

        for (const auto & column : columns)
        {
            if (column.name == sign_column)
            {
                if (!typeid_cast<const DataTypeInt8 *>(column.type.get()))
                    throw Exception("Sign column (" + sign_column + ")"
                        " for storage CollapsingMergeTree must have type Int8."
                        " Provided column of type " + column.type->getName() + ".", ErrorCodes::BAD_TYPE_OF_FIELD);
                break;
            }
        }
    }
    else if (!sign_column.empty())
        throw Exception("Sign column for MergeTree cannot be specified in all modes except Collapsing.", ErrorCodes::LOGICAL_ERROR);

    /// If colums_to_sum are set, then check that such columns exist.
    if (!columns_to_sum.empty())
    {
        if (mode != MergingParams::Summing)
            throw Exception("List of columns to sum for MergeTree cannot be specified in all modes except Summing.",
                ErrorCodes::LOGICAL_ERROR);

        for (const auto & column_to_sum : columns_to_sum)
            if (columns.end() == std::find_if(columns.begin(), columns.end(),
                [&](const NameAndTypePair & name_and_type) { return column_to_sum == name_and_type.name; }))
                throw Exception("Column " + column_to_sum + " listed in columns to sum does not exist in table declaration.");
    }

    /// Check that version_column column is set only for Replacing mode and is of unsigned integer type.
    if (!version_column.empty())
    {
        if (mode != MergingParams::Replacing)
            throw Exception("Version column for MergeTree cannot be specified in all modes except Replacing.",
                ErrorCodes::LOGICAL_ERROR);

        for (const auto & column : columns)
        {
            if (column.name == version_column)
            {
                if (!typeid_cast<const DataTypeUInt8 *>(column.type.get())
                    && !typeid_cast<const DataTypeUInt16 *>(column.type.get())
                    && !typeid_cast<const DataTypeUInt32 *>(column.type.get())
                    && !typeid_cast<const DataTypeUInt64 *>(column.type.get())
                    && !typeid_cast<const DataTypeDate *>(column.type.get())
                    && !typeid_cast<const DataTypeDateTime *>(column.type.get()))
                    throw Exception("Version column (" + version_column + ")"
                        " for storage ReplacingMergeTree must have type of UInt family or Date or DateTime."
                        " Provided column of type " + column.type->getName() + ".", ErrorCodes::BAD_TYPE_OF_FIELD);
                break;
            }
        }
    }

    /// TODO Checks for Graphite mode.
}


String MergeTreeData::MergingParams::getModeName() const
{
    switch (mode)
    {
        case Ordinary:         return "";
        case Collapsing:     return "Collapsing";
        case Summing:         return "Summing";
        case Aggregating:     return "Aggregating";
        case Unsorted:         return "Unsorted";
        case Replacing:     return "Replacing";
        case Graphite:         return "Graphite";

        default:
            throw Exception("Unknown mode of operation for MergeTreeData: " + toString<int>(mode), ErrorCodes::LOGICAL_ERROR);
    }
}


Int64 MergeTreeData::getMaxDataPartIndex()
{
    std::lock_guard<std::mutex> lock_all(all_data_parts_mutex);

    Int64 max_part_id = 0;
    for (const auto & part : all_data_parts)
        max_part_id = std::max(max_part_id, part->right);

    return max_part_id;
}


void MergeTreeData::loadDataParts(bool skip_sanity_checks)
{
    LOG_DEBUG(log, "Loading data parts");

    std::lock_guard<std::mutex> lock(data_parts_mutex);
    std::lock_guard<std::mutex> lock_all(all_data_parts_mutex);

    data_parts.clear();
    all_data_parts.clear();

    Strings part_file_names;
    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator it(full_path); it != end; ++it)
    {
        /// Skip temporary directories older than one day.
        if (startsWith(it.name(), "tmp_"))
            continue;

        part_file_names.push_back(it.name());
    }

    DataPartsVector broken_parts_to_remove;
    DataPartsVector broken_parts_to_detach;
    size_t suspicious_broken_parts = 0;

    for (const String & file_name : part_file_names)
    {
        MutableDataPartPtr part = std::make_shared<DataPart>(*this);
        if (!ActiveDataPartSet::parsePartNameImpl(file_name, part.get()))
            continue;

        part->name = file_name;
        bool broken = false;

        try
        {
            part->loadColumns(require_part_metadata);
            part->loadChecksums(require_part_metadata);
            part->loadIndex();
            part->checkNotBroken(require_part_metadata);
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
            if (part->level == 0)
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
                ++suspicious_broken_parts;

                for (const String & contained_name : part_file_names)
                {
                    if (contained_name == file_name)
                        continue;

                    DataPart contained_part(*this);
                    if (!ActiveDataPartSet::parsePartNameImpl(contained_name, &contained_part))
                        continue;

                    if (part->contains(contained_part))
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
                }
            }

            continue;
        }

        part->modification_time = Poco::File(full_path + file_name).getLastModified().epochTime();

        data_parts.insert(part);
    }

    if (suspicious_broken_parts > settings.max_suspicious_broken_parts && !skip_sanity_checks)
        throw Exception("Suspiciously many (" + toString(suspicious_broken_parts) + ") broken parts to remove.",
            ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);

    for (const auto & part : broken_parts_to_remove)
        part->remove();
    for (const auto & part : broken_parts_to_detach)
        part->renameAddPrefix(true, "");

    all_data_parts = data_parts;

    /// Delete from the set of current parts those parts that are covered by another part (those parts that
    /// were merged), but that for some reason are still not deleted from the file system.
    /// Deletion of files will be performed later in the clearOldParts() method.

    if (data_parts.size() >= 2)
    {
        DataParts::iterator prev_jt = data_parts.begin();
        DataParts::iterator curr_jt = prev_jt;
        ++curr_jt;
        while (curr_jt != data_parts.end())
        {
            /// Don't consider data parts belonging to different months.
            if ((*curr_jt)->month != (*prev_jt)->month)
            {
                ++prev_jt;
                ++curr_jt;
                continue;
            }

            if ((*curr_jt)->contains(**prev_jt))
            {
                (*prev_jt)->remove_time = (*prev_jt)->modification_time;
                data_parts.erase(prev_jt);
                prev_jt = curr_jt;
                ++curr_jt;
            }
            else if ((*prev_jt)->contains(**curr_jt))
            {
                (*curr_jt)->remove_time = (*curr_jt)->modification_time;
                data_parts.erase(curr_jt++);
            }
            else
            {
                ++prev_jt;
                ++curr_jt;
            }
        }
    }

    calculateColumnSizesImpl();

    LOG_DEBUG(log, "Loaded data parts (" << data_parts.size() << " items)");
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


void MergeTreeData::clearOldTemporaryDirectories()
{
    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock<std::mutex> lock(clear_old_temporary_directories_mutex, std::defer_lock);
    if (!lock.try_lock())
        return;

    time_t current_time = time(0);

    /// Delete temporary directories older than a day.
    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator it{full_path}; it != end; ++it)
    {
        if (startsWith(it.name(), "tmp_"))
        {
            Poco::File tmp_dir(full_path + it.name());

            try
            {
                if (tmp_dir.isDirectory() && isOldPartDirectory(tmp_dir, current_time - settings.temporary_directories_lifetime))
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

    time_t now = time(0);

    {
        std::lock_guard<std::mutex> lock(all_data_parts_mutex);

        for (DataParts::iterator it = all_data_parts.begin(); it != all_data_parts.end();)
        {
            if (it->unique() && /// After this ref_count cannot increase.
                (*it)->remove_time < now &&
                now - (*it)->remove_time > settings.old_parts_lifetime)
            {
                res.push_back(*it);
                all_data_parts.erase(it++);
            }
            else
                ++it;
        }
    }

    if (!res.empty())
        LOG_TRACE(log, "Found " << res.size() << " old parts to remove.");

    return res;
}


void MergeTreeData::addOldParts(const MergeTreeData::DataPartsVector & parts)
{
    std::lock_guard<std::mutex> lock(all_data_parts_mutex);
    all_data_parts.insert(parts.begin(), parts.end());
}

void MergeTreeData::clearOldParts()
{
    auto parts_to_remove = grabOldParts();

    for (const DataPartPtr & part : parts_to_remove)
    {
        LOG_DEBUG(log, "Removing part " << part->name);
        part->remove();
    }
}

void MergeTreeData::setPath(const String & new_full_path, bool move_data)
{
    if (move_data)
    {
        if (Poco::File{new_full_path}.exists())
            throw Exception{
                "Target path already exists: " + new_full_path,
                /// @todo existing target can also be a file, not directory
                ErrorCodes::DIRECTORY_ALREADY_EXISTS
            };
        Poco::File(full_path).renameTo(new_full_path);
        /// If we don't need to move the data, it means someone else has already moved it.
        /// We hope that he has also reset the caches.
        context.resetCaches();
    }

    full_path = new_full_path;
}

void MergeTreeData::dropAllData()
{
    LOG_TRACE(log, "dropAllData: waiting for locks.");

    std::lock_guard<std::mutex> lock(data_parts_mutex);
    std::lock_guard<std::mutex> lock_all(all_data_parts_mutex);

    LOG_TRACE(log, "dropAllData: removing data from memory.");

    data_parts.clear();
    all_data_parts.clear();
    column_sizes.clear();

    context.resetCaches();

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
    auto new_columns = *columns;
    auto new_materialized_columns = materialized_columns;
    auto new_alias_columns = alias_columns;
    auto new_column_defaults = column_defaults;
    commands.apply(new_columns, new_materialized_columns, new_alias_columns, new_column_defaults);

    checkNoMultidimensionalArrays(new_columns, /* attach = */ false);
    checkNoMultidimensionalArrays(new_materialized_columns, /* attach = */ false);

    /// Set of columns that shouldn't be altered.
    NameSet columns_alter_forbidden;

    columns_alter_forbidden.insert(date_column_name);

    if (!merging_params.sign_column.empty())
        columns_alter_forbidden.insert(merging_params.sign_column);

    /// Primary key columns can be ALTERed only if they are used in the primary key as-is
    /// (and not as a part of some expression) and if the ALTER only affects column metadata.
    /// We don't add sampling_expression columns here because they must be among the primary key columns.
    NameSet columns_alter_metadata_only;

    if (primary_expr)
    {
        for (const auto & action : primary_expr->getActions())
        {
            auto action_columns = action.getNeededColumns();
            columns_alter_forbidden.insert(action_columns.begin(), action_columns.end());
        }
        for (const auto & col : primary_expr->getRequiredColumns())
        {
            if (!columns_alter_forbidden.count(col))
                columns_alter_metadata_only.insert(col);
        }
    }

    std::map<String, const IDataType *> old_types;
    for (const auto & column : *columns)
    {
        old_types.emplace(column.name, column.type.get());
    }

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

    /// augment plain columns with materialized columns for convert expression creation
    new_columns.insert(std::end(new_columns),
        std::begin(new_materialized_columns), std::end(new_materialized_columns));

    createConvertExpression(nullptr, getColumnsList(), new_columns, unused_expression, unused_map, unused_bool);
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

    /// The number of columns in each Nested structure. Columns not belonging to Nested structure will also be
    /// in this map, but they won't interfere with the computation.
    std::map<String, size_t> nested_table_counts;
    for (const NameAndTypePair & column : old_columns)
        ++nested_table_counts[DataTypeNested::extractNestedTableName(column.name)];

    /// For every column that need to be converted: source column name, column name of calculated expression for conversion.
    std::vector<std::pair<String, String>> conversions;

    for (const NameAndTypePair & column : old_columns)
    {
        if (!new_types.count(column.name))
        {
            bool is_nullable = column.type.get()->isNullable();

            if (!part || part->hasColumnFiles(column.name))
            {
                /// The column must be deleted.
                const IDataType * observed_type;
                if (is_nullable)
                {
                    const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*column.type);
                    observed_type = nullable_type.getNestedType().get();
                }
                else
                    observed_type = column.type.get();

                String escaped_column = escapeForFileName(column.name);
                out_rename_map[escaped_column + ".bin"] = "";
                out_rename_map[escaped_column + ".mrk"] = "";

                if (is_nullable)
                {
                    out_rename_map[escaped_column + ".null.bin"] = "";
                    out_rename_map[escaped_column + ".null.mrk"] = "";
                }

                /// If the column is an array or the last column of nested structure, we must delete the
                /// sizes file.
                if (typeid_cast<const DataTypeArray *>(observed_type))
                {
                    String nested_table = DataTypeNested::extractNestedTableName(column.name);
                    /// If it was the last column referencing these .size0 files, delete them.
                    if (!--nested_table_counts[nested_table])
                    {
                        String escaped_nested_table = escapeForFileName(nested_table);
                        out_rename_map[escaped_nested_table + ".size0.bin"] = "";
                        out_rename_map[escaped_nested_table + ".size0.mrk"] = "";
                    }
                }
            }
        }
        else
        {
            const auto * new_type = new_types[column.name];
            const String new_type_name = new_type->getName();
            const auto * old_type = column.type.get();

            if (new_type_name != old_type->getName() && (!part || part->hasColumnFiles(column.name)))
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

                /// @todo invent the name more safely
                const auto new_type_name_column = '#' + new_type_name + "_column";
                out_expression->add(ExpressionAction::addColumn(
                    { std::make_shared<ColumnConstString>(1, new_type_name), std::make_shared<DataTypeString>(), new_type_name_column }));

                const FunctionPtr & function = FunctionFactory::instance().get("CAST", context);
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
            String converting_column_name = source_and_expression.first + " converting";
            projection.emplace_back(source_and_expression.second, converting_column_name);

            const String escaped_converted_column = escapeForFileName(converting_column_name);
            const String escaped_source_column = escapeForFileName(source_and_expression.first);

            /// After conversion, we need to rename temporary files into original.
            out_rename_map[escaped_converted_column + ".bin"] = escaped_source_column + ".bin";
            out_rename_map[escaped_converted_column + ".mrk"] = escaped_source_column + ".mrk";

            const IDataType * new_type = new_types[source_and_expression.first];

            /// NOTE Sizes of arrays are not updated during conversion.

            /// Information on how to update the null map if it is a nullable column.
            if (new_type->isNullable())
            {
                out_rename_map[escaped_converted_column + ".null.bin"] = escaped_source_column + ".null.bin";
                out_rename_map[escaped_converted_column + ".null.mrk"] = escaped_source_column + ".null.mrk";
            }
        }

        out_expression->add(ExpressionAction::project(projection));
    }

    if (part && !out_rename_map.empty())
    {
        std::string message;
        {
            WriteBufferFromString out(message);
            out << "Will rename ";
            bool first = true;
            for (const auto & from_to : out_rename_map)
            {
                if (!first)
                    out << ", ";
                first = false;
                out << from_to.first << " to " << from_to.second;
            }
            out << " in part " << part->name;
        }
        LOG_DEBUG(log, message);
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
    uint128 new_primary_key_hash{};

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
                new_index[i] = type.createConstColumn(part->size, type.getDefault())->convertToFullColumnIfConst();
            }
        }

        if (prev_position_of_existing_column == -1)
            throw Exception("No common columns while modifying primary key", ErrorCodes::BAD_ARGUMENTS);

        String index_tmp_path = full_path + part->name + "/primary.idx.tmp";
        WriteBufferFromFile index_file(index_tmp_path);
        HashingWriteBuffer index_stream(index_file);

        for (size_t i = 0, size = part->size; i < size; ++i)
            for (size_t j = 0; j < new_key_size; ++j)
                new_primary_key_sample.getByPosition(j).type.get()->serializeBinary(*new_index[j].get(), i, index_stream);

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
        MarkRanges ranges{MarkRange(0, part->size)};
        BlockInputStreamPtr part_in = std::make_shared<MergeTreeBlockInputStream>(
            *this, part, DEFAULT_MERGE_BLOCK_SIZE, 0, expression->getRequiredColumns(), ranges,
            false, nullptr, "", false, 0, DBMS_DEFAULT_BUFFER_SIZE, false);

        ExpressionBlockInputStream in(part_in, expression);
        MergedColumnOnlyOutputStream out(*this, full_path + part->name + '/', true, CompressionMethod::LZ4, false);
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
        if (it.second == "")
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
        Poco::ScopedWriteRWLock lock(data_part->columns_lock);

        String path = data_part->storage.full_path + data_part->name + "/";

        /// NOTE: checking that a file exists before renaming or deleting it
        /// is justified by the fact that, when converting an ordinary column
        /// to a nullable column, new files are created which did not exist
        /// before, i.e. they do not have older versions.

        /// 1) Rename the old files.
        for (auto it : rename_map)
        {
            String name = it.second.empty() ? it.first : it.second;
            Poco::File file{path + name};
            if (file.exists())
                file.renameTo(path + name + ".tmp2");
        }

        /// 2) Move new files in the place of old and update the metadata in memory.
        for (auto it : rename_map)
        {
            if (!it.second.empty())
                Poco::File{path + it.first}.renameTo(path + it.second);
        }

        DataPart & mutable_part = const_cast<DataPart &>(*data_part);
        mutable_part.checksums = new_checksums;
        mutable_part.columns = new_columns;

        /// 3) Delete the old files.
        for (auto it : rename_map)
        {
            String name = it.second.empty() ? it.first : it.second;
            Poco::File file{path + name + ".tmp2"};
            if (file.exists())
                file.remove();
        }

        mutable_part.size_in_bytes = MergeTreeData::DataPart::calcTotalSize(path);

        /// TODO: we can skip resetting caches when the column is added.
        data_part->storage.context.resetCaches();

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

        LOG_WARNING(data_part->storage.log, "Aborting ALTER of part " << data_part->name);

        String path = data_part->storage.full_path + data_part->name + "/";
        for (auto it : rename_map)
        {
            if (!it.second.empty())
            {
                try
                {
                    Poco::File file(path + it.first);
                    if (file.exists())
                        file.remove();
                }
                catch (Poco::Exception & e)
                {
                    LOG_WARNING(data_part->storage.log, "Can't remove " << path + it.first << ": " << e.displayText());
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
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
    if (out_transaction && out_transaction->data)
        throw Exception("Using the same MergeTreeData::Transaction for overlapping transactions is invalid", ErrorCodes::LOGICAL_ERROR);

    LOG_TRACE(log, "Renaming " << part->name << ".");

    String old_name = part->name;
    String old_path = getFullPath() + old_name + "/";

    DataPartsVector replaced;
    {
        std::lock_guard<std::mutex> lock(data_parts_mutex);

        /** It is important that obtaining new block number and adding that block to parts set is done atomically.
          * Otherwise there is race condition - merge of blocks could happen in interval that doesn't yet contain new part.
          */
        if (increment)
            part->left = part->right = increment->get();

        String new_name = ActiveDataPartSet::getPartName(part->left_date, part->right_date, part->left, part->right, part->level);

        part->is_temp = false;
        part->name = new_name;
        bool duplicate = data_parts.count(part);
        part->name = old_name;
        part->is_temp = true;

        if (duplicate)
            throw Exception("Part " + new_name + " already exists", ErrorCodes::DUPLICATE_DATA_PART);

        String new_path = getFullPath() + new_name + "/";

        /// Rename the part.
        Poco::File(old_path).renameTo(new_path);

        part->is_temp = false;
        part->name = new_name;

        bool obsolete = false; /// Is the part covered by some other part?

        /// Parts contained in the part are consecutive in data_parts, intersecting the insertion place
        /// for the part itself.
        DataParts::iterator it = data_parts.lower_bound(part);
        /// Go to the left.
        while (it != data_parts.begin())
        {
            --it;
            if (!part->contains(**it))
            {
                if ((*it)->contains(*part))
                    obsolete = true;
                ++it;
                break;
            }
            replaced.push_back(*it);
            (*it)->remove_time = time(0);
            removePartContributionToColumnSizes(*it);
            data_parts.erase(it++); /// Yes, ++, not --.
        }
        std::reverse(replaced.begin(), replaced.end()); /// Parts must be in ascending order.
        /// Go to the right.
        while (it != data_parts.end())
        {
            if (!part->contains(**it))
            {
                if ((*it)->name == part->name || (*it)->contains(*part))
                    obsolete = true;
                break;
            }
            replaced.push_back(*it);
            (*it)->remove_time = time(0);
            removePartContributionToColumnSizes(*it);
            data_parts.erase(it++);
        }

        if (obsolete)
        {
            LOG_WARNING(log, "Obsolete part " << part->name << " added");
            part->remove_time = time(0);
        }
        else
        {
            data_parts.insert(part);
            addPartContributionToColumnSizes(part);
        }

        {
            std::lock_guard<std::mutex> lock_all(all_data_parts_mutex);
            all_data_parts.insert(part);
        }
    }

    if (out_transaction)
    {
        out_transaction->data = this;
        out_transaction->parts_to_add_on_rollback = replaced;
        out_transaction->parts_to_remove_on_rollback = DataPartsVector(1, part);
    }

    return replaced;
}

void MergeTreeData::replaceParts(const DataPartsVector & remove, const DataPartsVector & add, bool clear_without_timeout)
{
    std::lock_guard<std::mutex> lock(data_parts_mutex);

    for (const DataPartPtr & part : remove)
    {
        part->remove_time = clear_without_timeout ? 0 : time(0);

        if (data_parts.erase(part))
            removePartContributionToColumnSizes(part);
    }

    for (const DataPartPtr & part : add)
    {
        if (data_parts.insert(part).second)
            addPartContributionToColumnSizes(part);
    }
}

void MergeTreeData::attachPart(const DataPartPtr & part)
{
    std::lock_guard<std::mutex> lock(data_parts_mutex);
    std::lock_guard<std::mutex> lock_all(all_data_parts_mutex);

    if (!all_data_parts.insert(part).second)
        throw Exception("Part " + part->name + " is already attached", ErrorCodes::DUPLICATE_DATA_PART);

    data_parts.insert(part);
    addPartContributionToColumnSizes(part);
}

void MergeTreeData::renameAndDetachPart(const DataPartPtr & part, const String & prefix, bool restore_covered, bool move_to_detached)
{
    LOG_INFO(log, "Renaming " << part->name << " to " << prefix << part->name << " and detaching it.");

    std::lock_guard<std::mutex> lock(data_parts_mutex);
    std::lock_guard<std::mutex> lock_all(all_data_parts_mutex);

    if (!all_data_parts.erase(part))
        throw Exception("No such data part", ErrorCodes::NO_SUCH_DATA_PART);

    removePartContributionToColumnSizes(part);
    data_parts.erase(part);
    if (move_to_detached || !prefix.empty())
        part->renameAddPrefix(move_to_detached, prefix);

    if (restore_covered)
    {
        auto it = all_data_parts.lower_bound(part);
        Strings restored;
        bool error = false;

        Int64 pos = part->left;

        if (it != all_data_parts.begin())
        {
            --it;
            if (part->contains(**it))
            {
                if ((*it)->left != part->left)
                    error = true;
                data_parts.insert(*it);
                addPartContributionToColumnSizes(*it);
                pos = (*it)->right + 1;
                restored.push_back((*it)->name);
            }
            else
                error = true;
            ++it;
        }
        else
            error = true;

        for (; it != all_data_parts.end() && part->contains(**it); ++it)
        {
            if ((*it)->left < pos)
                continue;
            if ((*it)->left > pos)
                error = true;
            data_parts.insert(*it);
            addPartContributionToColumnSizes(*it);
            pos = (*it)->right + 1;
            restored.push_back((*it)->name);
        }

        if (pos != part->right + 1)
            error = true;

        for (const String & name : restored)
        {
            LOG_INFO(log, "Activated part " << name);
        }

        if (error)
            LOG_ERROR(log, "The set of parts restored in place of " << part->name << " looks incomplete. There might or might not be a data loss.");
    }
}

void MergeTreeData::detachPartInPlace(const DataPartPtr & part)
{
    renameAndDetachPart(part, "", false, false);
}

MergeTreeData::DataParts MergeTreeData::getDataParts() const
{
    std::lock_guard<std::mutex> lock(data_parts_mutex);
    return data_parts;
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVector() const
{
    std::lock_guard<std::mutex> lock(data_parts_mutex);
    return DataPartsVector(std::begin(data_parts), std::end(data_parts));
}

size_t MergeTreeData::getTotalActiveSizeInBytes() const
{
    std::lock_guard<std::mutex> lock(data_parts_mutex);

    size_t res = 0;
    for (auto & part : data_parts)
        res += part->size_in_bytes;

    return res;
}

MergeTreeData::DataParts MergeTreeData::getAllDataParts() const
{
    std::lock_guard<std::mutex> lock(all_data_parts_mutex);
    return all_data_parts;
}

size_t MergeTreeData::getMaxPartsCountForMonth() const
{
    std::lock_guard<std::mutex> lock(data_parts_mutex);

    size_t res = 0;
    size_t cur_count = 0;
    DayNum_t cur_month = DayNum_t(0);

    for (const auto & part : data_parts)
    {
        if (part->month == cur_month)
        {
            ++cur_count;
        }
        else
        {
            cur_month = part->month;
            cur_count = 1;
        }

        res = std::max(res, cur_count);
    }

    return res;
}


std::pair<Int64, bool> MergeTreeData::getMinBlockNumberForMonth(DayNum_t month) const
{
    std::lock_guard<std::mutex> lock(all_data_parts_mutex);

    for (const auto & part : all_data_parts)    /// The search can be done better.
        if (part->month == month)
            return { part->left, true };    /// Blocks in data_parts are sorted by month and left.

    return { 0, false };
}


bool MergeTreeData::hasBlockNumberInMonth(Int64 block_number, DayNum_t month) const
{
    std::lock_guard<std::mutex> lock(data_parts_mutex);

    for (const auto & part : data_parts)    /// The search can be done better.
    {
        if (part->month == month && part->left <= block_number && part->right >= block_number)
            return true;

        if (part->month > month)
            break;
    }

    return false;
}

void MergeTreeData::delayInsertIfNeeded(Poco::Event * until)
{
    const size_t parts_count = getMaxPartsCountForMonth();
    if (parts_count < settings.parts_to_delay_insert)
        return;

    if (parts_count >= settings.parts_to_throw_insert)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception("Too much parts. Merges are processing significantly slower than inserts.", ErrorCodes::TOO_MUCH_PARTS);
    }

    const size_t max_k = settings.parts_to_throw_insert - settings.parts_to_delay_insert; /// always > 0
    const size_t k = 1 + parts_count - settings.parts_to_delay_insert; /// from 1 to max_k
    const double delay_sec = ::pow(settings.max_delay_to_insert, static_cast<double>(k) / max_k);

    ProfileEvents::increment(ProfileEvents::DelayedInserts);
    ProfileEvents::increment(ProfileEvents::DelayedInsertsMilliseconds, delay_sec * 1000);

    CurrentMetrics::Increment metric_increment(CurrentMetrics::DelayedInserts);

    LOG_INFO(log, "Delaying inserting block by "
        << std::fixed << std::setprecision(4) << delay_sec << " sec. because there are " << parts_count << " parts");

    if (until)
        until->tryWait(delay_sec * 1000);
    else
        std::this_thread::sleep_for(std::chrono::duration<double>(delay_sec));
}

MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(const String & part_name)
{
    MutableDataPartPtr tmp_part(new DataPart(*this));
    ActiveDataPartSet::parsePartName(part_name, *tmp_part);

    std::lock_guard<std::mutex> lock(data_parts_mutex);

    /// The part can be covered only by the previous or the next one in data_parts.
    DataParts::iterator it = data_parts.lower_bound(tmp_part);

    if (it != data_parts.end())
    {
        if ((*it)->name == part_name)
            return *it;
        if ((*it)->contains(*tmp_part))
            return *it;
    }

    if (it != data_parts.begin())
    {
        --it;
        if ((*it)->contains(*tmp_part))
            return *it;
    }

    return nullptr;
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExists(const String & part_name)
{
    MutableDataPartPtr tmp_part(new DataPart(*this));
    ActiveDataPartSet::parsePartName(part_name, *tmp_part);

    std::lock_guard<std::mutex> lock(all_data_parts_mutex);
    DataParts::iterator it = all_data_parts.lower_bound(tmp_part);
    if (it != all_data_parts.end() && (*it)->name == part_name)
        return *it;

    return nullptr;
}

MergeTreeData::DataPartPtr MergeTreeData::getShardedPartIfExists(const String & part_name, size_t shard_no)
{
    const MutableDataPartPtr & part_from_shard = per_shard_data_parts.at(shard_no);

    if (part_from_shard->name == part_name)
        return part_from_shard;
    else
        return nullptr;
}

MergeTreeData::MutableDataPartPtr MergeTreeData::loadPartAndFixMetadata(const String & relative_path)
{
    MutableDataPartPtr part = std::make_shared<DataPart>(*this);
    part->name = relative_path;
    ActiveDataPartSet::parsePartName(Poco::Path(relative_path).getFileName(), *part);

    /// Earlier the list of columns was written incorrectly. Delete it and re-create.
    if (Poco::File(full_path + relative_path + "/columns.txt").exists())
        Poco::File(full_path + relative_path + "/columns.txt").remove();

    part->loadColumns(false);
    part->loadChecksums(false);
    part->loadIndex();
    part->checkNotBroken(false);

    part->modification_time = Poco::File(full_path + relative_path).getLastModified().epochTime();

    /// If the checksums file is not present, calculate the checksums and write them to disk.
    /// Check the data while we are at it.
    if (part->checksums.empty())
    {
        MergeTreePartChecker::Settings settings;
        settings.setIndexGranularity(index_granularity);
        settings.setRequireColumnFiles(true);
        MergeTreePartChecker::checkDataPart(full_path + relative_path, settings, primary_key_data_types, &part->checksums);

        {
            WriteBufferFromFile out(full_path + relative_path + "/checksums.txt.tmp", 4096);
            part->checksums.write(out);
        }

        Poco::File(full_path + relative_path + "/checksums.txt.tmp").renameTo(full_path + relative_path + "/checksums.txt");
    }

    return part;
}


void MergeTreeData::calculateColumnSizesImpl()
{
    column_sizes.clear();

    for (const auto & part : data_parts)
        addPartContributionToColumnSizes(part);
}

void MergeTreeData::addPartContributionToColumnSizes(const DataPartPtr & part)
{
    const auto & files = part->checksums.files;

    /// TODO This method doesn't take into account columns with multiple files.
    for (const auto & column : getColumnsList())
    {
        const auto escaped_name = escapeForFileName(column.name);
        const auto bin_file_name = escaped_name + ".bin";
        const auto mrk_file_name = escaped_name + ".mrk";

        ColumnSize & column_size = column_sizes[column.name];

        if (files.count(bin_file_name))
        {
            const auto & bin_file_checksums = files.at(bin_file_name);
            column_size.data_compressed += bin_file_checksums.file_size;
            column_size.data_uncompressed += bin_file_checksums.uncompressed_size;
        }

        if (files.count(mrk_file_name))
            column_size.marks += files.at(mrk_file_name).file_size;
    }
}

static inline void logSubtract(size_t & from, size_t value, Logger * log, const String & variable)
{
    if (value > from)
        LOG_ERROR(log, "Possibly incorrect subtraction: " << from << " - " << value << " = " << from - value << ", variable " << variable);

    from -= value;
}

void MergeTreeData::removePartContributionToColumnSizes(const DataPartPtr & part)
{
    const auto & files = part->checksums.files;

    /// TODO This method doesn't take into account columns with multiple files.
    for (const auto & column : *columns)
    {
        const auto escaped_name = escapeForFileName(column.name);
        const auto bin_file_name = escaped_name + ".bin";
        const auto mrk_file_name = escaped_name + ".mrk";

        auto & column_size = column_sizes[column.name];

        if (files.count(bin_file_name))
        {
            const auto & bin_file_checksums = files.at(bin_file_name);
            logSubtract(column_size.data_compressed, bin_file_checksums.file_size, log, bin_file_name + ".file_size");
            logSubtract(column_size.data_uncompressed, bin_file_checksums.uncompressed_size, log, bin_file_name + ".uncompressed_size");
        }

        if (files.count(mrk_file_name))
            logSubtract(column_size.marks, files.at(mrk_file_name).file_size, log, mrk_file_name + ".file_size");
    }
}


void MergeTreeData::freezePartition(const std::string & prefix, const String & with_name)
{
    LOG_DEBUG(log, "Freezing parts with prefix " + prefix);

    String clickhouse_path = Poco::Path(context.getPath()).makeAbsolute().toString();
    String shadow_path = clickhouse_path + "shadow/";
    Poco::File(shadow_path).createDirectories();
    String backup_path = shadow_path
        + (!with_name.empty()
            ? escapeForFileName(with_name)
            : toString(Increment(shadow_path + "increment.txt").get(true)))
        + "/";

    LOG_DEBUG(log, "Snapshot will be placed at " + backup_path);

    size_t parts_processed = 0;
    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator it(full_path); it != end; ++it)
    {
        if (startsWith(it.name(), prefix))
        {
            LOG_DEBUG(log, "Freezing part " << it.name());

            String part_absolute_path = it.path().absolute().toString();
            if (!startsWith(part_absolute_path, clickhouse_path))
                throw Exception("Part path " + part_absolute_path + " is not inside " + clickhouse_path, ErrorCodes::LOGICAL_ERROR);

            String backup_part_absolute_path = part_absolute_path;
            backup_part_absolute_path.replace(0, clickhouse_path.size(), backup_path);
            localBackup(part_absolute_path, backup_part_absolute_path);
            ++parts_processed;
        }
    }

    LOG_DEBUG(log, "Freezed " << parts_processed << " parts");
}

size_t MergeTreeData::getPartitionSize(const std::string & partition_name) const
{
    size_t size = 0;

    Poco::DirectoryIterator end;
    Poco::DirectoryIterator end2;

    for (Poco::DirectoryIterator it(full_path); it != end; ++it)
    {
        const auto filename = it.name();
        if (!ActiveDataPartSet::isPartDirectory(filename))
            continue;
        if (!startsWith(filename, partition_name))
            continue;

        const auto part_path = it.path().absolute().toString();
        for (Poco::DirectoryIterator it2(part_path); it2 != end2; ++it2)
        {
            const auto part_file_path = it2.path().absolute().toString();
            size += Poco::File(part_file_path).getSize();
        }
    }

    return size;
}

static std::pair<String, DayNum_t> getMonthNameAndDayNum(const Field & partition)
{
    String month_name = partition.getType() == Field::Types::UInt64
        ? toString(partition.get<UInt64>())
        : partition.safeGet<String>();

    if (month_name.size() != 6 || !std::all_of(month_name.begin(), month_name.end(), isdigit))
        throw Exception("Invalid partition format: " + month_name + ". Partition should consist of 6 digits: YYYYMM",
            ErrorCodes::INVALID_PARTITION_NAME);

    DayNum_t date = DateLUT::instance().YYYYMMDDToDayNum(parse<UInt32>(month_name + "01"));

    /// Can't just compare date with 0, because 0 is a valid DayNum too.
    if (month_name != toString(DateLUT::instance().toNumYYYYMMDD(date) / 100))
        throw Exception("Invalid partition format: " + month_name + " doesn't look like month.",
            ErrorCodes::INVALID_PARTITION_NAME);

    return std::make_pair(month_name, date);
}


String MergeTreeData::getMonthName(const Field & partition)
{
    return getMonthNameAndDayNum(partition).first;
}

String MergeTreeData::getMonthName(DayNum_t month)
{
    return toString(DateLUT::instance().toNumYYYYMMDD(month) / 100);
}

DayNum_t MergeTreeData::getMonthDayNum(const Field & partition)
{
    return getMonthNameAndDayNum(partition).second;
}

DayNum_t MergeTreeData::getMonthFromName(const String & month_name)
{
    DayNum_t date = DateLUT::instance().YYYYMMDDToDayNum(parse<UInt32>(month_name + "01"));

    /// Can't just compare date with 0, because 0 is a valid DayNum too.
    if (month_name != toString(DateLUT::instance().toNumYYYYMMDD(date) / 100))
        throw Exception("Invalid partition format: " + month_name + " doesn't look like month.",
            ErrorCodes::INVALID_PARTITION_NAME);

    return date;
}

DayNum_t MergeTreeData::getMonthFromPartPrefix(const String & part_prefix)
{
    return getMonthFromName(part_prefix.substr(0, strlen("YYYYMM")));
}

}
