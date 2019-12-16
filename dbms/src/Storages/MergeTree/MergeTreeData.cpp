#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/MergeTree/MergeTreeSequentialBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/AlterCommands.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Formats/FormatFactory.h>
#include <DataStreams/copyData.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Compression/CompressedReadBuffer.h>
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
#include <Common/quoteString.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Stopwatch.h>
#include <Common/typeid_cast.h>
#include <Common/localBackup.h>
#include <Interpreters/PartLog.h>

#include <Poco/DirectoryIterator.h>

#include <boost/range/adaptor/filtered.hpp>

#include <algorithm>
#include <iomanip>
#include <set>
#include <thread>
#include <typeinfo>
#include <typeindex>
#include <optional>


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


namespace
{
    constexpr UInt64 RESERVATION_MIN_ESTIMATION_SIZE = 1u * 1024u * 1024u; /// 1MB
}


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int SYNTAX_ERROR;
    extern const int INVALID_PARTITION_VALUE;
    extern const int METADATA_MISMATCH;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int TOO_MANY_PARTS;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int CANNOT_UPDATE_COLUMN;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MUNMAP;
    extern const int CANNOT_MREMAP;
    extern const int BAD_TTL_EXPRESSION;
    extern const int INCORRECT_FILE_NAME;
    extern const int BAD_DATA_PART_NAME;
    extern const int UNKNOWN_SETTING;
    extern const int READONLY_SETTING;
    extern const int ABORTED;
}


namespace
{
    const char * DELETE_ON_DESTROY_MARKER_PATH = "delete-on-destroy.txt";
}


MergeTreeData::MergeTreeData(
    const String & database_,
    const String & table_,
    const ColumnsDescription & columns_,
    const IndicesDescription & indices_,
    const ConstraintsDescription & constraints_,
    Context & context_,
    const String & date_column_name,
    const ASTPtr & partition_by_ast_,
    const ASTPtr & order_by_ast_,
    const ASTPtr & primary_key_ast_,
    const ASTPtr & sample_by_ast_,
    const ASTPtr & ttl_table_ast_,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool require_part_metadata_,
    bool attach,
    BrokenPartCallback broken_part_callback_)
    : global_context(context_)
    , merging_params(merging_params_)
    , partition_by_ast(partition_by_ast_)
    , sample_by_ast(sample_by_ast_)
    , require_part_metadata(require_part_metadata_)
    , database_name(database_)
    , table_name(table_)
    , broken_part_callback(broken_part_callback_)
    , log_name(database_name + "." + table_name)
    , log(&Logger::get(log_name))
    , storage_settings(std::move(storage_settings_))
    , storage_policy(context_.getStoragePolicy(getSettings()->storage_policy))
    , data_parts_by_info(data_parts_indexes.get<TagByInfo>())
    , data_parts_by_state_and_info(data_parts_indexes.get<TagByStateAndInfo>())
    , parts_mover(this)
{
    const auto settings = getSettings();
    setProperties(order_by_ast_, primary_key_ast_, columns_, indices_, constraints_);

    /// NOTE: using the same columns list as is read when performing actual merges.
    merging_params.check(getColumns().getAllPhysical());

    if (sample_by_ast)
    {
        sampling_expr_column_name = sample_by_ast->getColumnName();

        if (!primary_key_sample.has(sampling_expr_column_name)
            && !attach && !settings->compatibility_allow_sampling_expression_not_in_primary_key) /// This is for backward compatibility.
            throw Exception("Sampling expression must be present in the primary key", ErrorCodes::BAD_ARGUMENTS);

        auto syntax = SyntaxAnalyzer(global_context).analyze(sample_by_ast, getColumns().getAllPhysical());
        columns_required_for_sampling = syntax->requiredSourceColumns();
    }

    MergeTreeDataFormatVersion min_format_version(0);
    if (!date_column_name.empty())
    {
        try
        {
            partition_by_ast = makeASTFunction("toYYYYMM", std::make_shared<ASTIdentifier>(date_column_name));
            initPartitionKey();

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
        initPartitionKey();
        min_format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    }

    setTTLExpressions(columns_.getColumnTTLs(), ttl_table_ast_);

    // format_file always contained on any data path
    String version_file_path;

    /// Creating directories, if not exist.
    auto paths = getDataPaths();
    for (const String & path : paths)
    {
        Poco::File(path).createDirectories();
        Poco::File(path + "detached").createDirectory();
        if (Poco::File{path + "format_version.txt"}.exists())
        {
            if (!version_file_path.empty())
            {
                LOG_ERROR(log, "Duplication of version file " << version_file_path << " and " << path << "format_file.txt");
                throw Exception("Multiple format_version.txt file", ErrorCodes::CORRUPTED_DATA);
            }
            version_file_path = path + "format_version.txt";
        }
    }

    /// If not choose any
    if (version_file_path.empty())
        version_file_path = getFullPathOnDisk(storage_policy->getAnyDisk()) + "format_version.txt";

    bool version_file_exists = Poco::File(version_file_path).exists();

    // When data path or file not exists, ignore the format_version check
    if (!attach || !version_file_exists)
    {
        format_version = min_format_version;
        WriteBufferFromFile buf(version_file_path);
        writeIntText(format_version.toUnderType(), buf);
    }
    else
    {
        ReadBufferFromFile buf(version_file_path);
        UInt32 read_format_version;
        readIntText(read_format_version, buf);
        format_version = read_format_version;
        if (!buf.eof())
            throw Exception("Bad version file: " + version_file_path, ErrorCodes::CORRUPTED_DATA);
    }

    if (format_version < min_format_version)
    {
        if (min_format_version == MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING.toUnderType())
            throw Exception(
                "MergeTree data format version on disk doesn't support custom partitioning",
                ErrorCodes::METADATA_MISMATCH);
    }
}


static void checkKeyExpression(const ExpressionActions & expr, const Block & sample_block, const String & key_name)
{
    for (const ExpressionAction & action : expr.getActions())
    {
        if (action.type == ExpressionAction::ARRAY_JOIN)
            throw Exception(key_name + " key cannot contain array joins", ErrorCodes::ILLEGAL_COLUMN);

        if (action.type == ExpressionAction::APPLY_FUNCTION)
        {
            IFunctionBase & func = *action.function_base;
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

        if (element.type->isNullable())
            throw Exception{key_name + " key cannot contain nullable columns", ErrorCodes::ILLEGAL_COLUMN};
    }
}


void MergeTreeData::setProperties(
    const ASTPtr & new_order_by_ast, const ASTPtr & new_primary_key_ast,
    const ColumnsDescription & new_columns, const IndicesDescription & indices_description,
    const ConstraintsDescription & constraints_description, bool only_check)
{
    if (!new_order_by_ast)
        throw Exception("ORDER BY cannot be empty", ErrorCodes::BAD_ARGUMENTS);

    ASTPtr new_sorting_key_expr_list = extractKeyExpressionList(new_order_by_ast);
    ASTPtr new_primary_key_expr_list = new_primary_key_ast
        ? extractKeyExpressionList(new_primary_key_ast) : new_sorting_key_expr_list->clone();

    if (merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
        new_sorting_key_expr_list->children.push_back(std::make_shared<ASTIdentifier>(merging_params.version_column));

    size_t primary_key_size = new_primary_key_expr_list->children.size();
    size_t sorting_key_size = new_sorting_key_expr_list->children.size();
    if (primary_key_size > sorting_key_size)
        throw Exception("Primary key must be a prefix of the sorting key, but its length: "
            + toString(primary_key_size) + " is greater than the sorting key length: " + toString(sorting_key_size),
            ErrorCodes::BAD_ARGUMENTS);

    Names new_primary_key_columns;
    Names new_sorting_key_columns;

    for (size_t i = 0; i < sorting_key_size; ++i)
    {
        String sorting_key_column = new_sorting_key_expr_list->children[i]->getColumnName();
        new_sorting_key_columns.push_back(sorting_key_column);

        if (i < primary_key_size)
        {
            String pk_column = new_primary_key_expr_list->children[i]->getColumnName();
            if (pk_column != sorting_key_column)
                throw Exception("Primary key must be a prefix of the sorting key, but in position "
                    + toString(i) + " its column is " + pk_column + ", not " + sorting_key_column,
                    ErrorCodes::BAD_ARGUMENTS);

            new_primary_key_columns.push_back(pk_column);
        }
    }

    auto all_columns = new_columns.getAllPhysical();

    if (order_by_ast && only_check)
    {
        /// This is ALTER, not CREATE/ATTACH TABLE. Let us check that all new columns used in the sorting key
        /// expression have just been added (so that the sorting order is guaranteed to be valid with the new key).

        ASTPtr added_key_column_expr_list = std::make_shared<ASTExpressionList>();
        for (size_t new_i = 0, old_i = 0; new_i < sorting_key_size; ++new_i)
        {
            if (old_i < sorting_key_columns.size())
            {
                if (new_sorting_key_columns[new_i] != sorting_key_columns[old_i])
                    added_key_column_expr_list->children.push_back(new_sorting_key_expr_list->children[new_i]);
                else
                    ++old_i;
            }
            else
                added_key_column_expr_list->children.push_back(new_sorting_key_expr_list->children[new_i]);
        }

        if (!added_key_column_expr_list->children.empty())
        {
            auto syntax = SyntaxAnalyzer(global_context).analyze(added_key_column_expr_list, all_columns);
            Names used_columns = syntax->requiredSourceColumns();

            NamesAndTypesList deleted_columns;
            NamesAndTypesList added_columns;
            getColumns().getAllPhysical().getDifference(all_columns, deleted_columns, added_columns);

            for (const String & col : used_columns)
            {
                if (!added_columns.contains(col) || deleted_columns.contains(col))
                    throw Exception("Existing column " + col + " is used in the expression that was "
                        "added to the sorting key. You can add expressions that use only the newly added columns",
                        ErrorCodes::BAD_ARGUMENTS);

                if (new_columns.getDefaults().count(col))
                    throw Exception("Newly added column " + col + " has a default expression, so adding "
                        "expressions that use it to the sorting key is forbidden",
                        ErrorCodes::BAD_ARGUMENTS);
            }
        }
    }

    auto new_sorting_key_syntax = SyntaxAnalyzer(global_context).analyze(new_sorting_key_expr_list, all_columns);
    auto new_sorting_key_expr = ExpressionAnalyzer(new_sorting_key_expr_list, new_sorting_key_syntax, global_context)
        .getActions(false);
    auto new_sorting_key_sample =
        ExpressionAnalyzer(new_sorting_key_expr_list, new_sorting_key_syntax, global_context)
        .getActions(true)->getSampleBlock();

    checkKeyExpression(*new_sorting_key_expr, new_sorting_key_sample, "Sorting");

    auto new_primary_key_syntax = SyntaxAnalyzer(global_context).analyze(new_primary_key_expr_list, all_columns);
    auto new_primary_key_expr = ExpressionAnalyzer(new_primary_key_expr_list, new_primary_key_syntax, global_context)
        .getActions(false);

    Block new_primary_key_sample;
    DataTypes new_primary_key_data_types;
    for (size_t i = 0; i < primary_key_size; ++i)
    {
        const auto & elem = new_sorting_key_sample.getByPosition(i);
        new_primary_key_sample.insert(elem);
        new_primary_key_data_types.push_back(elem.type);
    }

    ASTPtr skip_indices_with_primary_key_expr_list = new_primary_key_expr_list->clone();
    ASTPtr skip_indices_with_sorting_key_expr_list = new_sorting_key_expr_list->clone();

    MergeTreeIndices new_indices;

    if (!indices_description.indices.empty())
    {
        std::set<String> indices_names;

        for (const auto & index_ast : indices_description.indices)
        {
            const auto & index_decl = std::dynamic_pointer_cast<ASTIndexDeclaration>(index_ast);

            new_indices.push_back(
                 MergeTreeIndexFactory::instance().get(
                        all_columns,
                        std::dynamic_pointer_cast<ASTIndexDeclaration>(index_decl->clone()),
                        global_context));

            if (indices_names.find(new_indices.back()->name) != indices_names.end())
                throw Exception(
                        "Index with name " + backQuote(new_indices.back()->name) + " already exsists",
                        ErrorCodes::LOGICAL_ERROR);

            ASTPtr expr_list = MergeTreeData::extractKeyExpressionList(index_decl->expr->clone());
            for (const auto & expr : expr_list->children)
            {
                skip_indices_with_primary_key_expr_list->children.push_back(expr->clone());
                skip_indices_with_sorting_key_expr_list->children.push_back(expr->clone());
            }

            indices_names.insert(new_indices.back()->name);
        }
    }
    auto syntax_primary = SyntaxAnalyzer(global_context, {}).analyze(
            skip_indices_with_primary_key_expr_list, all_columns);
    auto new_indices_with_primary_key_expr = ExpressionAnalyzer(
            skip_indices_with_primary_key_expr_list, syntax_primary, global_context).getActions(false);

    auto syntax_sorting = SyntaxAnalyzer(global_context, {}).analyze(
            skip_indices_with_sorting_key_expr_list, all_columns);
    auto new_indices_with_sorting_key_expr = ExpressionAnalyzer(
            skip_indices_with_sorting_key_expr_list, syntax_sorting, global_context).getActions(false);

    if (!only_check)
    {
        setColumns(std::move(new_columns));

        order_by_ast = new_order_by_ast;
        sorting_key_columns = std::move(new_sorting_key_columns);
        sorting_key_expr_ast = std::move(new_sorting_key_expr_list);
        sorting_key_expr = std::move(new_sorting_key_expr);

        primary_key_ast = new_primary_key_ast;
        primary_key_columns = std::move(new_primary_key_columns);
        primary_key_expr_ast = std::move(new_primary_key_expr_list);
        primary_key_expr = std::move(new_primary_key_expr);
        primary_key_sample = std::move(new_primary_key_sample);
        primary_key_data_types = std::move(new_primary_key_data_types);

        setIndices(indices_description);
        skip_indices = std::move(new_indices);

        setConstraints(constraints_description);

        primary_key_and_skip_indices_expr = new_indices_with_primary_key_expr;
        sorting_key_and_skip_indices_expr = new_indices_with_sorting_key_expr;
    }
}


ASTPtr MergeTreeData::extractKeyExpressionList(const ASTPtr & node)
{
    if (!node)
        return std::make_shared<ASTExpressionList>();

    const auto * expr_func = node->as<ASTFunction>();

    if (expr_func && expr_func->name == "tuple")
    {
        /// Primary key is specified in tuple, extract its arguments.
        return expr_func->arguments->clone();
    }
    else
    {
        /// Primary key consists of one column.
        auto res = std::make_shared<ASTExpressionList>();
        res->children.push_back(node);
        return res;
    }
}


void MergeTreeData::initPartitionKey()
{
    ASTPtr partition_key_expr_list = extractKeyExpressionList(partition_by_ast);

    if (partition_key_expr_list->children.empty())
        return;

    {
        auto syntax_result = SyntaxAnalyzer(global_context).analyze(partition_key_expr_list, getColumns().getAllPhysical());
        partition_key_expr = ExpressionAnalyzer(partition_key_expr_list, syntax_result, global_context).getActions(false);
    }

    for (const ASTPtr & ast : partition_key_expr_list->children)
    {
        String col_name = ast->getColumnName();
        partition_key_sample.insert(partition_key_expr->getSampleBlock().getByName(col_name));
    }

    checkKeyExpression(*partition_key_expr, partition_key_sample, "Partition");

    /// Add all columns used in the partition key to the min-max index.
    const NamesAndTypesList & minmax_idx_columns_with_types = partition_key_expr->getRequiredColumnsWithTypes();
    minmax_idx_expr = std::make_shared<ExpressionActions>(minmax_idx_columns_with_types, global_context);
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

namespace
{

void checkTTLExpression(const ExpressionActionsPtr & ttl_expression, const String & result_column_name)
{
    for (const auto & action : ttl_expression->getActions())
    {
        if (action.type == ExpressionAction::APPLY_FUNCTION)
        {
            IFunctionBase & func = *action.function_base;
            if (!func.isDeterministic())
                throw Exception("TTL expression cannot contain non-deterministic functions, "
                    "but contains function " + func.getName(), ErrorCodes::BAD_ARGUMENTS);
        }
    }

    const auto & result_column = ttl_expression->getSampleBlock().getByName(result_column_name);

    if (!typeid_cast<const DataTypeDateTime *>(result_column.type.get())
        && !typeid_cast<const DataTypeDate *>(result_column.type.get()))
    {
        throw Exception("TTL expression result column should have DateTime or Date type, but has "
            + result_column.type->getName(), ErrorCodes::BAD_TTL_EXPRESSION);
    }
}

}


void MergeTreeData::setTTLExpressions(const ColumnsDescription::ColumnTTLs & new_column_ttls,
        const ASTPtr & new_ttl_table_ast, bool only_check)
{
    auto create_ttl_entry = [this](ASTPtr ttl_ast)
    {
        TTLEntry result;

        auto syntax_result = SyntaxAnalyzer(global_context).analyze(ttl_ast, getColumns().getAllPhysical());
        result.expression = ExpressionAnalyzer(ttl_ast, syntax_result, global_context).getActions(false);
        result.destination_type = PartDestinationType::DELETE;
        result.result_column = ttl_ast->getColumnName();

        checkTTLExpression(result.expression, result.result_column);
        return result;
    };

    if (!new_column_ttls.empty())
    {
        NameSet columns_ttl_forbidden;

        if (partition_key_expr)
            for (const auto & col : partition_key_expr->getRequiredColumns())
                columns_ttl_forbidden.insert(col);

        if (sorting_key_expr)
            for (const auto & col : sorting_key_expr->getRequiredColumns())
                columns_ttl_forbidden.insert(col);

        for (const auto & [name, ast] : new_column_ttls)
        {
            if (columns_ttl_forbidden.count(name))
                throw Exception("Trying to set TTL for key column " + name, ErrorCodes::ILLEGAL_COLUMN);
            else
            {
                auto new_ttl_entry = create_ttl_entry(ast);
                if (!only_check)
                    column_ttl_entries_by_name.emplace(name, new_ttl_entry);
            }
        }
    }

    if (new_ttl_table_ast)
    {
        bool seen_delete_ttl = false;
        for (auto ttl_element_ptr : new_ttl_table_ast->children)
        {
            ASTTTLElement & ttl_element = static_cast<ASTTTLElement &>(*ttl_element_ptr);
            if (ttl_element.destination_type == PartDestinationType::DELETE)
            {
                if (seen_delete_ttl)
                {
                    throw Exception("More than one DELETE TTL expression is not allowed", ErrorCodes::BAD_TTL_EXPRESSION);
                }

                auto new_ttl_table_entry = create_ttl_entry(ttl_element.children[0]);
                if (!only_check)
                {
                    ttl_table_ast = ttl_element.children[0];
                    ttl_table_entry = new_ttl_table_entry;
                }

                seen_delete_ttl = true;
            }
            else
            {
                auto new_ttl_entry = create_ttl_entry(ttl_element.children[0]);
                if (!only_check)
                {
                    new_ttl_entry.entry_ast = ttl_element_ptr;
                    new_ttl_entry.destination_type = ttl_element.destination_type;
                    new_ttl_entry.destination_name = ttl_element.destination_name;
                    move_ttl_entries.emplace_back(std::move(new_ttl_entry));
                }
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

    const auto settings = getSettings();
    std::vector<std::pair<String, DiskPtr>> part_names_with_disks;
    Strings part_file_names;
    Poco::DirectoryIterator end;

    auto disks = storage_policy->getDisks();

    /// Reversed order to load part from low priority disks firstly.
    /// Used for keep part on low priority disk if duplication found
    for (auto disk_it = disks.rbegin(); disk_it != disks.rend(); ++disk_it)
    {
        auto disk_ptr = *disk_it;
        for (Poco::DirectoryIterator it(getFullPathOnDisk(disk_ptr)); it != end; ++it)
        {
            /// Skip temporary directories.
            if (startsWith(it.name(), "tmp"))
                continue;

            part_names_with_disks.emplace_back(it.name(), disk_ptr);
        }
    }

    auto part_lock = lockParts();
    data_parts_indexes.clear();

    if (part_names_with_disks.empty())
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

            MutableDataPartPtr part = std::make_shared<DataPart>(*this, part_disk_ptr, part_name, part_info);
            part->relative_path = part_name;
            bool broken = false;

            Poco::Path part_path(getFullPathOnDisk(part_disk_ptr), part_name);
            Poco::Path marker_path(part_path, DELETE_ON_DESTROY_MARKER_PATH);
            if (Poco::File(marker_path).exists())
            {
                LOG_WARNING(log, "Detaching stale part " << getFullPathOnDisk(part_disk_ptr) << part_name << ", which should have been deleted after a move. That can only happen after unclean restart of ClickHouse after move of a part having an operation blocking that stale copy of part.");
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
                if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED
                    || e.code() == ErrorCodes::CANNOT_ALLOCATE_MEMORY
                    || e.code() == ErrorCodes::CANNOT_MUNMAP
                    || e.code() == ErrorCodes::CANNOT_MREMAP)
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
                    LOG_ERROR(log, "Considering to remove broken part " << getFullPathOnDisk(part_disk_ptr) << part_name << " because it's impossible to repair.");
                    std::lock_guard loading_lock(mutex);
                    broken_parts_to_remove.push_back(part);
                }
                else
                {
                    /// Count the number of parts covered by the broken part. If it is at least two, assume that
                    /// the broken part was created as a result of merging them and we won't lose data if we
                    /// delete it.
                    size_t contained_parts = 0;

                    LOG_ERROR(log, "Part " << getFullPathOnDisk(part_disk_ptr) << part_name << " is broken. Looking for parts to replace it.");

                    for (const auto & [contained_name, contained_disk_ptr] : part_names_with_disks)
                    {
                        if (contained_name == part_name)
                            continue;

                        MergeTreePartInfo contained_part_info;
                        if (!MergeTreePartInfo::tryParsePartName(contained_name, &contained_part_info, format_version))
                            continue;

                        if (part->info.contains(contained_part_info))
                        {
                            LOG_ERROR(log, "Found part " << getFullPathOnDisk(contained_disk_ptr) << contained_name);
                            ++contained_parts;
                        }
                    }

                    if (contained_parts >= 2)
                    {
                        LOG_ERROR(log, "Considering to remove broken part " << getFullPathOnDisk(part_disk_ptr) << part_name << " because it covers at least 2 other parts");
                        std::lock_guard loading_lock(mutex);
                        broken_parts_to_remove.push_back(part);
                    }
                    else
                    {
                        LOG_ERROR(log, "Detaching broken part " << getFullPathOnDisk(part_disk_ptr) << part_name
                            << " because it covers less than 2 parts. You need to resolve this manually");
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

            part->modification_time = Poco::File(getFullPathOnDisk(part_disk_ptr) + part_name).getLastModified().epochTime();
            /// Assume that all parts are Committed, covered parts will be detected and marked as Outdated later
            part->state = DataPartState::Committed;

            std::lock_guard loading_lock(mutex);
            if (!data_parts_indexes.insert(part).second)
                throw Exception("Part " + part->name + " already exists", ErrorCodes::DUPLICATE_DATA_PART);
        });
    }

    pool.wait();

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
    std::unique_lock lock(clear_old_temporary_directories_mutex, std::defer_lock);
    if (!lock.try_lock())
        return;

    const auto settings = getSettings();
    time_t current_time = time(nullptr);
    ssize_t deadline = (custom_directories_lifetime_seconds >= 0)
        ? current_time - custom_directories_lifetime_seconds
        : current_time - settings->temporary_directories_lifetime.totalSeconds();

    const auto full_paths = getDataPaths();

    /// Delete temporary directories older than a day.
    Poco::DirectoryIterator end;
    for (auto && full_data_path : full_paths)
    {
        for (Poco::DirectoryIterator it{full_data_path}; it != end; ++it)
        {
            if (startsWith(it.name(), "tmp_"))
            {
                Poco::File tmp_dir(full_data_path + it.name());

                try
                {
                    if (tmp_dir.isDirectory() && isOldPartDirectory(tmp_dir, deadline))
                    {
                        LOG_WARNING(log, "Removing temporary directory " << full_data_path << it.name());
                        Poco::File(full_data_path + it.name()).remove(true);
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


MergeTreeData::DataPartsVector MergeTreeData::grabOldParts()
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
                part_remove_time < now &&
                now - part_remove_time > getSettings()->old_parts_lifetime.totalSeconds())
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
    auto lock = lockParts();
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
        auto lock = lockParts();

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
    if (auto part_log = global_context.getPartLog(database_name))
    {
        PartLogElement part_log_elem;

        part_log_elem.event_type = PartLogElement::REMOVE_PART;
        part_log_elem.event_time = time(nullptr);
        part_log_elem.duration_ms = 0;

        part_log_elem.database_name = database_name;
        part_log_elem.table_name = table_name;

        for (auto & part : parts)
        {
            part_log_elem.partition_id = part->info.partition_id;
            part_log_elem.part_name = part->name;
            part_log_elem.bytes_compressed_on_disk = part->bytes_on_disk;
            part_log_elem.rows = part->rows_count;

            part_log->add(part_log_elem);
        }
    }
}

void MergeTreeData::clearOldPartsFromFilesystem()
{
    DataPartsVector parts_to_remove = grabOldParts();
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
                LOG_DEBUG(log, "Removing part from filesystem " << part->name);
                part->remove();
            });
        }

        pool.wait();
    }
    else
    {
        for (const DataPartPtr & part : parts_to_remove)
        {
            LOG_DEBUG(log, "Removing part from filesystem " << part->name);
            part->remove();
        }
    }
}

void MergeTreeData::rename(
    const String & /*new_path_to_db*/, const String & new_database_name,
    const String & new_table_name, TableStructureWriteLockHolder &)
{
    auto old_table_path = "data/" + escapeForFileName(database_name) + '/' + escapeForFileName(table_name) + '/';
    auto new_db_path = "data/" + escapeForFileName(new_database_name) + '/';
    auto new_table_path = new_db_path + escapeForFileName(new_table_name) + '/';

    auto disks = storage_policy->getDisks();

    for (const auto & disk : disks)
    {
        if (disk->exists(new_table_path))
            throw Exception{"Target path already exists: " + fullPath(disk, new_table_path), ErrorCodes::DIRECTORY_ALREADY_EXISTS};
    }

    for (const auto & disk : disks)
    {
        disk->createDirectory(new_db_path);

        disk->moveFile(old_table_path, new_table_path);
    }

    global_context.dropCaches();

    database_name = new_database_name;
    table_name = new_table_name;
}

void MergeTreeData::dropAllData()
{
    LOG_TRACE(log, "dropAllData: waiting for locks.");

    auto lock = lockParts();

    LOG_TRACE(log, "dropAllData: removing data from memory.");

    DataPartsVector all_parts(data_parts_by_info.begin(), data_parts_by_info.end());

    data_parts_indexes.clear();
    column_sizes.clear();

    global_context.dropCaches();

    LOG_TRACE(log, "dropAllData: removing data from filesystem.");

    /// Removing of each data part before recursive removal of directory is to speed-up removal, because there will be less number of syscalls.
    clearPartsFromFilesystem(all_parts);

    auto full_paths = getDataPaths();

    for (auto && full_data_path : full_paths)
        Poco::File(full_data_path).remove(true);

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

void MergeTreeData::checkAlter(const AlterCommands & commands, const Context & context)
{
    /// Check that needed transformations can be applied to the list of columns without considering type conversions.
    auto new_columns = getColumns();
    auto new_indices = getIndices();
    auto new_constraints = getConstraints();
    ASTPtr new_order_by_ast = order_by_ast;
    ASTPtr new_primary_key_ast = primary_key_ast;
    ASTPtr new_ttl_table_ast = ttl_table_ast;
    SettingsChanges new_changes;
    commands.apply(new_columns, new_indices, new_constraints, new_order_by_ast, new_primary_key_ast, new_ttl_table_ast, new_changes);
    if (getIndices().empty() && !new_indices.empty() &&
            !context.getSettingsRef().allow_experimental_data_skipping_indices)
        throw Exception("You must set the setting `allow_experimental_data_skipping_indices` to 1 " \
                        "before using data skipping indices.", ErrorCodes::BAD_ARGUMENTS);

    /// Set of columns that shouldn't be altered.
    NameSet columns_alter_forbidden;

    /// Primary key columns can be ALTERed only if they are used in the key as-is
    /// (and not as a part of some expression) and if the ALTER only affects column metadata.
    NameSet columns_alter_metadata_only;

    if (partition_key_expr)
    {
        /// Forbid altering partition key columns because it can change partition ID format.
        /// TODO: in some cases (e.g. adding an Enum value) a partition key column can still be ALTERed.
        /// We should allow it.
        for (const String & col : partition_key_expr->getRequiredColumns())
            columns_alter_forbidden.insert(col);
    }

    for (const auto & index : skip_indices)
    {
        for (const String & col : index->expr->getRequiredColumns())
            columns_alter_forbidden.insert(col);
    }

    if (sorting_key_expr)
    {
        for (const ExpressionAction & action : sorting_key_expr->getActions())
        {
            auto action_columns = action.getNeededColumns();
            columns_alter_forbidden.insert(action_columns.begin(), action_columns.end());
        }
        for (const String & col : sorting_key_expr->getRequiredColumns())
            columns_alter_metadata_only.insert(col);

        /// We don't process sample_by_ast separately because it must be among the primary key columns
        /// and we don't process primary_key_expr separately because it is a prefix of sorting_key_expr.
    }

    if (!merging_params.sign_column.empty())
        columns_alter_forbidden.insert(merging_params.sign_column);

    std::map<String, const IDataType *> old_types;
    for (const auto & column : getColumns().getAllPhysical())
        old_types.emplace(column.name, column.type.get());

    for (const AlterCommand & command : commands)
    {
        if (!command.isMutable())
        {
            continue;
        }

        if (columns_alter_forbidden.count(command.column_name))
            throw Exception("Trying to ALTER key column " + command.column_name, ErrorCodes::ILLEGAL_COLUMN);

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

        if (command.type == AlterCommand::MODIFY_ORDER_BY)
        {
            if (!is_custom_partitioned)
                throw Exception(
                    "ALTER MODIFY ORDER BY is not supported for default-partitioned tables created with the old syntax",
                    ErrorCodes::BAD_ARGUMENTS);
        }
    }

    setProperties(new_order_by_ast, new_primary_key_ast,
            new_columns, new_indices, new_constraints, /* only_check = */ true);

    setTTLExpressions(new_columns.getColumnTTLs(), new_ttl_table_ast, /* only_check = */ true);

    for (const auto & setting : new_changes)
        checkSettingCanBeChanged(setting.name);

    /// Check that type conversions are possible.
    ExpressionActionsPtr unused_expression;
    NameToNameMap unused_map;
    bool unused_bool;
    createConvertExpression(nullptr, getColumns().getAllPhysical(), new_columns.getAllPhysical(),
            getIndices().indices, new_indices.indices, unused_expression, unused_map, unused_bool);
}

void MergeTreeData::createConvertExpression(const DataPartPtr & part, const NamesAndTypesList & old_columns, const NamesAndTypesList & new_columns,
    const IndicesASTs & old_indices, const IndicesASTs & new_indices, ExpressionActionsPtr & out_expression,
    NameToNameMap & out_rename_map, bool & out_force_update_metadata) const
{
    const auto settings = getSettings();
    out_expression = nullptr;
    out_rename_map = {};
    out_force_update_metadata = false;
    String part_mrk_file_extension;
    if (part)
        part_mrk_file_extension = part->index_granularity_info.marks_file_extension;
    else
        part_mrk_file_extension = settings->index_granularity_bytes == 0 ? getNonAdaptiveMrkExtension() : getAdaptiveMrkExtension();

    using NameToType = std::map<String, const IDataType *>;
    NameToType new_types;
    for (const NameAndTypePair & column : new_columns)
        new_types.emplace(column.name, column.type.get());

    /// For every column that need to be converted: source column name, column name of calculated expression for conversion.
    std::vector<std::pair<String, String>> conversions;


    /// Remove old indices
    std::set<String> new_indices_set;
    for (const auto & index_decl : new_indices)
        new_indices_set.emplace(index_decl->as<ASTIndexDeclaration &>().name);
    for (const auto & index_decl : old_indices)
    {
        const auto & index = index_decl->as<ASTIndexDeclaration &>();
        if (!new_indices_set.count(index.name))
        {
            out_rename_map["skp_idx_" + index.name + ".idx"] = "";
            out_rename_map["skp_idx_" + index.name + part_mrk_file_extension] = "";
        }
    }

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
            if (!part || part->hasColumnFiles(column.name, *column.type))
            {
                column.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
                {
                    String file_name = IDataType::getFileNameForStream(column.name, substream_path);

                    /// Delete files if they are no longer shared with another column.
                    if (--stream_counts[file_name] == 0)
                    {
                        out_rename_map[file_name + ".bin"] = "";
                        out_rename_map[file_name + part_mrk_file_extension] = "";
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

            if (!new_type->equals(*old_type) && (!part || part->hasColumnFiles(column.name, *column.type)))
            {
                if (isMetadataOnlyConversion(old_type, new_type))
                {
                    out_force_update_metadata = true;
                    continue;
                }

                /// Need to modify column type.
                if (!out_expression)
                    out_expression = std::make_shared<ExpressionActions>(NamesAndTypesList(), global_context);

                out_expression->addInput(ColumnWithTypeAndName(nullptr, column.type, column.name));

                Names out_names;

                /// This is temporary name for expression. TODO Invent the name more safely.
                const String new_type_name_column = '#' + new_type_name + "_column";
                out_expression->add(ExpressionAction::addColumn(
                    { DataTypeString().createColumnConst(1, new_type_name), std::make_shared<DataTypeString>(), new_type_name_column }));

                const auto & function = FunctionFactory::instance().get("CAST", global_context);
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
                    out_rename_map[temporary_file_name + part_mrk_file_extension] = original_file_name + part_mrk_file_extension;
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

void MergeTreeData::alterDataPart(
    const NamesAndTypesList & new_columns,
    const IndicesASTs & new_indices,
    bool skip_sanity_checks,
    AlterDataPartTransactionPtr & transaction)
{
    const auto settings = getSettings();
    ExpressionActionsPtr expression;
    const auto & part = transaction->getDataPart();
    bool force_update_metadata;
    createConvertExpression(part, part->columns, new_columns,
            getIndices().indices, new_indices,
            expression, transaction->rename_map, force_update_metadata);

    size_t num_files_to_modify = transaction->rename_map.size();
    size_t num_files_to_remove = 0;

    for (const auto & from_to : transaction->rename_map)
        if (from_to.second.empty())
            ++num_files_to_remove;

    if (!skip_sanity_checks
        && (num_files_to_modify > settings->max_files_to_modify_in_alter_columns
            || num_files_to_remove > settings->max_files_to_remove_in_alter_columns))
    {
        transaction->clear();

        const bool forbidden_because_of_modify = num_files_to_modify > settings->max_files_to_modify_in_alter_columns;

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
                exception_message << "from " << backQuote(from_to.first) << " to " << backQuote(from_to.second);
                first = false;
            }
            else if (from_to.second.empty())
            {
                exception_message << backQuote(from_to.first);
                first = false;
            }
        }

        exception_message
            << ") need to be "
            << (forbidden_because_of_modify ? "modified" : "removed")
            << " in part " << part->name << " of table at " << part->getFullPath() << ". Aborting just in case."
            << " If it is not an error, you could increase merge_tree/"
            << (forbidden_because_of_modify ? "max_files_to_modify_in_alter_columns" : "max_files_to_remove_in_alter_columns")
            << " parameter in configuration file (current value: "
            << (forbidden_because_of_modify ? settings->max_files_to_modify_in_alter_columns : settings->max_files_to_remove_in_alter_columns)
            << ")";

        throw Exception(exception_message.str(), ErrorCodes::TABLE_DIFFERS_TOO_MUCH);
    }

    DataPart::Checksums add_checksums;

    if (transaction->rename_map.empty() && !force_update_metadata)
    {
        transaction->clear();
        return;
    }

    /// Apply the expression and write the result to temporary files.
    if (expression)
    {
        BlockInputStreamPtr part_in = std::make_shared<MergeTreeSequentialBlockInputStream>(
                *this, part, expression->getRequiredColumns(), false, /* take_column_types_from_storage = */ false);


        auto compression_codec = global_context.chooseCompressionCodec(
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
        IMergedBlockOutputStream::WrittenOffsetColumns unused_written_offsets;

        MergedColumnOnlyOutputStream out(
            *this,
            in.getHeader(),
            part->getFullPath(),
            true /* sync */,
            compression_codec,
            true /* skip_offsets */,
            /// Don't recalc indices because indices alter is restricted
            std::vector<MergeTreeIndexPtr>{},
            unused_written_offsets,
            part->index_granularity,
            &part->index_granularity_info);

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

    /// Write the checksums to the temporary file.
    if (!part->checksums.empty())
    {
        transaction->new_checksums = new_checksums;
        WriteBufferFromFile checksums_file(part->getFullPath() + "checksums.txt.tmp", 4096);
        new_checksums.write(checksums_file);
        transaction->rename_map["checksums.txt.tmp"] = "checksums.txt";
    }

    /// Write the new column list to the temporary file.
    {
        transaction->new_columns = new_columns.filter(part->columns.getNames());
        WriteBufferFromFile columns_file(part->getFullPath() + "columns.txt.tmp", 4096);
        transaction->new_columns.writeText(columns_file);
        transaction->rename_map["columns.txt.tmp"] = "columns.txt";
    }

    return;
}

void MergeTreeData::changeSettings(
        const SettingsChanges & new_changes,
        TableStructureWriteLockHolder & /* table_lock_holder */)
{
    if (!new_changes.empty())
    {
        MergeTreeSettings copy = *getSettings();
        copy.applyChanges(new_changes);
        storage_settings.set(std::make_unique<const MergeTreeSettings>(copy));
    }
}

void MergeTreeData::checkSettingCanBeChanged(const String & setting_name) const
{
    if (MergeTreeSettings::findIndex(setting_name) == MergeTreeSettings::npos)
        throw Exception{"Storage '" + getName() + "' doesn't have setting '" + setting_name + "'", ErrorCodes::UNKNOWN_SETTING};
    if (MergeTreeSettings::isReadonlySetting(setting_name))
        throw Exception{"Setting '" + setting_name + "' is readonly for storage '" + getName() + "'", ErrorCodes::READONLY_SETTING};

}

void MergeTreeData::removeEmptyColumnsFromPart(MergeTreeData::MutableDataPartPtr & data_part)
{
    auto & empty_columns = data_part->empty_columns;
    if (empty_columns.empty())
        return;

    NamesAndTypesList new_columns;
    for (const auto & [name, type] : data_part->columns)
        if (!empty_columns.count(name))
            new_columns.emplace_back(name, type);

    std::stringstream log_message;
    for (auto it = empty_columns.begin(); it != empty_columns.end(); ++it)
    {
        if (it != empty_columns.begin())
            log_message << ", ";
        log_message << *it;
    }

    LOG_INFO(log, "Removing empty columns: " << log_message.str() << " from part " << data_part->name);
    AlterDataPartTransactionPtr transaction(new AlterDataPartTransaction(data_part));
    alterDataPart(new_columns, getIndices().indices, false, transaction);
    if (transaction->isValid())
        transaction->commit();

    empty_columns.clear();
}

void MergeTreeData::freezeAll(const String & with_name, const Context & context, TableStructureReadLockHolder &)
{
    freezePartitionsByMatcher([] (const DataPartPtr &){ return true; }, with_name, context);
}


bool MergeTreeData::AlterDataPartTransaction::isValid() const
{
    return valid && data_part;
}

void MergeTreeData::AlterDataPartTransaction::clear()
{
    valid = false;
}

void MergeTreeData::AlterDataPartTransaction::commit()
{
    if (!isValid())
        return;
    if (!data_part)
        return;

    try
    {
        std::unique_lock<std::shared_mutex> lock(data_part->columns_lock);

        String path = data_part->getFullPath();

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

        mutable_part.bytes_on_disk = new_checksums.getTotalSizeOnDisk();

        /// TODO: we can skip resetting caches when the column is added.
        data_part->storage.global_context.dropCaches();

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

    if (!isValid())
        return;
    if (!data_part)
        return;

    try
    {
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

void MergeTreeData::PartsTemporaryRename::addPart(const String & old_name, const String & new_name)
{
    old_and_new_names.push_back({old_name, new_name});
    const auto paths = storage.getDataPaths();
    for (const auto & full_path : paths)
    {
        for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
        {
            String name = it.name();
            if (name == old_name)
            {
                old_part_name_to_full_path[old_name] = full_path;
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
            const auto & names = old_and_new_names[i];
            if (names.first.empty() || names.second.empty())
                throw DB::Exception("Empty part name. Most likely it's a bug.", ErrorCodes::INCORRECT_FILE_NAME);
            const auto full_path = old_part_name_to_full_path[names.first] + source_dir; /// old_name
            Poco::File(full_path + names.first).renameTo(full_path + names.second);
        }
        catch (...)
        {
            old_and_new_names.resize(i);
            LOG_WARNING(storage.log, "Cannot rename parts to perform operation on them: " << getCurrentExceptionMessage(false));
            throw;
        }
    }
}

MergeTreeData::PartsTemporaryRename::~PartsTemporaryRename()
{
    // TODO what if server had crashed before this destructor was called?
    if (!renamed)
        return;
    for (const auto & names : old_and_new_names)
    {
        if (names.first.empty())
            continue;

        try
        {
            const auto full_path = old_part_name_to_full_path[names.first] + source_dir; /// old_name
            Poco::File(full_path + names.second).renameTo(full_path + names.first);
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


void MergeTreeData::renameTempPartAndAdd(MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction)
{
    auto removed = renameTempPartAndReplace(part, increment, out_transaction);
    if (!removed.empty())
        throw Exception("Added part " + part->name + " covers " + toString(removed.size())
            + " existing part(s) (including " + removed[0]->name + ")", ErrorCodes::LOGICAL_ERROR);
}


void MergeTreeData::renameTempPartAndReplace(
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
        part_info.min_block = part_info.max_block = part_info.mutation = increment->get();
        part_name = part->getNewName(part_info);
    }
    else
        part_name = part->name;

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
        return;
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

    if (out_covered_parts)
    {
        for (DataPartPtr & covered_part : covered_parts)
            out_covered_parts->emplace_back(std::move(covered_part));
    }
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
        if (part->state == MergeTreeDataPart::State::Committed)
            removePartContributionToColumnSizes(part);

        if (part->state == MergeTreeDataPart::State::Committed || clear_without_timeout)
            part->remove_time.store(remove_time, std::memory_order_relaxed);

        if (part->state != MergeTreeDataPart::State::Outdated)
            modifyPartState(part, MergeTreeDataPart::State::Outdated);
    }
}

void MergeTreeData::removePartsFromWorkingSet(const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock * acquired_lock)
{
    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();

    for (auto & part : remove)
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
    LOG_INFO(log, "Renaming " << part_to_detach->relative_path << " to " << prefix << part_to_detach->name << " and forgiving it.");

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


void MergeTreeData::tryRemovePartImmediately(DataPartPtr && part)
{
    DataPartPtr part_to_delete;
    {
        auto lock = lockParts();

        LOG_TRACE(log, "Trying to immediately remove part " << part->getNameWithState());

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
    LOG_TRACE(log, "Removed part " << part_to_delete->name);
}


size_t MergeTreeData::getTotalActiveSizeInBytes() const
{
    size_t res = 0;
    {
        auto lock = lockParts();

        for (auto & part : getDataPartsStateRange(DataPartState::Committed))
            res += part->bytes_on_disk;
    }

    return res;
}


size_t MergeTreeData::getTotalActiveSizeInRows() const
{
    size_t res = 0;
    {
        auto lock = lockParts();

        for (auto & part : getDataPartsStateRange(DataPartState::Committed))
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

    LOG_INFO(log, "Delaying inserting block by "
        << std::fixed << std::setprecision(4) << delay_milliseconds << " ms. because there are " << parts_count_in_partition << " parts");

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
    for (auto original_active_part : getDataPartsStateRange(DataPartState::Committed))
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

            Poco::Path marker_path(Poco::Path(original_active_part->getFullPath()), DELETE_ON_DESTROY_MARKER_PATH);
            try
            {
                Poco::File(marker_path).createFile();
            }
            catch (Poco::Exception & e)
            {
                LOG_ERROR(log, e.what() << " (while creating DeleteOnDestroy marker: " + backQuote(marker_path.toString()) + ")");
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


MergeTreeData::MutableDataPartPtr MergeTreeData::loadPartAndFixMetadata(const DiskPtr & disk, const String & relative_path)
{
    MutableDataPartPtr part = std::make_shared<DataPart>(*this, disk, Poco::Path(relative_path).getFileName());
    part->relative_path = relative_path;
    loadPartAndFixMetadata(part);
    return part;
}

void MergeTreeData::loadPartAndFixMetadata(MutableDataPartPtr part)
{
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
        part->checksums = checkDataPart(part, false, primary_key_data_types, skip_indices);
        {
            WriteBufferFromFile out(full_part_path + "checksums.txt.tmp", 4096);
            part->checksums.write(out);
        }

        Poco::File(full_part_path + "checksums.txt.tmp").renameTo(full_part_path + "checksums.txt");
    }
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
        ColumnSize & total_column_size = column_sizes[column.name];
        ColumnSize part_column_size = part->getColumnSize(column.name, *column.type);
        total_column_size.add(part_column_size);
    }
}

void MergeTreeData::removePartContributionToColumnSizes(const DataPartPtr & part)
{
    std::shared_lock<std::shared_mutex> lock(part->columns_lock);

    for (const auto & column : part->columns)
    {
        ColumnSize & total_column_size = column_sizes[column.name];
        ColumnSize part_column_size = part->getColumnSize(column.name, *column.type);

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


void MergeTreeData::freezePartition(const ASTPtr & partition_ast, const String & with_name, const Context & context, TableStructureReadLockHolder &)
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
        LOG_DEBUG(log, "Freezing parts with prefix " + *prefix);
    else
        LOG_DEBUG(log, "Freezing parts with partition ID " + partition_id);


    freezePartitionsByMatcher(
        [&prefix, &partition_id](const DataPartPtr & part)
        {
            if (prefix)
                return startsWith(part->info.partition_id, *prefix);
            else
                return part->info.partition_id == partition_id;
        },
        with_name,
        context);
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

    auto disk = storage_policy->getDiskByName(name);
    if (!disk)
        throw Exception("Disk " + name + " does not exists on policy " + storage_policy->getName(), ErrorCodes::UNKNOWN_DISK);

    parts.erase(std::remove_if(parts.begin(), parts.end(), [&](auto part_ptr)
        {
            return part_ptr->disk->getName() == disk->getName();
        }), parts.end());

    if (parts.empty())
        throw Exception("Nothing to move", ErrorCodes::NO_SUCH_DATA_PART);

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

    auto volume = storage_policy->getVolumeByName(name);
    if (!volume)
        throw Exception("Volume " + name + " does not exists on policy " + storage_policy->getName(), ErrorCodes::UNKNOWN_DISK);

    if (parts.empty())
        throw Exception("Nothing to move", ErrorCodes::NO_SUCH_DATA_PART);

    parts.erase(std::remove_if(parts.begin(), parts.end(), [&](auto part_ptr)
        {
            for (const auto & disk : volume->disks)
            {
                if (part_ptr->disk->getName() == disk->getName())
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


String MergeTreeData::getPartitionIDFromQuery(const ASTPtr & ast, const Context & context)
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

    size_t fields_count = partition_key_sample.columns();
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

        auto input_stream = FormatFactory::instance().getInput("Values", buf, partition_key_sample, context, context.getSettingsRef().max_block_size);

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

    for (const auto & [path, disk] : getDataPathsWithDisks())
    {
        for (Poco::DirectoryIterator it(path + "detached");
            it != Poco::DirectoryIterator(); ++it)
        {
            auto dir_name = it.name();

            res.emplace_back();
            auto & part = res.back();

            DetachedPartInfo::tryParseDetachedPartName(dir_name, part, format_version);
            part.disk = disk->getName();
        }
    }
    return res;
}

void MergeTreeData::validateDetachedPartName(const String & name) const
{
    if (name.find('/') != std::string::npos || name == "." || name == "..")
        throw DB::Exception("Invalid part name '" + name + "'", ErrorCodes::INCORRECT_FILE_NAME);

    String full_path = getFullPathForPart(name, "detached/");

    if (full_path.empty() || !Poco::File(full_path + name).exists())
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

    LOG_DEBUG(log, "Will drop " << renamed_parts.old_and_new_names.size() << " detached parts.");

    renamed_parts.tryRenameAll();

    for (auto & [old_name, new_name] : renamed_parts.old_and_new_names)
    {
        Poco::File(renamed_parts.old_part_name_to_full_path[old_name] + "detached/" + new_name).remove(true);
        LOG_DEBUG(log, "Dropped detached part " << old_name);
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
        LOG_DEBUG(log, "Looking for parts for partition " << partition_id << " in " << source_dir);
        ActiveDataPartSet active_parts(format_version);

        const auto disks = storage_policy->getDisks();
        for (const DiskPtr & disk : disks)
        {
            const auto full_path = getFullPathOnDisk(disk);
            for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
            {
                const String & name = it.name();
                MergeTreePartInfo part_info;
                // TODO what if name contains "_tryN" suffix?
                /// Parts with prefix in name (e.g. attaching_1_3_3_0, deleting_1_3_3_0) will be ignored
                if (!MergeTreePartInfo::tryParsePartName(name, &part_info, format_version)
                    || part_info.partition_id != partition_id)
                {
                    continue;
                }
                LOG_DEBUG(log, "Found part " << name);
                active_parts.add(name);
                name_to_disk[name] = disk;
            }
        }
        LOG_DEBUG(log, active_parts.size() << " of them are active");
        /// Inactive parts rename so they can not be attached in case of repeated ATTACH.
        for (const auto & [name, disk] : name_to_disk)
        {
            String containing_part = active_parts.getContainingPart(name);
            if (!containing_part.empty() && containing_part != name)
            {
                auto full_path = getFullPathOnDisk(disk);
                // TODO maybe use PartsTemporaryRename here?
                Poco::File(full_path + source_dir + name)
                    .renameTo(full_path + source_dir + "inactive_" + name);
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
        LOG_DEBUG(log, "Checking part " << part_names.second);
        MutableDataPartPtr part = std::make_shared<DataPart>(*this, name_to_disk[part_names.first], part_names.first);
        part->relative_path = source_dir + part_names.second;
        loadPartAndFixMetadata(part);
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

    throw Exception("Cannot reserve " + formatReadableSizeWithBinarySuffix(expected_size) + ", not enough space",
                    ErrorCodes::NOT_ENOUGH_SPACE);
}

}

ReservationPtr MergeTreeData::reserveSpace(UInt64 expected_size) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    auto reservation = storage_policy->reserve(expected_size);

    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeData::reserveSpace(UInt64 expected_size, SpacePtr space) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    auto reservation = tryReserveSpace(expected_size, space);

    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeData::tryReserveSpace(UInt64 expected_size, SpacePtr space) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    return space->reserve(expected_size);
}

ReservationPtr MergeTreeData::reserveSpacePreferringTTLRules(UInt64 expected_size,
        const MergeTreeDataPart::TTLInfos & ttl_infos,
        time_t time_of_move) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    ReservationPtr reservation = tryReserveSpacePreferringTTLRules(expected_size, ttl_infos, time_of_move);

    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeData::tryReserveSpacePreferringTTLRules(UInt64 expected_size,
        const MergeTreeDataPart::TTLInfos & ttl_infos,
        time_t time_of_move) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    ReservationPtr reservation;

    auto ttl_entry = selectTTLEntryForTTLInfos(ttl_infos, time_of_move);
    if (ttl_entry != nullptr)
    {
        SpacePtr destination_ptr = ttl_entry->getDestination(storage_policy);
        if (!destination_ptr)
        {
            if (ttl_entry->destination_type == PartDestinationType::VOLUME)
                LOG_WARNING(log, "Would like to reserve space on volume '"
                        << ttl_entry->destination_name << "' by TTL rule of table '"
                        << log_name << "' but volume was not found");
            else if (ttl_entry->destination_type == PartDestinationType::DISK)
                LOG_WARNING(log, "Would like to reserve space on disk '"
                        << ttl_entry->destination_name << "' by TTL rule of table '"
                        << log_name << "' but disk was not found");
        }
        else
        {
            reservation = destination_ptr->reserve(expected_size);
            if (reservation)
                return reservation;
        }
    }

    reservation = storage_policy->reserve(expected_size);

    return reservation;
}

SpacePtr MergeTreeData::TTLEntry::getDestination(const StoragePolicyPtr & policy) const
{
    if (destination_type == PartDestinationType::VOLUME)
        return policy->getVolumeByName(destination_name);
    else if (destination_type == PartDestinationType::DISK)
        return policy->getDiskByName(destination_name);
    else
        return {};
}

bool MergeTreeData::TTLEntry::isPartInDestination(const StoragePolicyPtr & policy, const MergeTreeDataPart & part) const
{
    if (destination_type == PartDestinationType::VOLUME)
    {
        for (const auto & disk : policy->getVolumeByName(destination_name)->disks)
            if (disk->getName() == part.disk->getName())
                return true;
    }
    else if (destination_type == PartDestinationType::DISK)
        return policy->getDiskByName(destination_name)->getName() == part.disk->getName();
    return false;
}

const MergeTreeData::TTLEntry * MergeTreeData::selectTTLEntryForTTLInfos(
        const MergeTreeDataPart::TTLInfos & ttl_infos,
        time_t time_of_move) const
{
    const MergeTreeData::TTLEntry * result = nullptr;
    /// Prefer TTL rule which went into action last.
    time_t max_max_ttl = 0;

    for (const auto & ttl_entry : move_ttl_entries)
    {
        auto ttl_info_it = ttl_infos.moves_ttl.find(ttl_entry.result_column);
        if (ttl_info_it != ttl_infos.moves_ttl.end()
                && ttl_info_it->second.max <= time_of_move
                && max_max_ttl <= ttl_info_it->second.max)
        {
            result = &ttl_entry;
            max_max_ttl = ttl_info_it->second.max;
        }
    }

    return result;
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
    const String & partition_id, DataPartsLock & /*data_parts_lock*/)
{
    auto it = data_parts_by_state_and_info.lower_bound(DataPartStateAndPartitionID{DataPartState::Committed, partition_id});

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
        LOG_DEBUG(data.log, "Undoing transaction." << ss.str());

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
        auto owing_parts_lock = acquired_parts_lock ? acquired_parts_lock : &parts_lock;

        auto current_time = time(nullptr);
        for (const DataPartPtr & part : precommitted_parts)
        {
            DataPartPtr covering_part;
            DataPartsVector covered_parts = data.getActivePartsToReplace(part->info, part->name, covering_part, *owing_parts_lock);
            if (covering_part)
            {
                LOG_WARNING(data.log, "Tried to commit obsolete part " << part->name
                    << " covered by " << covering_part->getNameWithState());

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

bool MergeTreeData::isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(const ASTPtr & node) const
{
    const String column_name = node->getColumnName();

    for (const auto & name : primary_key_columns)
        if (column_name == name)
            return true;

    for (const auto & name : minmax_idx_columns)
        if (column_name == name)
            return true;

    if (const auto * func = node->as<ASTFunction>())
        if (func->arguments->children.size() == 1)
            return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(func->arguments->children.front());

    return false;
}

bool MergeTreeData::mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context &) const
{
    /// Make sure that the left side of the IN operator contain part of the key.
    /// If there is a tuple on the left side of the IN operator, at least one item of the tuple
    ///  must be part of the key (probably wrapped by a chain of some acceptable functions).
    const auto * left_in_operand_tuple = left_in_operand->as<ASTFunction>();
    if (left_in_operand_tuple && left_in_operand_tuple->name == "tuple")
    {
        for (const auto & item : left_in_operand_tuple->arguments->children)
        {
            if (isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(item))
                return true;
            for (const auto & index : skip_indices)
                if (index->mayBenefitFromIndexForIn(item))
                    return true;
        }
        /// The tuple itself may be part of the primary key, so check that as a last resort.
        return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(left_in_operand);
    }
    else
    {
        for (const auto & index : skip_indices)
            if (index->mayBenefitFromIndexForIn(left_in_operand))
                return true;

        return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(left_in_operand);
    }
}

MergeTreeData & MergeTreeData::checkStructureAndGetMergeTreeData(const StoragePtr & source_table) const
{
    MergeTreeData * src_data = dynamic_cast<MergeTreeData *>(source_table.get());
    if (!src_data)
        throw Exception("Table " + table_name + " supports attachPartitionFrom only for MergeTree family of table engines."
                        " Got " + source_table->getName(), ErrorCodes::NOT_IMPLEMENTED);

    if (getColumns().getAllPhysical().sizeOfDifference(src_data->getColumns().getAllPhysical()))
        throw Exception("Tables have different structure", ErrorCodes::INCOMPATIBLE_COLUMNS);

    auto query_to_string = [] (const ASTPtr & ast)
    {
        return ast ? queryToString(ast) : "";
    };

    if (query_to_string(order_by_ast) != query_to_string(src_data->order_by_ast))
        throw Exception("Tables have different ordering", ErrorCodes::BAD_ARGUMENTS);

    if (query_to_string(partition_by_ast) != query_to_string(src_data->partition_by_ast))
        throw Exception("Tables have different partition key", ErrorCodes::BAD_ARGUMENTS);

    if (format_version != src_data->format_version)
        throw Exception("Tables have different format_version", ErrorCodes::BAD_ARGUMENTS);

    return *src_data;
}

MergeTreeData::MutableDataPartPtr MergeTreeData::cloneAndLoadDataPartOnSameDisk(const MergeTreeData::DataPartPtr & src_part,
                                                                                const String & tmp_part_prefix,
                                                                                const MergeTreePartInfo & dst_part_info)
{
    String dst_part_name = src_part->getNewName(dst_part_info);
    String tmp_dst_part_name = tmp_part_prefix + dst_part_name;

    auto reservation = reserveSpace(src_part->bytes_on_disk, src_part->disk);
    String dst_part_path = getFullPathOnDisk(reservation->getDisk());
    Poco::Path dst_part_absolute_path = Poco::Path(dst_part_path + tmp_dst_part_name).absolute();
    Poco::Path src_part_absolute_path = Poco::Path(src_part->getFullPath()).absolute();

    if (Poco::File(dst_part_absolute_path).exists())
        throw Exception("Part in " + dst_part_absolute_path.toString() + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    LOG_DEBUG(log, "Cloning part " << src_part_absolute_path.toString() << " to " << dst_part_absolute_path.toString());
    localBackup(src_part_absolute_path, dst_part_absolute_path);

    MergeTreeData::MutableDataPartPtr dst_data_part = std::make_shared<MergeTreeData::DataPart>(
        *this, reservation->getDisk(), dst_part_name, dst_part_info);

    dst_data_part->relative_path = tmp_dst_part_name;
    dst_data_part->is_temp = true;

    dst_data_part->loadColumnsChecksumsIndexes(require_part_metadata, true);
    dst_data_part->modification_time = Poco::File(dst_part_absolute_path).getLastModified().epochTime();
    return dst_data_part;
}

String MergeTreeData::getFullPathOnDisk(const DiskPtr & disk) const
{
    return disk->getPath() + "data/" + escapeForFileName(database_name) + '/' + escapeForFileName(table_name) + '/';
}


DiskPtr MergeTreeData::getDiskForPart(const String & part_name, const String & relative_path) const
{
    const auto disks = storage_policy->getDisks();
    for (const DiskPtr & disk : disks)
    {
        const auto disk_path = getFullPathOnDisk(disk);
        for (Poco::DirectoryIterator it = Poco::DirectoryIterator(disk_path + relative_path); it != Poco::DirectoryIterator(); ++it)
            if (it.name() == part_name)
                return disk;
    }
    return nullptr;
}


String MergeTreeData::getFullPathForPart(const String & part_name, const String & relative_path) const
{
    auto disk = getDiskForPart(part_name, relative_path);
    if (disk)
        return getFullPathOnDisk(disk) + relative_path;
    return "";
}

Strings MergeTreeData::getDataPaths() const
{
    Strings res;
    auto disks = storage_policy->getDisks();
    for (const auto & disk : disks)
        res.push_back(getFullPathOnDisk(disk));
    return res;
}

MergeTreeData::PathsWithDisks MergeTreeData::getDataPathsWithDisks() const
{
    PathsWithDisks res;
    auto disks = storage_policy->getDisks();
    for (const auto & disk : disks)
        res.emplace_back(getFullPathOnDisk(disk), disk);
    return res;
}

void MergeTreeData::freezePartitionsByMatcher(MatcherFn matcher, const String & with_name, const Context & context)
{
    String clickhouse_path = Poco::Path(context.getPath()).makeAbsolute().toString();
    String default_shadow_path = clickhouse_path + "shadow/";
    Poco::File(default_shadow_path).createDirectories();
    auto increment = Increment(default_shadow_path + "increment.txt").get(true);

    /// Acquire a snapshot of active data parts to prevent removing while doing backup.
    const auto data_parts = getDataParts();

    size_t parts_processed = 0;
    for (const auto & part : data_parts)
    {
        if (!matcher(part))
            continue;

        String shadow_path = part->disk->getPath() + "shadow/";

        Poco::File(shadow_path).createDirectories();
        String backup_path = shadow_path
            + (!with_name.empty()
                ? escapeForFileName(with_name)
                : toString(increment))
            + "/";

        LOG_DEBUG(log, "Freezing part " << part->name << " snapshot will be placed at " + backup_path);

        String part_absolute_path = Poco::Path(part->getFullPath()).absolute().toString();
        String backup_part_absolute_path = backup_path
            + "data/"
            + escapeForFileName(getDatabaseName()) + "/"
            + escapeForFileName(getTableName()) + "/"
            + part->relative_path;
        localBackup(part_absolute_path, backup_part_absolute_path);
        part->is_frozen.store(true, std::memory_order_relaxed);
        ++parts_processed;
    }

    LOG_DEBUG(log, "Freezed " << parts_processed << " parts");
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
    auto part_log = global_context.getPartLog(database_name);
    if (!part_log)
        return;

    PartLogElement part_log_elem;

    part_log_elem.event_type = type;

    part_log_elem.error = static_cast<UInt16>(execution_status.code);
    part_log_elem.exception = execution_status.message;

    part_log_elem.event_time = time(nullptr);
    /// TODO: Stop stopwatch in outer code to exclude ZK timings and so on
    part_log_elem.duration_ms = elapsed_ns / 1000000;

    part_log_elem.database_name = database_name;
    part_log_elem.table_name = table_name;
    part_log_elem.partition_id = MergeTreePartInfo::fromPartName(new_part_name, format_version).partition_id;
    part_log_elem.part_name = new_part_name;

    if (result_part)
    {
        part_log_elem.path_on_disk = result_part->getFullPath();
        part_log_elem.bytes_compressed_on_disk = result_part->bytes_on_disk;
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
    if (moving_tagger.parts_to_move.empty())
        return false;

    return moveParts(std::move(moving_tagger));
}

bool MergeTreeData::areBackgroundMovesNeeded() const
{
    return storage_policy->getVolumes().size() > 1;
}

bool MergeTreeData::movePartsToSpace(const DataPartsVector & parts, SpacePtr space)
{
    if (parts_mover.moves_blocker.isCancelled())
        return false;

    auto moving_tagger = checkPartsForMove(parts, space);
    if (moving_tagger.parts_to_move.empty())
        return false;

    return moveParts(std::move(moving_tagger));
}

MergeTreeData::CurrentlyMovingPartsTagger MergeTreeData::selectPartsForMove()
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
    return CurrentlyMovingPartsTagger(std::move(parts_to_move), *this);
}

MergeTreeData::CurrentlyMovingPartsTagger MergeTreeData::checkPartsForMove(const DataPartsVector & parts, SpacePtr space)
{
    std::lock_guard moving_lock(moving_parts_mutex);

    MergeTreeMovingParts parts_to_move;
    for (const auto & part : parts)
    {
        auto reservation = space->reserve(part->bytes_on_disk);
        if (!reservation)
            throw Exception("Move is not possible. Not enough space on '" + space->getName() + "'", ErrorCodes::NOT_ENOUGH_SPACE);

        auto reserved_disk = reservation->getDisk();
        String path_to_clone = getFullPathOnDisk(reserved_disk);

        if (Poco::File(path_to_clone + part->name).exists())
            throw Exception(
                "Move is not possible: " + path_to_clone + part->name + " already exists",
                ErrorCodes::DIRECTORY_ALREADY_EXISTS);

        if (currently_moving_parts.count(part) || partIsAssignedToBackgroundOperation(part))
            throw Exception(
                "Cannot move part '" + part->name + "' because it's participating in background process",
                ErrorCodes::PART_IS_TEMPORARILY_LOCKED);

        parts_to_move.emplace_back(part, std::move(reservation));
    }
    return CurrentlyMovingPartsTagger(std::move(parts_to_move), *this);
}

bool MergeTreeData::moveParts(CurrentlyMovingPartsTagger && moving_tagger)
{
    LOG_INFO(log, "Got " << moving_tagger.parts_to_move.size() << " parts to move.");

    for (const auto & moving_part : moving_tagger.parts_to_move)
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

}
