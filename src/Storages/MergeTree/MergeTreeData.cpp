#include <Compression/CompressedReadBuffer.h>
#include <DataStreams/ExpressionBlockInputStream.h>
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
#include <Interpreters/SyntaxAnalyzer.h>
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
    extern const int UNKNOWN_SETTING;
    extern const int READONLY_SETTING;
    extern const int ABORTED;
    extern const int UNKNOWN_PART_TYPE;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int UNKNOWN_DISK;
    extern const int NOT_ENOUGH_SPACE;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}


const char * DELETE_ON_DESTROY_MARKER_PATH = "delete-on-destroy.txt";


MergeTreeData::MergeTreeData(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata,
    Context & context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool require_part_metadata_,
    bool attach,
    BrokenPartCallback broken_part_callback_)
    : IStorage(table_id_)
    , global_context(context_)
    , merging_params(merging_params_)
    , settings_ast(metadata.settings_ast)
    , require_part_metadata(require_part_metadata_)
    , relative_data_path(relative_data_path_)
    , broken_part_callback(broken_part_callback_)
    , log_name(table_id_.getNameForLogs())
    , log(&Logger::get(log_name))
    , storage_settings(std::move(storage_settings_))
    , data_parts_by_info(data_parts_indexes.get<TagByInfo>())
    , data_parts_by_state_and_info(data_parts_indexes.get<TagByStateAndInfo>())
    , parts_mover(this)
{
    if (relative_data_path.empty())
        throw Exception("MergeTree storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    const auto settings = getSettings();
    setProperties(metadata, /*only_check*/ false, attach);

    /// NOTE: using the same columns list as is read when performing actual merges.
    merging_params.check(getColumns().getAllPhysical());

    if (metadata.sample_by_ast != nullptr)
    {
        StorageMetadataKeyField candidate_sampling_key = StorageMetadataKeyField::getKeyFromAST(metadata.sample_by_ast, getColumns(), global_context);

        const auto & pk_sample_block = getPrimaryKey().sample_block;
        if (!pk_sample_block.has(candidate_sampling_key.column_names[0]) && !attach
            && !settings->compatibility_allow_sampling_expression_not_in_primary_key) /// This is for backward compatibility.
            throw Exception("Sampling expression must be present in the primary key", ErrorCodes::BAD_ARGUMENTS);

        setSamplingKey(candidate_sampling_key);
    }

    MergeTreeDataFormatVersion min_format_version(0);
    if (!date_column_name.empty())
    {
        try
        {
            auto partition_by_ast = makeASTFunction("toYYYYMM", std::make_shared<ASTIdentifier>(date_column_name));
            initPartitionKey(partition_by_ast);

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
        initPartitionKey(metadata.partition_by_ast);
        min_format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    }

    setTTLExpressions(metadata.columns, metadata.ttl_for_table_ast);

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
        LOG_WARNING(log, "{} Settings 'min_bytes_for_wide_part' and 'min_bytes_for_wide_part' will be ignored.", reason);
}


StorageInMemoryMetadata MergeTreeData::getInMemoryMetadata() const
{
    StorageInMemoryMetadata metadata(getColumns(), getIndices(), getConstraints());

    if (isPartitionKeyDefined())
        metadata.partition_by_ast = getPartitionKeyAST()->clone();

    if (isSortingKeyDefined())
        metadata.order_by_ast = getSortingKeyAST()->clone();

    if (isPrimaryKeyDefined())
        metadata.primary_key_ast = getPrimaryKeyAST()->clone();

    if (ttl_table_ast)
        metadata.ttl_for_table_ast = ttl_table_ast->clone();

    if (isSamplingKeyDefined())
        metadata.sample_by_ast = getSamplingKeyAST()->clone();

    if (settings_ast)
        metadata.settings_ast = settings_ast->clone();

    return metadata;
}

StoragePolicyPtr MergeTreeData::getStoragePolicy() const
{
    return global_context.getStoragePolicy(getSettings()->storage_policy);
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

void MergeTreeData::setProperties(const StorageInMemoryMetadata & metadata, bool only_check, bool attach)
{
    if (!metadata.order_by_ast)
        throw Exception("ORDER BY cannot be empty", ErrorCodes::BAD_ARGUMENTS);

    ASTPtr new_sorting_key_expr_list = extractKeyExpressionList(metadata.order_by_ast);
    ASTPtr new_primary_key_expr_list = metadata.primary_key_ast
        ? extractKeyExpressionList(metadata.primary_key_ast) : new_sorting_key_expr_list->clone();

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
    NameSet primary_key_columns_set;

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

            if (!primary_key_columns_set.emplace(pk_column).second)
                throw Exception("Primary key contains duplicate columns", ErrorCodes::BAD_ARGUMENTS);

            new_primary_key_columns.push_back(pk_column);
        }
    }

    auto all_columns = metadata.columns.getAllPhysical();

    /// Order by check AST
    if (hasSortingKey() && only_check)
    {
        /// This is ALTER, not CREATE/ATTACH TABLE. Let us check that all new columns used in the sorting key
        /// expression have just been added (so that the sorting order is guaranteed to be valid with the new key).

        ASTPtr added_key_column_expr_list = std::make_shared<ASTExpressionList>();
        const auto & old_sorting_key_columns = getSortingKeyColumns();
        for (size_t new_i = 0, old_i = 0; new_i < sorting_key_size; ++new_i)
        {
            if (old_i < old_sorting_key_columns.size())
            {
                if (new_sorting_key_columns[new_i] != old_sorting_key_columns[old_i])
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

                if (metadata.columns.getDefaults().count(col))
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

    DataTypes new_sorting_key_data_types;
    for (size_t i = 0; i < sorting_key_size; ++i)
    {
        new_sorting_key_data_types.push_back(new_sorting_key_sample.getByPosition(i).type);
    }

    ASTPtr skip_indices_with_primary_key_expr_list = new_primary_key_expr_list->clone();
    ASTPtr skip_indices_with_sorting_key_expr_list = new_sorting_key_expr_list->clone();

    MergeTreeIndices new_indices;

    if (!metadata.indices.indices.empty())
    {
        std::set<String> indices_names;

        for (const auto & index_ast : metadata.indices.indices)
        {
            const auto & index_decl = std::dynamic_pointer_cast<ASTIndexDeclaration>(index_ast);

            new_indices.push_back(
                 MergeTreeIndexFactory::instance().get(
                        all_columns,
                        std::dynamic_pointer_cast<ASTIndexDeclaration>(index_decl),
                        global_context,
                        attach));

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
    auto syntax_primary = SyntaxAnalyzer(global_context).analyze(
            skip_indices_with_primary_key_expr_list, all_columns);
    auto new_indices_with_primary_key_expr = ExpressionAnalyzer(
            skip_indices_with_primary_key_expr_list, syntax_primary, global_context).getActions(false);

    auto syntax_sorting = SyntaxAnalyzer(global_context).analyze(
            skip_indices_with_sorting_key_expr_list, all_columns);
    auto new_indices_with_sorting_key_expr = ExpressionAnalyzer(
            skip_indices_with_sorting_key_expr_list, syntax_sorting, global_context).getActions(false);

    if (!only_check)
    {
        setColumns(std::move(metadata.columns));

        StorageMetadataKeyField new_sorting_key;
        new_sorting_key.definition_ast = metadata.order_by_ast;
        new_sorting_key.column_names = std::move(new_sorting_key_columns);
        new_sorting_key.expression_list_ast = std::move(new_sorting_key_expr_list);
        new_sorting_key.expression = std::move(new_sorting_key_expr);
        new_sorting_key.sample_block = std::move(new_sorting_key_sample);
        new_sorting_key.data_types = std::move(new_sorting_key_data_types);
        setSortingKey(new_sorting_key);

        StorageMetadataKeyField new_primary_key;
        new_primary_key.definition_ast = metadata.primary_key_ast;
        new_primary_key.column_names = std::move(new_primary_key_columns);
        new_primary_key.expression_list_ast = std::move(new_primary_key_expr_list);
        new_primary_key.expression = std::move(new_primary_key_expr);
        new_primary_key.sample_block = std::move(new_primary_key_sample);
        new_primary_key.data_types = std::move(new_primary_key_data_types);
        setPrimaryKey(new_primary_key);

        setIndices(metadata.indices);
        skip_indices = std::move(new_indices);

        setConstraints(metadata.constraints);

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


void MergeTreeData::initPartitionKey(ASTPtr partition_by_ast)
{
    StorageMetadataKeyField new_partition_key = StorageMetadataKeyField::getKeyFromAST(partition_by_ast, getColumns(), global_context);

    if (new_partition_key.expression_list_ast->children.empty())
        return;

    checkKeyExpression(*new_partition_key.expression, new_partition_key.sample_block, "Partition");

    /// Add all columns used in the partition key to the min-max index.
    const NamesAndTypesList & minmax_idx_columns_with_types = new_partition_key.expression->getRequiredColumnsWithTypes();
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
    setPartitionKey(new_partition_key);
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


void MergeTreeData::setTTLExpressions(const ColumnsDescription & new_columns,
        const ASTPtr & new_ttl_table_ast, bool only_check)
{

    auto new_column_ttls = new_columns.getColumnTTLs();

    auto create_ttl_entry = [this, &new_columns](ASTPtr ttl_expr_ast)
    {
        TTLEntry result;

        auto ttl_syntax_result = SyntaxAnalyzer(global_context).analyze(ttl_expr_ast, new_columns.getAllPhysical());
        result.expression = ExpressionAnalyzer(ttl_expr_ast, ttl_syntax_result, global_context).getActions(false);
        result.result_column = ttl_expr_ast->getColumnName();

        result.destination_type = PartDestinationType::DELETE;
        result.mode = TTLMode::DELETE;

        checkTTLExpression(result.expression, result.result_column);
        return result;
    };

    auto create_rows_ttl_entry = [this, &new_columns, &create_ttl_entry](const ASTTTLElement * ttl_element)
    {
        auto result = create_ttl_entry(ttl_element->ttl());
        result.mode = ttl_element->mode;

        if (ttl_element->mode == TTLMode::DELETE)
        {
            if (ASTPtr where_expr_ast = ttl_element->where())
            {
                auto where_syntax_result = SyntaxAnalyzer(global_context).analyze(where_expr_ast, new_columns.getAllPhysical());
                result.where_expression = ExpressionAnalyzer(where_expr_ast, where_syntax_result, global_context).getActions(false);
                result.where_result_column = where_expr_ast->getColumnName();
            }
        }
        else if (ttl_element->mode == TTLMode::GROUP_BY)
        {
            if (ttl_element->group_by_key.size() > this->getPrimaryKey().column_names.size())
                throw Exception("TTL Expression GROUP BY key should be a prefix of primary key", ErrorCodes::BAD_TTL_EXPRESSION);

            NameSet primary_key_columns_set(this->getPrimaryKey().column_names.begin(), this->getPrimaryKey().column_names.end());
            NameSet aggregation_columns_set;

            for (const auto & column : this->getPrimaryKey().expression->getRequiredColumns())
                primary_key_columns_set.insert(column);

            for (size_t i = 0; i < ttl_element->group_by_key.size(); ++i)
            {
                if (ttl_element->group_by_key[i]->getColumnName() != this->getPrimaryKey().column_names[i])
                    throw Exception("TTL Expression GROUP BY key should be a prefix of primary key", ErrorCodes::BAD_TTL_EXPRESSION);
            }
            for (const auto & [name, value] : ttl_element->group_by_aggregations)
            {
                if (primary_key_columns_set.count(name))
                    throw Exception("Can not set custom aggregation for column in primary key in TTL Expression", ErrorCodes::BAD_TTL_EXPRESSION);
                aggregation_columns_set.insert(name);
            }
            if (aggregation_columns_set.size() != ttl_element->group_by_aggregations.size())
                throw Exception("Multiple aggregations set for one column in TTL Expression", ErrorCodes::BAD_TTL_EXPRESSION);

            result.group_by_keys = Names(this->getPrimaryKey().column_names.begin(), this->getPrimaryKey().column_names.begin() + ttl_element->group_by_key.size());

            auto aggregations = ttl_element->group_by_aggregations;
            for (size_t i = 0; i < this->getPrimaryKey().column_names.size(); ++i)
            {
                ASTPtr value = this->getPrimaryKey().expression_list_ast->children[i]->clone();

                if (i >= ttl_element->group_by_key.size())
                {
                    ASTPtr value_max = makeASTFunction("max", value->clone());
                    aggregations.emplace_back(value->getColumnName(), std::move(value_max));
                }

                if (value->as<ASTFunction>())
                {
                    auto syntax_result = SyntaxAnalyzer(global_context).analyze(value, new_columns.getAllPhysical(), {}, true);
                    auto expr_actions = ExpressionAnalyzer(value, syntax_result, global_context).getActions(false);
                    for (const auto & column : expr_actions->getRequiredColumns())
                    {
                        if (i < ttl_element->group_by_key.size())
                        {
                            ASTPtr expr = makeASTFunction("any", std::make_shared<ASTIdentifier>(column));
                            aggregations.emplace_back(column, std::move(expr));
                        }
                        else
                        {
                            ASTPtr expr = makeASTFunction("argMax", std::make_shared<ASTIdentifier>(column), value->clone());
                            aggregations.emplace_back(column, std::move(expr));
                        }
                    }
                }
            }
            for (const auto & column : new_columns.getAllPhysical())
            {
                if (!primary_key_columns_set.count(column.name) && !aggregation_columns_set.count(column.name))
                {
                    ASTPtr expr = makeASTFunction("any", std::make_shared<ASTIdentifier>(column.name));
                    aggregations.emplace_back(column.name, std::move(expr));
                }
            }

            for (auto [name, value] : aggregations)
            {
                auto syntax_result = SyntaxAnalyzer(global_context).analyze(value, new_columns.getAllPhysical(), {}, true);
                auto expr_analyzer = ExpressionAnalyzer(value, syntax_result, global_context);

                result.group_by_aggregations.emplace_back(name, value->getColumnName(), expr_analyzer.getActions(false));

                for (const auto & descr : expr_analyzer.getAnalyzedData().aggregate_descriptions)
                    result.aggregate_descriptions.push_back(descr);
            }
        }
        return result;
    };

    if (!new_column_ttls.empty())
    {
        NameSet columns_ttl_forbidden;

        if (hasPartitionKey())
            for (const auto & col : getColumnsRequiredForPartitionKey())
                columns_ttl_forbidden.insert(col);

        if (hasSortingKey())
            for (const auto & col : getColumnsRequiredForSortingKey())
                columns_ttl_forbidden.insert(col);

        for (const auto & [name, ast] : new_column_ttls)
        {
            if (columns_ttl_forbidden.count(name))
                throw Exception("Trying to set TTL for key column " + name, ErrorCodes::ILLEGAL_COLUMN);
            else
            {
                auto new_ttl_entry = create_ttl_entry(ast);
                if (!only_check)
                    column_ttl_entries_by_name[name] = new_ttl_entry;
            }
        }
    }

    if (new_ttl_table_ast)
    {
        std::vector<TTLEntry> update_move_ttl_entries;
        TTLEntry update_rows_ttl_entry;

        bool seen_delete_ttl = false;
        for (const auto & ttl_element_ptr : new_ttl_table_ast->children)
        {
            const auto * ttl_element = ttl_element_ptr->as<ASTTTLElement>();
            if (!ttl_element)
                throw Exception("Unexpected AST element in TTL expression", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

            if (ttl_element->destination_type == PartDestinationType::DELETE)
            {
                if (seen_delete_ttl)
                {
                    throw Exception("More than one DELETE TTL expression is not allowed", ErrorCodes::BAD_TTL_EXPRESSION);
                }

                auto new_rows_ttl_entry = create_rows_ttl_entry(ttl_element);
                if (!only_check)
                    update_rows_ttl_entry = new_rows_ttl_entry;

                seen_delete_ttl = true;
            }
            else
            {
                auto new_ttl_entry = create_rows_ttl_entry(ttl_element);

                new_ttl_entry.entry_ast = ttl_element_ptr;
                new_ttl_entry.destination_type = ttl_element->destination_type;
                new_ttl_entry.destination_name = ttl_element->destination_name;
                if (!new_ttl_entry.getDestination(getStoragePolicy()))
                {
                    String message;
                    if (new_ttl_entry.destination_type == PartDestinationType::DISK)
                        message = "No such disk " + backQuote(new_ttl_entry.destination_name) + " for given storage policy.";
                    else
                        message = "No such volume " + backQuote(new_ttl_entry.destination_name) + " for given storage policy.";
                    throw Exception(message, ErrorCodes::BAD_TTL_EXPRESSION);
                }

                if (!only_check)
                    update_move_ttl_entries.emplace_back(std::move(new_ttl_entry));
            }
        }

        if (!only_check)
        {
            rows_ttl_entry = update_rows_ttl_entry;
            ttl_table_ast = new_ttl_table_ast;

            auto move_ttl_entries_lock = std::lock_guard<std::mutex>(move_ttl_entries_mutex);
            move_ttl_entries = update_move_ttl_entries;
        }
    }
}


void MergeTreeData::checkStoragePolicy(const StoragePolicyPtr & new_storage_policy) const
{
    const auto old_storage_policy = getStoragePolicy();
    old_storage_policy->checkCompatibleWith(new_storage_policy);
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

            auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, part_disk_ptr);
            auto part = createPart(part_name, part_info, single_disk_volume, part_name);
            bool broken = false;

            String part_path = relative_data_path + "/" + part_name;
            String marker_path = part_path + "/" + DELETE_ON_DESTROY_MARKER_PATH;
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
                now - part_remove_time > getSettings()->old_parts_lifetime.totalSeconds()) || force))
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

    global_context.dropCaches();

    LOG_TRACE(log, "dropAllData: removing data from filesystem.");

    /// Removing of each data part before recursive removal of directory is to speed-up removal, because there will be less number of syscalls.
    clearPartsFromFilesystem(all_parts);

    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
        disk->removeRecursive(path);

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

void MergeTreeData::checkAlterIsPossible(const AlterCommands & commands, const Settings & settings)
{
    /// Check that needed transformations can be applied to the list of columns without considering type conversions.
    StorageInMemoryMetadata metadata = getInMemoryMetadata();
    commands.apply(metadata);
    if (getIndices().empty() && !metadata.indices.empty() &&
            !settings.allow_experimental_data_skipping_indices)
        throw Exception("You must set the setting `allow_experimental_data_skipping_indices` to 1 " \
                        "before using data skipping indices.", ErrorCodes::BAD_ARGUMENTS);

    /// Set of columns that shouldn't be altered.
    NameSet columns_alter_type_forbidden;

    /// Primary key columns can be ALTERed only if they are used in the key as-is
    /// (and not as a part of some expression) and if the ALTER only affects column metadata.
    NameSet columns_alter_type_metadata_only;

    if (hasPartitionKey())
    {
        /// Forbid altering partition key columns because it can change partition ID format.
        /// TODO: in some cases (e.g. adding an Enum value) a partition key column can still be ALTERed.
        /// We should allow it.
        for (const String & col : getColumnsRequiredForPartitionKey())
            columns_alter_type_forbidden.insert(col);
    }

    for (const auto & index : skip_indices)
    {
        for (const String & col : index->expr->getRequiredColumns())
            columns_alter_type_forbidden.insert(col);
    }

    if (hasSortingKey())
    {
        auto sorting_key_expr = getSortingKey().expression;
        for (const ExpressionAction & action : sorting_key_expr->getActions())
        {
            auto action_columns = action.getNeededColumns();
            columns_alter_type_forbidden.insert(action_columns.begin(), action_columns.end());
        }
        for (const String & col : sorting_key_expr->getRequiredColumns())
            columns_alter_type_metadata_only.insert(col);

        /// We don't process sample_by_ast separately because it must be among the primary key columns
        /// and we don't process primary_key_expr separately because it is a prefix of sorting_key_expr.
    }
    if (!merging_params.sign_column.empty())
        columns_alter_type_forbidden.insert(merging_params.sign_column);

    std::map<String, const IDataType *> old_types;
    for (const auto & column : getColumns().getAllPhysical())
        old_types.emplace(column.name, column.type.get());


    for (const AlterCommand & command : commands)
    {
        if (command.type == AlterCommand::MODIFY_ORDER_BY && !is_custom_partitioned)
        {
            throw Exception(
                "ALTER MODIFY ORDER BY is not supported for default-partitioned tables created with the old syntax",
                ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::ADD_INDEX && !is_custom_partitioned)
        {
            throw Exception(
                "ALTER ADD INDEX is not supported for tables with the old syntax",
                ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::RENAME_COLUMN)
        {
            if (columns_alter_type_forbidden.count(command.column_name) || columns_alter_type_metadata_only.count(command.column_name))
            {
                throw Exception(
                    "Trying to ALTER RENAME key " + backQuoteIfNeed(command.column_name) + " column which is a part of key expression",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }
        }
        else if (command.isModifyingData(getInMemoryMetadata()))
        {
            if (columns_alter_type_forbidden.count(command.column_name))
                throw Exception("ALTER of key column " + command.column_name + " is forbidden", ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);

            if (columns_alter_type_metadata_only.count(command.column_name))
            {
                if (command.type == AlterCommand::MODIFY_COLUMN)
                {
                    auto it = old_types.find(command.column_name);
                    if (it == old_types.end() || !isMetadataOnlyConversion(it->second, command.data_type.get()))
                        throw Exception("ALTER of key column " + command.column_name + " must be metadata-only",
                            ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
                }
            }
        }
    }

    setProperties(metadata, /* only_check = */ true);

    setTTLExpressions(metadata.columns, metadata.ttl_for_table_ast, /* only_check = */ true);

    if (settings_ast)
    {
        const auto & current_changes = settings_ast->as<const ASTSetQuery &>().changes;
        const auto & new_changes = metadata.settings_ast->as<const ASTSetQuery &>().changes;
        for (const auto & changed_setting : new_changes)
        {
            if (MergeTreeSettings::findIndex(changed_setting.name) == MergeTreeSettings::npos)
                throw Exception{"Storage '" + getName() + "' doesn't have setting '" + changed_setting.name + "'",
                                ErrorCodes::UNKNOWN_SETTING};

            auto comparator = [&changed_setting](const auto & change) { return change.name == changed_setting.name; };

            auto current_setting_it
                = std::find_if(current_changes.begin(), current_changes.end(), comparator);

            if ((current_setting_it == current_changes.end() || *current_setting_it != changed_setting)
                && MergeTreeSettings::isReadonlySetting(changed_setting.name))
            {
                throw Exception{"Setting '" + changed_setting.name + "' is readonly for storage '" + getName() + "'",
                                 ErrorCodes::READONLY_SETTING};
            }

            if (current_setting_it == current_changes.end()
                && MergeTreeSettings::isPartFormatSetting(changed_setting.name))
            {
                MergeTreeSettings copy = *getSettings();
                copy.applyChange(changed_setting);
                String reason;
                if (!canUsePolymorphicParts(copy, &reason) && !reason.empty())
                    throw Exception("Can't change settings. Reason: " + reason, ErrorCodes::NOT_IMPLEMENTED);
            }

            if (changed_setting.name == "storage_policy")
                checkStoragePolicy(global_context.getStoragePolicy(changed_setting.value.safeGet<String>()));
        }
    }
}

MergeTreeDataPartType MergeTreeData::choosePartType(size_t bytes_uncompressed, size_t rows_count) const
{
    if (!canUseAdaptiveGranularity())
        return MergeTreeDataPartType::WIDE;

    const auto settings = getSettings();
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
    else
        throw Exception("Unknown type in part " + relative_path, ErrorCodes::UNKNOWN_PART_TYPE);
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
        type = choosePartType(0, 0);
    }

    return createPart(name, type, part_info, volume, relative_path);
}

void MergeTreeData::changeSettings(
        const ASTPtr & new_settings,
        TableStructureWriteLockHolder & /* table_lock_holder */)
{
    if (new_settings)
    {
        const auto & new_changes = new_settings->as<const ASTSetQuery &>().changes;

        for (const auto & change : new_changes)
            if (change.name == "storage_policy")
            {
                StoragePolicyPtr new_storage_policy = global_context.getStoragePolicy(change.value.safeGet<String>());
                StoragePolicyPtr old_storage_policy = getStoragePolicy();

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
            }

        MergeTreeSettings copy = *getSettings();
        copy.applyChanges(new_changes);
        storage_settings.set(std::make_unique<const MergeTreeSettings>(copy));
        settings_ast = new_settings;
    }
}

void MergeTreeData::freezeAll(const String & with_name, const Context & context, TableStructureReadLockHolder &)
{
    freezePartitionsByMatcher([] (const DataPartPtr &){ return true; }, with_name, context);
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
        part_info.min_block = part_info.max_block = increment->get();
        part_info.mutation = 0; /// it's equal to min_block by default
        part_name = part->getNewName(part_info);
    }
    else
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

    if (covering_part)
    {
        LOG_WARNING(log, "Tried to add obsolete part {} covered by {}", part_name, covering_part->getNameWithState());
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
        if (part->state == IMergeTreeDataPart::State::Committed)
            removePartContributionToColumnSizes(part);

        if (part->state == IMergeTreeDataPart::State::Committed || clear_without_timeout)
            part->remove_time.store(remove_time, std::memory_order_relaxed);

        if (part->state != IMergeTreeDataPart::State::Outdated)
            modifyPartState(part,IMergeTreeDataPart::State::Outdated);
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
            String marker_path = original_active_part->getFullRelativePath() + DELETE_ON_DESTROY_MARKER_PATH;
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

    /// Earlier the list of  columns was written incorrectly. Delete it and re-create.
    /// But in compact parts we can't get list of columns without this file.
    if (isWidePart(part))
        disk->removeIfExists(full_part_path + "columns.txt");

    part->loadColumnsChecksumsIndexes(false, true);
    part->modification_time = disk->getLastModified(full_part_path).epochTime();

    /// If the checksums file is not present, calculate the checksums and write them to disk.
    /// Check the data while we are at it.
    if (part->checksums.empty())
    {
        part->checksums = checkDataPart(part, false);
        {
            auto out = disk->writeFile(full_part_path + "checksums.txt.tmp", 4096);
            part->checksums.write(*out);
        }

        disk->moveFile(full_part_path + "checksums.txt.tmp", full_part_path + "checksums.txt");
    }
}

MergeTreeData::MutableDataPartPtr MergeTreeData::loadPartAndFixMetadata(const VolumePtr & volume, const String & relative_path) const
{
    MutableDataPartPtr part = createPart(Poco::Path(relative_path).getFileName(), volume, relative_path);
    loadPartAndFixMetadataImpl(part);
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
        LOG_DEBUG(log, "Freezing parts with prefix {}", *prefix);
    else
        LOG_DEBUG(log, "Freezing parts with partition ID {}", partition_id);


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

    auto disk = getStoragePolicy()->getDiskByName(name);
    if (!disk)
        throw Exception("Disk " + name + " does not exists on policy " + getStoragePolicy()->getName(), ErrorCodes::UNKNOWN_DISK);

    parts.erase(std::remove_if(parts.begin(), parts.end(), [&](auto part_ptr)
        {
            return part_ptr->volume->getDisk()->getName() == disk->getName();
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

    size_t fields_count = getPartitionKey().sample_block.columns();
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

        auto input_stream = FormatFactory::instance().getInput("Values", buf, getPartitionKey().sample_block, context, context.getSettingsRef().max_block_size);

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
        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_names.first, name_to_disk[part_names.first]);
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

    throw Exception("Cannot reserve " + formatReadableSizeWithBinarySuffix(expected_size) + ", not enough space",
                    ErrorCodes::NOT_ENOUGH_SPACE);
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

ReservationPtr MergeTreeData::reserveSpacePreferringTTLRules(UInt64 expected_size,
        const IMergeTreeDataPart::TTLInfos & ttl_infos,
        time_t time_of_move,
        size_t min_volume_index) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    ReservationPtr reservation = tryReserveSpacePreferringTTLRules(expected_size, ttl_infos, time_of_move, min_volume_index);

    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeData::tryReserveSpacePreferringTTLRules(UInt64 expected_size,
        const IMergeTreeDataPart::TTLInfos & ttl_infos,
        time_t time_of_move,
        size_t min_volume_index) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    ReservationPtr reservation;

    auto ttl_entry = selectTTLEntryForTTLInfos(ttl_infos, time_of_move);
    if (ttl_entry)
    {
        SpacePtr destination_ptr = ttl_entry->getDestination(getStoragePolicy());
        if (!destination_ptr)
        {
            if (ttl_entry->destination_type == PartDestinationType::VOLUME)
                LOG_WARNING(log, "Would like to reserve space on volume '{}' by TTL rule of table '{}' but volume was not found", ttl_entry->destination_name, log_name);
            else if (ttl_entry->destination_type == PartDestinationType::DISK)
                LOG_WARNING(log, "Would like to reserve space on disk '{}' by TTL rule of table '{}' but disk was not found", ttl_entry->destination_name, log_name);
        }
        else
        {
            reservation = destination_ptr->reserve(expected_size);
            if (reservation)
                return reservation;
            else
                if (ttl_entry->destination_type == PartDestinationType::VOLUME)
                    LOG_WARNING(log, "Would like to reserve space on volume '{}' by TTL rule of table '{}' but there is not enough space", ttl_entry->destination_name, log_name);
                else if (ttl_entry->destination_type == PartDestinationType::DISK)
                    LOG_WARNING(log, "Would like to reserve space on disk '{}' by TTL rule of table '{}' but there is not enough space", ttl_entry->destination_name, log_name);
        }
    }

    reservation = getStoragePolicy()->reserve(expected_size, min_volume_index);

    return reservation;
}

SpacePtr MergeTreeData::TTLEntry::getDestination(StoragePolicyPtr policy) const
{
    if (destination_type == PartDestinationType::VOLUME)
        return policy->getVolumeByName(destination_name);
    else if (destination_type == PartDestinationType::DISK)
        return policy->getDiskByName(destination_name);
    else
        return {};
}

bool MergeTreeData::TTLEntry::isPartInDestination(StoragePolicyPtr policy, const IMergeTreeDataPart & part) const
{
    if (destination_type == PartDestinationType::VOLUME)
    {
        for (const auto & disk : policy->getVolumeByName(destination_name)->getDisks())
            if (disk->getName() == part.volume->getDisk()->getName())
                return true;
    }
    else if (destination_type == PartDestinationType::DISK)
        return policy->getDiskByName(destination_name)->getName() == part.volume->getDisk()->getName();
    return false;
}

std::optional<MergeTreeData::TTLEntry> MergeTreeData::selectTTLEntryForTTLInfos(
        const IMergeTreeDataPart::TTLInfos & ttl_infos,
        time_t time_of_move) const
{
    time_t max_max_ttl = 0;
    std::vector<DB::MergeTreeData::TTLEntry>::const_iterator best_entry_it;

    auto lock = std::lock_guard(move_ttl_entries_mutex);
    for (auto ttl_entry_it = move_ttl_entries.begin(); ttl_entry_it != move_ttl_entries.end(); ++ttl_entry_it)
    {
        auto ttl_info_it = ttl_infos.moves_ttl.find(ttl_entry_it->result_column);
        /// Prefer TTL rule which went into action last.
        if (ttl_info_it != ttl_infos.moves_ttl.end()
                && ttl_info_it->second.max <= time_of_move
                && max_max_ttl <= ttl_info_it->second.max)
        {
            best_entry_it = ttl_entry_it;
            max_max_ttl = ttl_info_it->second.max;
        }
    }

    return max_max_ttl ? *best_entry_it : std::optional<MergeTreeData::TTLEntry>();
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
        LOG_DEBUG(data.log, "Undoing transaction.{}", ss.str());

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

bool MergeTreeData::isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(const ASTPtr & node) const
{
    const String column_name = node->getColumnName();

    for (const auto & name : getPrimaryKeyColumns())
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

MergeTreeData & MergeTreeData::checkStructureAndGetMergeTreeData(IStorage & source_table) const
{
    MergeTreeData * src_data = dynamic_cast<MergeTreeData *>(&source_table);
    if (!src_data)
        throw Exception("Table " + source_table.getStorageID().getNameForLogs() +
                        " supports attachPartitionFrom only for MergeTree family of table engines."
                        " Got " + source_table.getName(), ErrorCodes::NOT_IMPLEMENTED);

    if (getColumns().getAllPhysical().sizeOfDifference(src_data->getColumns().getAllPhysical()))
        throw Exception("Tables have different structure", ErrorCodes::INCOMPATIBLE_COLUMNS);

    auto query_to_string = [] (const ASTPtr & ast)
    {
        return ast ? queryToString(ast) : "";
    };

    if (query_to_string(getSortingKeyAST()) != query_to_string(src_data->getSortingKeyAST()))
        throw Exception("Tables have different ordering", ErrorCodes::BAD_ARGUMENTS);

    if (query_to_string(getPartitionKeyAST()) != query_to_string(src_data->getPartitionKeyAST()))
        throw Exception("Tables have different partition key", ErrorCodes::BAD_ARGUMENTS);

    if (format_version != src_data->format_version)
        throw Exception("Tables have different format_version", ErrorCodes::BAD_ARGUMENTS);

    return *src_data;
}

MergeTreeData & MergeTreeData::checkStructureAndGetMergeTreeData(const StoragePtr & source_table) const
{
    return checkStructureAndGetMergeTreeData(*source_table);
}

MergeTreeData::MutableDataPartPtr MergeTreeData::cloneAndLoadDataPartOnSameDisk(const MergeTreeData::DataPartPtr & src_part,
                                                                                const String & tmp_part_prefix,
                                                                                const MergeTreePartInfo & dst_part_info)
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
            "Could not clone and load part " + quoteString(src_part->getFullPath()) + " because disk does not belong to storage policy", ErrorCodes::BAD_ARGUMENTS);

    String dst_part_name = src_part->getNewName(dst_part_info);
    String tmp_dst_part_name = tmp_part_prefix + dst_part_name;

    auto reservation = reserveSpace(src_part->getBytesOnDisk(), src_part->volume->getDisk());
    auto disk = reservation->getDisk();
    String src_part_path = src_part->getFullRelativePath();
    String dst_part_path = relative_data_path + tmp_dst_part_name;

    if (disk->exists(dst_part_path))
        throw Exception("Part in " + fullPath(disk, dst_part_path) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    LOG_DEBUG(log, "Cloning part {} to {}", fullPath(disk, src_part_path), fullPath(disk, dst_part_path));
    localBackup(disk, src_part_path, dst_part_path);
    disk->removeIfExists(dst_part_path + "/" + DELETE_ON_DESTROY_MARKER_PATH);

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk);
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

void MergeTreeData::freezePartitionsByMatcher(MatcherFn matcher, const String & with_name, const Context & context)
{
    String clickhouse_path = Poco::Path(context.getPath()).makeAbsolute().toString();
    String default_shadow_path = clickhouse_path + "shadow/";
    Poco::File(default_shadow_path).createDirectories();
    auto increment = Increment(default_shadow_path + "increment.txt").get(true);

    const String shadow_path = "shadow/";

    /// Acquire a snapshot of active data parts to prevent removing while doing backup.
    const auto data_parts = getDataParts();

    size_t parts_processed = 0;
    for (const auto & part : data_parts)
    {
        if (!matcher(part))
            continue;

        part->volume->getDisk()->createDirectories(shadow_path);

        String backup_path = shadow_path
            + (!with_name.empty()
                ? escapeForFileName(with_name)
                : toString(increment))
            + "/";

        LOG_DEBUG(log, "Freezing part {} snapshot will be placed at {}", part->name, backup_path);

        String backup_part_path = backup_path + relative_data_path + part->relative_path;
        localBackup(part->volume->getDisk(), part->getFullRelativePath(), backup_part_path);
        part->volume->getDisk()->removeIfExists(backup_part_path + "/" + DELETE_ON_DESTROY_MARKER_PATH);

        part->is_frozen.store(true, std::memory_order_relaxed);
        ++parts_processed;
    }

    LOG_DEBUG(log, "Freezed {} parts", parts_processed);
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
    if (moving_tagger.parts_to_move.empty())
        return false;

    return moveParts(std::move(moving_tagger));
}

bool MergeTreeData::areBackgroundMovesNeeded() const
{
    auto policy = getStoragePolicy();

    if (policy->getVolumes().size() > 1)
        return true;

    return policy->getVolumes().size() == 1 && policy->getVolumes()[0]->getDisks().size() > 1 && !move_ttl_entries.empty();
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
    return CurrentlyMovingPartsTagger(std::move(parts_to_move), *this);
}

bool MergeTreeData::moveParts(CurrentlyMovingPartsTagger && moving_tagger)
{
    LOG_INFO(log, "Got {} parts to move.", moving_tagger.parts_to_move.size());

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

ColumnDependencies MergeTreeData::getColumnDependencies(const NameSet & updated_columns) const
{
    if (updated_columns.empty())
        return {};

    ColumnDependencies res;

    NameSet indices_columns;
    NameSet required_ttl_columns;
    NameSet updated_ttl_columns;

    auto add_dependent_columns = [&updated_columns](const auto & expression, auto & to_set)
    {
        auto requiered_columns = expression->getRequiredColumns();
        for (const auto & dependency : requiered_columns)
        {
            if (updated_columns.count(dependency))
            {
                to_set.insert(requiered_columns.begin(), requiered_columns.end());
                return true;
            }
        }

        return false;
    };

    for (const auto & index : skip_indices)
        add_dependent_columns(index->expr, indices_columns);

    if (hasRowsTTL())
    {
        if (add_dependent_columns(rows_ttl_entry.expression, required_ttl_columns))
        {
            /// Filter all columns, if rows TTL expression have to be recalculated.
            for (const auto & column : getColumns().getAllPhysical())
                updated_ttl_columns.insert(column.name);
        }
    }

    for (const auto & [name, entry] : column_ttl_entries_by_name)
    {
        if (add_dependent_columns(entry.expression, required_ttl_columns))
            updated_ttl_columns.insert(name);
    }

    for (const auto & entry : move_ttl_entries)
        add_dependent_columns(entry.expression, required_ttl_columns);

    for (const auto & column : indices_columns)
        res.emplace(column, ColumnDependency::SKIP_INDEX);
    for (const auto & column : required_ttl_columns)
        res.emplace(column, ColumnDependency::TTL_EXPRESSION);
    for (const auto & column : updated_ttl_columns)
        res.emplace(column, ColumnDependency::TTL_TARGET);

    return res;
}

bool MergeTreeData::canUsePolymorphicParts(const MergeTreeSettings & settings, String * out_reason) const
{
    if (!canUseAdaptiveGranularity())
    {
        if ((settings.min_rows_for_wide_part != 0 || settings.min_bytes_for_wide_part != 0) && out_reason)
        {
            std::ostringstream message;
            message << "Table can't create parts with adaptive granularity, but settings min_rows_for_wide_part = "
                    << settings.min_rows_for_wide_part << ", min_bytes_for_wide_part = " << settings.min_bytes_for_wide_part
                    << ". Parts with non-adaptive granularity can be stored only in Wide (default) format.";
            *out_reason = message.str();
        }

        return false;
    }

    return true;
}

MergeTreeData::AlterConversions MergeTreeData::getAlterConversionsForPart(const MergeTreeDataPartPtr part) const
{
    MutationCommands commands = getFirtsAlterMutationCommandsForPart(part);

    AlterConversions result{};
    for (const auto & command : commands)
        /// Currently we need explicit conversions only for RENAME alter
        /// all other conversions can be deduced from diff between part columns
        /// and columns in storage.
        if (command.type == MutationCommand::Type::RENAME_COLUMN)
            result.rename_map[command.rename_to] = command.column_name;

    return result;
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
}
