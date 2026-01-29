#include <Storages/IPartitionStrategy.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/PartitionedSink.h>
#include <Functions/generateSnowflakeID.h>
#include <Interpreters/Context.h>
#include <Storages/KeyDescription.h>
#include <Poco/String.h>
#include <Core/Settings.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}

namespace
{
    /// Creates Expression actions to create hive path part of format
    ///  `partition_column_1=toString(partition_value_expr_1)/ ... /partition_column_N=toString(partition_value_expr_N)/`
    /// for given partition columns list and a partition by AST.
    /// The actions will be computed over chunk to convert partition values to string values.
    HiveStylePartitionStrategy::PartitionExpressionActionsAndColumnName buildExpressionHive(
        ASTPtr partition_by,
        const NamesAndTypesList & partition_columns,
        const Block & sample_block,
        ContextPtr context)
    {
        HiveStylePartitionStrategy::PartitionExpressionActionsAndColumnName actions_with_column_name;
        ASTs concat_args;

        if (const auto * tuple_function = partition_by->as<ASTFunction>();
            tuple_function && tuple_function->name == "tuple")
        {
            if (tuple_function->arguments->children.size() != partition_columns.size())
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "The partition by expression has a different number of columns than what is expected by ClickHouse."
                    "This is a bug.");
            }

            std::size_t index = 0;

            for (const auto & partition_column : partition_columns)
            {
                const auto & child = tuple_function->arguments->children[index++];

                concat_args.push_back(std::make_shared<ASTLiteral>(partition_column.name + "="));

                concat_args.push_back(makeASTFunction("toString", child));

                concat_args.push_back(std::make_shared<ASTLiteral>("/"));
            }
        }
        else
        {
            if (partition_columns.size() != 1)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Expected partition expression to contain a single argument, got {} instead", partition_columns.size());
            }

            ASTs to_string_args = {1, partition_by};
            concat_args.push_back(std::make_shared<ASTLiteral>(partition_columns.front().name + "="));
            concat_args.push_back(makeASTFunction("toString", std::move(to_string_args)));
            concat_args.push_back(std::make_shared<ASTLiteral>("/"));
        }

        ASTPtr hive_expr = makeASTFunction("concat", std::move(concat_args));
        auto hive_syntax_result = TreeRewriter(context).analyze(hive_expr, sample_block.getNamesAndTypesList());
        actions_with_column_name.actions = ExpressionAnalyzer(hive_expr, hive_syntax_result, context).getActions(false);
        actions_with_column_name.column_name = hive_expr->getColumnName();

        return actions_with_column_name;
    }

    Block buildBlockWithoutPartitionColumns(
        const Block & sample_block,
        const std::unordered_set<std::string> & partition_expression_required_columns_set)
    {
        Block result;
        for (size_t i = 0; i < sample_block.columns(); i++)
        {
            if (!partition_expression_required_columns_set.contains(sample_block.getByPosition(i).name))
            {
                result.insert(sample_block.getByPosition(i));
            }
        }

        return result;
    }

    std::shared_ptr<IPartitionStrategy> createHivePartitionStrategy(
        ASTPtr partition_by,
        const Block & sample_block,
        ContextPtr context,
        const std::string & file_format,
        bool globbed_path,
        bool partition_columns_in_data_file)
    {
        if (!partition_by)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition strategy hive can not be used without a PARTITION BY expression");
        }

        if (globbed_path)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition strategy {} can not be used with a globbed path", "hive");
        }

        if (file_format.empty() || file_format == "auto")
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "File format can't be empty for hive style partitioning");
        }

        const auto partition_key_description = KeyDescription::getKeyFromAST(partition_by, ColumnsDescription::fromNamesAndTypes(sample_block.getNamesAndTypes()), context);

        for (const auto & partition_expression_column : partition_key_description.sample_block)
        {
            if (!sample_block.has(partition_expression_column.name))
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Hive partitioning expects that the partition by expression columns are a part of the storage columns, could not find '{}' in storage",
                    partition_expression_column.name);
            }

            const auto & type = partition_expression_column.type;
            const bool is_type_supported = isInteger(type) || isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(type) || isStringOrFixedString(type);

            if (!is_type_supported)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Hive partitioning supports only partition columns of types: Integer, Date, Time, DateTime and String/FixedString. Found '{}'",
                    type->getName());
            }
        }

        return std::make_shared<HiveStylePartitionStrategy>(
            partition_key_description,
            sample_block,
            context,
            file_format,
            partition_columns_in_data_file);
    }

    std::shared_ptr<IPartitionStrategy> createWildcardPartitionStrategy(
        ASTPtr partition_by,
        const Block & sample_block,
        ContextPtr context,
        bool contains_partition_wildcard,
        bool partition_columns_in_data_file)
    {
        if (!partition_by)
        {
            return nullptr;
        }

        /// Backwards incompatible in the sense that `create table s3_table engine=s3('path_without_wildcard') PARTITION BY ... used to be valid,
        /// but it is not anymore
        if (!contains_partition_wildcard)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition strategy wildcard can not be used without a '_partition_id' wildcard");
        }

        if (!partition_columns_in_data_file)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition strategy {} can not be used with partition_columns_in_data_file=0", "wildcard");
        }

        return std::make_shared<WildcardPartitionStrategy>(
            KeyDescription::getKeyFromAST(partition_by, ColumnsDescription::fromNamesAndTypes(sample_block.getNamesAndTypes()), context),
            sample_block,
            context);
    }
}

IPartitionStrategy::IPartitionStrategy(KeyDescription partition_key_description_, const Block & sample_block_, ContextPtr context_)
: partition_key_description(partition_key_description_), sample_block(sample_block_), context(context_)
{
}

NamesAndTypesList IPartitionStrategy::getPartitionColumns() const
{
    return partition_key_description.sample_block.getNamesAndTypesList();
}

const KeyDescription & IPartitionStrategy::getPartitionKeyDescription() const
{
    return partition_key_description;
}

std::shared_ptr<IPartitionStrategy> PartitionStrategyFactory::get(StrategyType strategy,
                                                                 ASTPtr partition_by,
                                                                 const NamesAndTypesList & partition_columns,
                                                                 ContextPtr context,
                                                                 const std::string & file_format,
                                                                 bool globbed_path,
                                                                 bool contains_partition_wildcard,
                                                                 bool partition_columns_in_data_file)
{
    Block block;
    for (const auto & partition_column : partition_columns)
    {
        block.insert({partition_column.type, partition_column.name});
    }

    switch (strategy)
    {
        case StrategyType::WILDCARD:
            return createWildcardPartitionStrategy(
                partition_by,
                block,
                context,
                contains_partition_wildcard,
                partition_columns_in_data_file);
        case StrategyType::HIVE:
            return createHivePartitionStrategy(
                partition_by,
                block,
                context,
                file_format,
                globbed_path,
                partition_columns_in_data_file);
        case StrategyType::NONE:
        {
            if (!partition_columns_in_data_file && strategy == PartitionStrategyFactory::StrategyType::NONE)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Partition strategy `none` cannot be used with partition_columns_in_data_file=0");
            }
            /// Unreachable for plain object storage, used only by Data Lakes for now
            return nullptr;
        }
    }
}

WildcardPartitionStrategy::WildcardPartitionStrategy(KeyDescription partition_key_description_, const Block & sample_block_, ContextPtr context_)
    : IPartitionStrategy(partition_key_description_, sample_block_, context_)
{
    ASTs arguments(1, partition_key_description_.definition_ast);
    ASTPtr partition_by_string = makeASTFunction("toString", std::move(arguments));
    auto syntax_result = TreeRewriter(context).analyze(partition_by_string, sample_block.getNamesAndTypesList());
    actions_with_column_name.actions = ExpressionAnalyzer(partition_by_string, syntax_result, context).getActions(false);
    actions_with_column_name.column_name = partition_by_string->getColumnName();
}

ColumnPtr WildcardPartitionStrategy::computePartitionKey(const Chunk & chunk)
{
    Block block_with_partition_by_expr = sample_block.cloneWithoutColumns();
    block_with_partition_by_expr.setColumns(chunk.getColumns());
    actions_with_column_name.actions->execute(block_with_partition_by_expr);

    return block_with_partition_by_expr.getByName(actions_with_column_name.column_name).column;
}

std::string WildcardPartitionStrategy::getPathForRead(
    const std::string & prefix)
{
    return prefix;
}

std::string WildcardPartitionStrategy::getPathForWrite(
    const std::string & prefix,
    const std::string & partition_key)
{
    return PartitionedSink::replaceWildcards(prefix, partition_key);
}

HiveStylePartitionStrategy::HiveStylePartitionStrategy(
    KeyDescription partition_key_description_,
    const Block & sample_block_,
    ContextPtr context_,
    const std::string & file_format_,
    bool partition_columns_in_data_file_)
    : IPartitionStrategy(partition_key_description_, sample_block_, context_),
    file_format(file_format_),
    partition_columns_in_data_file(partition_columns_in_data_file_)
{
    const auto partition_columns = getPartitionColumns();
    for (const auto & partition_column : partition_columns)
    {
        partition_columns_name_set.insert(partition_column.name);
    }
    actions_with_column_name = buildExpressionHive(partition_key_description.definition_ast, partition_columns, sample_block, context);
    block_without_partition_columns = buildBlockWithoutPartitionColumns(sample_block, partition_columns_name_set);
}

std::string HiveStylePartitionStrategy::getPathForRead(const std::string & prefix)
{
    return prefix + "**." + Poco::toLower(file_format);
}

std::string HiveStylePartitionStrategy::getPathForWrite(
    const std::string & prefix,
    const std::string & partition_key)
{
    std::string path;

    if (!prefix.empty())
    {
        path += prefix;
        if (path.back() != '/')
        {
            path += '/';
        }
    }

    /// Not adding '/' because buildExpressionHive() always adds a trailing '/'
    path += partition_key;

    /*
     * File extension is toLower(format)
     * This isn't ideal, but I guess multiple formats can be specified and introduced.
     * So I think it is simpler to keep it this way.
     *
     * Or perhaps implement something like `IInputFormat::getFileExtension()`
     */
    path += std::to_string(generateSnowflakeID()) + "." + Poco::toLower(file_format);

    return path;
}

ColumnPtr HiveStylePartitionStrategy::computePartitionKey(const Chunk & chunk)
{
    Block block_with_partition_by_expr = sample_block.cloneWithoutColumns();
    block_with_partition_by_expr.setColumns(chunk.getColumns());
    actions_with_column_name.actions->execute(block_with_partition_by_expr);

    return block_with_partition_by_expr.getByName(actions_with_column_name.column_name).column;
}

ColumnRawPtrs HiveStylePartitionStrategy::getFormatChunkColumns(const Chunk & chunk)
{
    ColumnRawPtrs result;
    if (partition_columns_in_data_file)
    {
        for (const auto & column : chunk.getColumns())
        {
            result.emplace_back(column.get());
        }

        return result;
    }

    if (chunk.getNumColumns() != sample_block.columns())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Incorrect number of columns in chunk. Expected {}, found {}",
            sample_block.columns(), chunk.getNumColumns());
    }

    for (size_t i = 0; i < sample_block.columns(); i++)
    {
        if (!partition_columns_name_set.contains(sample_block.getByPosition(i).name))
        {
            result.emplace_back(chunk.getColumns()[i].get());
        }
    }

    return result;
}

Block HiveStylePartitionStrategy::getFormatHeader()
{
    if (partition_columns_in_data_file)
    {
        return sample_block;
    }

    return block_without_partition_columns;
}

}
