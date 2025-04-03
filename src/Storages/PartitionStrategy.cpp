#include <Storages/PartitionStrategy.h>
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
}

namespace
{
    Names extractPartitionRequiredColumns(ASTPtr & partition_by, const Block & sample_block, ContextPtr context)
    {
        auto pby_clone = partition_by->clone();
        // sample_block columns might be out of order, and we need to respect the partition by expression order
        auto key_description = KeyDescription::getKeyFromAST(pby_clone, ColumnsDescription::fromNamesAndTypes(sample_block.getNamesAndTypes()), context);
        auto syntax_result = TreeRewriter(context).analyze(pby_clone, key_description.sample_block.getNamesAndTypesList());
        auto exp_analyzer = ExpressionAnalyzer(pby_clone, syntax_result, context).getActions(false);
        return exp_analyzer->getRequiredColumns();
    }

    HiveStylePartitionStrategy::PartitionExpressionActionsAndColumnName buildExpressionHive(
        ASTPtr partition_by,
        const Names & partition_expression_required_columns,
        const Block & sample_block,
        ContextPtr context)
    {
        HiveStylePartitionStrategy::PartitionExpressionActionsAndColumnName actions_with_column_name;
        ASTs concat_args;

        if (const auto * tuple_function = partition_by->as<ASTFunction>();
            tuple_function && tuple_function->name == "tuple")
        {
            chassert(tuple_function->arguments->children.size() == partition_expression_required_columns.size());

            for (size_t i = 0; i < tuple_function->arguments->children.size(); i++)
            {
                const auto & child = tuple_function->arguments->children[i];

                concat_args.push_back(std::make_shared<ASTLiteral>(partition_expression_required_columns[i] + "="));

                concat_args.push_back(makeASTFunction("toString", child));

                concat_args.push_back(std::make_shared<ASTLiteral>("/"));
            }
        }
        else
        {
            chassert(partition_expression_required_columns.size() == 1);

            ASTs to_string_args = {1, partition_by};
            concat_args.push_back(std::make_shared<ASTLiteral>(partition_expression_required_columns[0] + "="));
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
}

PartitionStrategy::PartitionStrategy(ASTPtr partition_by_, const Block & sample_block_, ContextPtr context_)
: partition_by(partition_by_), sample_block(sample_block_), context(context_)
{

}

std::shared_ptr<PartitionStrategy> PartitionStrategyProvider::get(ASTPtr partition_by,
                                                                  const Block & sample_block,
                                                                  ContextPtr context,
                                                                  const std::string & file_format,
                                                                  const std::string & partition_strategy,
                                                                  bool write_partition_columns_into_files)
{
    if (partition_strategy == "hive")
    {
        if (file_format.empty())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "File format can't be empty for hive style partitioning");
        }

        return std::make_shared<HiveStylePartitionStrategy>(
            partition_by,
            sample_block,
            context,
            file_format,
            write_partition_columns_into_files);
    }

    return std::make_shared<StringfiedPartitionStrategy>(partition_by, sample_block, context);
}

StringfiedPartitionStrategy::StringfiedPartitionStrategy(ASTPtr partition_by_, const Block & sample_block_, ContextPtr context_)
    : PartitionStrategy(partition_by_, sample_block_, context_)
{
}

StringfiedPartitionStrategy::PartitionExpressionActionsAndColumnName StringfiedPartitionStrategy::getExpression()
{
    PartitionExpressionActionsAndColumnName actions_with_column_name;

    ASTs arguments(1, partition_by);
    ASTPtr partition_by_string = makeASTFunction("toString", std::move(arguments));
    auto syntax_result = TreeRewriter(context).analyze(partition_by_string, sample_block.getNamesAndTypesList());
    actions_with_column_name.actions = ExpressionAnalyzer(partition_by_string, syntax_result, context).getActions(false);
    actions_with_column_name.column_name = partition_by_string->getColumnName();

    return actions_with_column_name;
}

std::string StringfiedPartitionStrategy::getPath(
    const std::string & prefix,
    const std::string & partition_key)
{
    return PartitionedSink::replaceWildcards(prefix, partition_key);
}

HiveStylePartitionStrategy::HiveStylePartitionStrategy(
    ASTPtr partition_by_,
    const Block & sample_block_,
    ContextPtr context_,
    const std::string & file_format_,
    bool write_partition_columns_into_files_)
    : PartitionStrategy(partition_by_, sample_block_, context_),
    file_format(file_format_),
    write_partition_columns_into_files(write_partition_columns_into_files_),
    partition_expression_required_columns(extractPartitionRequiredColumns(partition_by, sample_block, context)),
    partition_expression_required_columns_set(partition_expression_required_columns.begin(), partition_expression_required_columns.end())
{
    actions_with_column_name = buildExpressionHive(partition_by, partition_expression_required_columns, sample_block, context);
    block_without_partition_columns = buildBlockWithoutPartitionColumns(sample_block, partition_expression_required_columns_set);
}

HiveStylePartitionStrategy::PartitionExpressionActionsAndColumnName HiveStylePartitionStrategy::getExpression()
{
    return actions_with_column_name;
}

std::string HiveStylePartitionStrategy::getPath(
    const std::string & prefix,
    const std::string & partition_key)
{
    std::string path;

    if (!prefix.empty())
    {
        path += prefix + "/";
    }

    /*
     * File extension is toLower(format)
     * This isn't ideal, but I guess multiple formats can be specified and introduced.
     * So I think it is simpler to keep it this way.
     *
     * Or perhaps implement something like `IInputFormat::getFileExtension()`
     */
    return path + partition_key + "/" + std::to_string(generateSnowflakeID()) + "." + Poco::toLower(file_format);
}

Chunk HiveStylePartitionStrategy::getChunkWithoutPartitionColumnsIfNeeded(const Chunk & chunk)
{
    Chunk result;

    if (write_partition_columns_into_files)
    {
        for (const auto & column : chunk.getColumns())
        {
            result.addColumn(column);
        }

        return result;
    }

    chassert(chunk.getColumns().size() == sample_block.columns());

    for (size_t i = 0; i < sample_block.columns(); i++)
    {
        if (!partition_expression_required_columns_set.contains(sample_block.getByPosition(i).name))
        {
            result.addColumn(chunk.getColumns()[i]);
        }
    }

    return result;
}

Block HiveStylePartitionStrategy::getBlockWithoutPartitionColumnsIfNeeded()
{
    if (write_partition_columns_into_files)
    {
        return sample_block;
    }

    return block_without_partition_columns;
}

}
