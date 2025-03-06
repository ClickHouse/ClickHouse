#include <Storages/PartitionStrategy.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/PartitionedSink.h>
#include <Functions/generateSnowflakeID.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}


namespace
{
    Names extractPartitionRequiredColumns(ASTPtr & partition_by, const Block & sample_block, ContextPtr context)
    {
        auto pby_clone = partition_by->clone();
        auto syntax_result = TreeRewriter(context).analyze(pby_clone, sample_block.getNamesAndTypesList());
        auto exp_analyzer = ExpressionAnalyzer(pby_clone, syntax_result, context).getActions(false);
        return exp_analyzer->getRequiredColumns();
    }

    static std::string formatToFileExtension(const std::string & format)
    {
        if (format == "Parquet")
        {
            return "parquet";
        }

        if (format == "CSV")
        {
            return "csv";
        }

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for format {}", format);
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
                                                                  const std::string & partitioning_style)
{
    if (partitioning_style == "hive")
    {
        if (file_format.empty())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "File format can't be empty for hive style partitioning");
        }
        return std::make_shared<HiveStylePartitionStrategy>(partition_by, sample_block, context, file_format);
    }

    return std::make_shared<StringfiedPartitionStrategy>(partition_by, sample_block, context);
}

StringfiedPartitionStrategy::StringfiedPartitionStrategy(ASTPtr partition_by_, const Block & sample_block_, ContextPtr context_)
    : PartitionStrategy(partition_by_, sample_block_, context_)
{
}

StringfiedPartitionStrategy::PartitionExpressionActionsAndColumnName StringfiedPartitionStrategy::getExpression()
{
    StringfiedPartitionStrategy::PartitionExpressionActionsAndColumnName actions_with_column_name;

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

HiveStylePartitionStrategy::HiveStylePartitionStrategy(ASTPtr partition_by_, const Block & sample_block_, ContextPtr context_, const std::string & file_format_)
: PartitionStrategy(partition_by_, sample_block_, context_), file_format(file_format_)
{

}

HiveStylePartitionStrategy::PartitionExpressionActionsAndColumnName HiveStylePartitionStrategy::getExpression()
{
    StringfiedPartitionStrategy::PartitionExpressionActionsAndColumnName actions_with_column_name;

    const Names partition_expression_required_columns = extractPartitionRequiredColumns(partition_by, sample_block, context);
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

std::string HiveStylePartitionStrategy::getPath(
    const std::string & prefix,
    const std::string & partition_key)
{
    std::string path;

    if (!prefix.empty())
    {
        path += prefix + "/";
    }

    return path + partition_key + "/" + std::to_string(generateSnowflakeID()) + "." + formatToFileExtension(file_format);
}

}
