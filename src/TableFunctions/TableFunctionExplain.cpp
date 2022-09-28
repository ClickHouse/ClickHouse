#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/StorageValues.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionExplain.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

void TableFunctionExplain::parseArguments(const ASTPtr & ast_function, ContextPtr /*context*/)
{
    const auto * function = ast_function->as<ASTFunction>();


    if (function && function->arguments && function->arguments->children.size() == 2)
    {
        const auto * kind_literal = function->arguments->children[0]->as<ASTLiteral>();
        if (!kind_literal)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires `kind` argument to be a string literal, got: '{}'",
                getName(), queryToString(function->arguments->children[0]));

        auto kind = ASTExplainQuery::kindFromString(kind_literal->value.safeGet<String>());
        std::shared_ptr<ASTExplainQuery> explain_query = std::make_shared<ASTExplainQuery>(kind);

        const auto * select_query = function->arguments->children[1]->as<ASTSelectWithUnionQuery>();
        if (!select_query)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires `query` argument to be a select query, got: '{}'",
                getName(), queryToString(function->arguments->children[1]));

        explain_query->setExplainedQuery(select_query->clone());

        query = std::move(explain_query);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires `kind` and `query` arguments", getName());
    }
}

ColumnsDescription TableFunctionExplain::getActualTableStructure(ContextPtr context) const
{
    Block sample_block = getInterpreter(context).getSampleBlock(query->as<ASTExplainQuery>()->getKind());
    ColumnsDescription columns_description;
    for (const auto & column : sample_block.getColumnsWithTypeAndName())
        columns_description.add(ColumnDescription(column.name, column.type));
    return columns_description;
}

static Block executeMonoBlock(QueryPipeline & pipeline)
{
    if (!pipeline.pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected pulling pipeline");

    PullingPipelineExecutor pulling_executor(pipeline);
    std::vector<Block> blocks;
    while (true)
    {
        Block block;
        if (pulling_executor.pull(block))
            blocks.push_back(std::move(block));
        else
            break;
    }

    if (blocks.size() == 1)
        return blocks[0];

    return concatenateBlocks(std::move(blocks));
}

StoragePtr TableFunctionExplain::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    BlockIO blockio = getInterpreter(context).execute();
    Block block = executeMonoBlock(blockio.pipeline);

    StorageID storage_id(getDatabaseName(), table_name);
    auto storage = std::make_shared<StorageValues>(storage_id, getActualTableStructure(context), std::move(block));
    storage->startup();
    return storage;
}

InterpreterExplainQuery TableFunctionExplain::getInterpreter(ContextPtr context) const
{
    if (!query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' requires a explain query argument", getName());

    return InterpreterExplainQuery(query, context);
}

void registerTableFunctionExplain(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionExplain>();
}

}
