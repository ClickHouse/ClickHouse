#include <Formats/ReadSchemaUtils.h>

#include <IO/ReadBufferFromString.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTLiteral.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Storages/StorageValues.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <TableFunctions/TableFunctionFormat.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionFormat::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 2)
        throw Exception("Table function '" + getName() + "' requires 2 arguments: format and data", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    format = checkAndGetLiteralArgument<String>(args[0], "format");
    data = checkAndGetLiteralArgument<String>(args[1], "data");
}

ColumnsDescription TableFunctionFormat::getActualTableStructure(ContextPtr context) const
{
    ReadBufferIterator read_buffer_iterator = [&]()
    {
        return std::make_unique<ReadBufferFromString>(data);
    };
    return readSchemaFromFormat(format, std::nullopt, read_buffer_iterator, false, context);
}

Block TableFunctionFormat::parseData(ColumnsDescription columns, ContextPtr context) const
{
    Block block;
    for (const auto & name_and_type : columns.getAllPhysical())
        block.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});

    auto read_buf = std::make_unique<ReadBufferFromString>(data);
    auto input_format = context->getInputFormat(format, *read_buf, block, context->getSettingsRef().max_block_size);
    auto pipeline = std::make_unique<QueryPipeline>(input_format);
    auto reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    std::vector<Block> blocks;
    while (reader->pull(block))
        blocks.push_back(std::move(block));

    if (blocks.size() == 1)
        return blocks[0];

    /// In case when data contains more then 1 block we combine
    /// them all to one big block (this is considered a rare case).
    return concatenateBlocks(blocks);
}

StoragePtr TableFunctionFormat::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    Block res_block = parseData(columns, context);
    auto res = std::make_shared<StorageValues>(StorageID(getDatabaseName(), table_name), columns, res_block);
    res->startup();
    return res;
}

void registerTableFunctionFormat(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFormat>(TableFunctionFactory::CaseInsensitive);
}

}
