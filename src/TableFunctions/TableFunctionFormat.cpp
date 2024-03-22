#include <Formats/ReadSchemaUtils.h>

#include <IO/ReadBufferFromString.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/parseColumnsListForTableFunction.h>

#include <Parsers/ASTLiteral.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 2 && args.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 2 or 3 arguments: format, [structure], data", getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    format = checkAndGetLiteralArgument<String>(args[0], "format");
    data = checkAndGetLiteralArgument<String>(args.back(), "data");
    if (args.size() == 3)
        structure = checkAndGetLiteralArgument<String>(args[1], "structure");
}

ColumnsDescription TableFunctionFormat::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        SingleReadBufferIterator read_buffer_iterator(std::make_unique<ReadBufferFromString>(data));
        return readSchemaFromFormat(format, std::nullopt, read_buffer_iterator, false, context);
    }
    return parseColumnsListFromString(structure, context);
}

Block TableFunctionFormat::parseData(ColumnsDescription columns, ContextPtr context) const
{
    Block block;
    for (const auto & name_and_type : columns.getAllPhysical())
        block.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});

    auto read_buf = std::make_unique<ReadBufferFromString>(data);
    auto input_format = context->getInputFormat(format, *read_buf, block, context->getSettingsRef().max_block_size);
    QueryPipelineBuilder builder;
    builder.init(Pipe(input_format));
    if (columns.hasDefaults())
    {
        builder.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AddingDefaultsTransform>(header, columns, *input_format, context);
        });
    }

    auto pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
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

StoragePtr TableFunctionFormat::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    Block res_block = parseData(columns, context);
    auto res = std::make_shared<StorageValues>(StorageID(getDatabaseName(), table_name), columns, res_block);
    res->startup();
    return res;
}

static const FunctionDocumentation format_table_function_documentation =
{
    .description=R"(
Extracts table structure from data and parses it according to specified input format.
Syntax: `format(format_name, data)`.
Parameters:
    - `format_name` - the format of the data.
    - `data ` - String literal or constant expression that returns a string containing data in specified format.
Returned value: A table with data parsed from `data` argument according specified format and extracted schema.
)",
    .examples
    {
        {
            "First example",
            R"(
Query:
```
:) select * from format(JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

Result:
```
┌───b─┬─a─────┐
│ 111 │ Hello │
│ 123 │ World │
│ 112 │ Hello │
│ 124 │ World │
└─────┴───────┘
```
)", ""
        },
        {
            "Second example",
            R"(
Query:
```
:) desc format(JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

Result:
```
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ b    │ Nullable(Float64) │              │                    │         │                  │                │
│ a    │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
)", ""
        },
    },
    .categories{"format", "table-functions"}
};

void registerTableFunctionFormat(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFormat>({format_table_function_documentation, false}, TableFunctionFactory::CaseInsensitive);
}
}
