#include <Formats/ReadSchemaUtils.h>

#include <Core/Settings.h>

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

#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_block_size;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/* format(format_name, structure, data) - parses data according to the specified format and structure.
 * format(format_name, data) - infers the schema from the data and parses it according to the specified format.
 * format(data) - detects the format, infers the schema and parses data according to inferred format and structure.
 */
class TableFunctionFormat : public ITableFunction
{
public:
    static constexpr auto name = "format";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return false; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "Values"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    Block parseData(const ColumnsDescription & columns, const String & format_name, const ContextPtr & context) const;

    String format = "auto";
    String structure = "auto";
    String data;
};

void TableFunctionFormat::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.empty() || args.size() > 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires from 1 to 3 arguments: [format, [structure]], data", getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    data = checkAndGetLiteralArgument<String>(args.back(), "data");
    if (args.size() > 1)
        format = checkAndGetLiteralArgument<String>(args[0], "format");
    if (args.size() == 3)
        structure = checkAndGetLiteralArgument<String>(args[1], "structure");
}

ColumnsDescription TableFunctionFormat::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        SingleReadBufferIterator read_buffer_iterator(std::make_unique<ReadBufferFromString>(data));
        if (format == "auto")
            return detectFormatAndReadSchema(std::nullopt, read_buffer_iterator, context).first;
        return readSchemaFromFormat(format, std::nullopt, read_buffer_iterator, context);
    }
    return parseColumnsListFromString(structure, context);
}

Block TableFunctionFormat::parseData(const ColumnsDescription & columns, const String & format_name, const ContextPtr & context) const
{
    Block block;
    for (const auto & name_and_type : columns.getAllPhysical())
        block.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});

    auto read_buf = std::make_unique<ReadBufferFromString>(data);
    auto input_format = context->getInputFormat(format_name, *read_buf, block, context->getSettingsRef()[Setting::max_block_size]);
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

StoragePtr TableFunctionFormat::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool /*is_insert_query*/) const
{
    ColumnsDescription columns;
    String format_name = format;
    if (structure == "auto")
    {
        SingleReadBufferIterator read_buffer_iterator(std::make_unique<ReadBufferFromString>(data));
        if (format_name == "auto")
            std::tie(columns, format_name) = detectFormatAndReadSchema(std::nullopt, read_buffer_iterator, context);
        else
            columns = readSchemaFromFormat(format, std::nullopt, read_buffer_iterator, context);
    }
    else
    {
        columns = parseColumnsListFromString(structure, context);
    }

    Block res_block = parseData(columns, format_name, context);
    auto res = std::make_shared<StorageValues>(StorageID(getDatabaseName(), table_name), columns, res_block);
    res->startup();
    return res;
}

const FunctionDocumentation format_table_function_documentation =
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

}


void registerTableFunctionFormat(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFormat>({format_table_function_documentation, false}, TableFunctionFactory::Case::Insensitive);
}

}
