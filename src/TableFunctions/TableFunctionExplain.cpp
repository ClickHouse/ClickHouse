#include <Core/Settings.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/StorageValues.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Analyzer/TableFunctionNode.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterExplainQuery.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

namespace
{

class TableFunctionExplain : public ITableFunction
{
public:
    static constexpr auto name = "viewExplain";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "Explain"; }

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    InterpreterExplainQuery getInterpreter(ContextPtr context) const;

    ASTPtr query = nullptr;
};

std::vector<size_t> TableFunctionExplain::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr /*context*/) const
{
    const auto & table_function_node = query_node_table_function->as<TableFunctionNode &>();
    const auto & table_function_node_arguments = table_function_node.getArguments().getNodes();
    size_t table_function_node_arguments_size = table_function_node_arguments.size();

    if (table_function_node_arguments_size == 3)
        return {2};

    return {};
}

void TableFunctionExplain::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' cannot be called directly, use `SELECT * FROM (EXPLAIN ...)` syntax", getName());

    size_t num_args = function->arguments->children.size();
    if (num_args != 2 && num_args != 3)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' requires 2 or 3 arguments, got {}", getName(), num_args);

    const auto & kind_arg = function->arguments->children[0];
    const auto * kind_literal = kind_arg->as<ASTLiteral>();
    if (!kind_literal || kind_literal->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' requires a String argument for EXPLAIN kind, got '{}'",
            getName(), queryToString(kind_arg));

    ASTExplainQuery::ExplainKind kind = ASTExplainQuery::fromString(kind_literal->value.safeGet<String>());
    auto explain_query = std::make_shared<ASTExplainQuery>(kind);

    const auto * settings_arg = function->arguments->children[1]->as<ASTLiteral>();
    if (!settings_arg || settings_arg->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' requires a serialized string settings argument, got '{}'",
            getName(), queryToString(function->arguments->children[1]));

    const auto & settings_str = settings_arg->value.safeGet<String>();
    if (!settings_str.empty())
    {
        const Settings & settings = context->getSettingsRef();

        /// parse_only_internals_ = true - we don't want to parse `SET` keyword
        ParserSetQuery settings_parser(/* parse_only_internals_ = */ true);
        ASTPtr settings_ast = parseQuery(
            settings_parser, settings_str, settings[Setting::max_query_size], settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
        explain_query->setSettings(std::move(settings_ast));
    }

    if (function->arguments->children.size() > 2)
    {
        const auto & subquery_arg = function->arguments->children[2];
        const auto * subquery = subquery_arg->as<ASTSubquery>();

        if (!subquery)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function '{}' requires a subquery argument, got '{}'",
                getName(), queryToString(subquery_arg));

        if (subquery->children.empty())
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                "A subquery AST element must have a child");

        const auto & query_arg = subquery->children[0];

        if (!query_arg->as<ASTSelectWithUnionQuery>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function '{}' requires a EXPLAIN's SELECT query argument, got '{}'",
                getName(), queryToString(query_arg));

        explain_query->setExplainedQuery(query_arg);
    }
    else if (kind != ASTExplainQuery::ExplainKind::CurrentTransaction)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires a query argument", getName());
    }

    query = std::move(explain_query);
}

ColumnsDescription TableFunctionExplain::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    Block sample_block = getInterpreter(context).getSampleBlock(query->as<ASTExplainQuery>()->getKind()); /// NOLINT(readability-static-accessed-through-instance)
    ColumnsDescription columns_description;
    for (const auto & column : sample_block.getColumnsWithTypeAndName())
        columns_description.add(ColumnDescription(column.name, column.type));
    return columns_description;
}

Block executeMonoBlock(QueryPipeline & pipeline)
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

    return concatenateBlocks(blocks);
}

StoragePtr TableFunctionExplain::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    /// To support settings inside explain subquery.
    auto mutable_context = Context::createCopy(context);
    InterpreterSetQuery::applySettingsFromQuery(query, mutable_context);
    BlockIO blockio = getInterpreter(mutable_context).execute();
    Block block = executeMonoBlock(blockio.pipeline);

    StorageID storage_id(getDatabaseName(), table_name);
    auto storage = std::make_shared<StorageValues>(storage_id, getActualTableStructure(context, is_insert_query), std::move(block));
    storage->startup();
    return storage;
}

InterpreterExplainQuery TableFunctionExplain::getInterpreter(ContextPtr context) const
{
    if (!query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' requires a explain query argument", getName());

    return InterpreterExplainQuery(query, context);
}

}

void registerTableFunctionExplain(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionExplain>({.documentation = {
            .description=R"(
                Returns result of EXPLAIN query.
                The function should not be called directly but can be invoked via `SELECT * FROM (EXPLAIN <query>)`.
                You can use this query to process the result of EXPLAIN further using SQL (e.g., in tests).
                Example:
                [example:1]
                )",
            .examples={{"1", "SELECT explain FROM (EXPLAIN AST SELECT * FROM system.numbers) WHERE explain LIKE '%Asterisk%'", ""}}
        }});
}

}
