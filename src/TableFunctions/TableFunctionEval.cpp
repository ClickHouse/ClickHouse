#include <Columns/ColumnsCommon.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/StorageView.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_experimental_eval_table_function;
    extern const SettingsSetOperationMode except_default_mode;
    extern const SettingsSetOperationMode intersect_default_mode;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsSetOperationMode union_default_mode;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class TableFunctionEval : public ITableFunction
{
public:
    static constexpr auto name = "eval";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "View"; }

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override { return {0}; }

    const ASTSelectWithUnionQuery * getSelectQueryForDistributedRewrite() const override { return create.select; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    ASTCreateQuery create;
};

bool isStringFamily(const DataTypePtr & type)
{
    if (isString(type))
        return true;

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        return isString(nullable_type->getNestedType());

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        const auto & dictionary_type = low_cardinality_type->getDictionaryType();
        if (isString(dictionary_type))
            return true;

        if (const auto * nullable_dictionary_type = typeid_cast<const DataTypeNullable *>(dictionary_type.get()))
            return isString(nullable_dictionary_type->getNestedType());
    }

    return false;
}

String extractQueryTextFromField(const Field & field, const DataTypePtr & type, const char * source)
{
    if (!isStringFamily(type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Table function `eval` requires {} to return a value of type `String`, `Nullable(String)`, "
            "`LowCardinality(String)`, or `LowCardinality(Nullable(String))`, got {}",
            source,
            type->getName());

    if (field.isNull())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function `eval` query text cannot be `NULL`");

    if (field.getType() != Field::Types::String)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Table function `eval` requires {} to return a string value, got {}",
            source,
            field.getTypeName());

    return field.safeGet<String>();
}

String evaluateConstantQueryText(const ASTPtr & argument, ContextPtr context)
{
    const auto & [field, type] = evaluateConstantExpression(argument, context);
    return extractQueryTextFromField(field, type, "constant expression");
}

ASTPtr getSubqueryArgument(const ASTPtr & argument)
{
    if (argument->as<ASTSelectWithUnionQuery>())
        return argument;

    if (const auto * subquery = argument->as<ASTSubquery>())
    {
        if (!subquery->children.empty() && subquery->children[0]->as<ASTSelectWithUnionQuery>())
            return subquery->children[0];
    }

    return nullptr;
}

String evaluateSubqueryQueryText(const ASTPtr & query, ContextPtr context)
{
    SharedHeader sample_block;
    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
        sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(query, context);
    else
        sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(query, context);

    if (sample_block->columns() != 1)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Table function `eval` input subquery must return exactly one column, got {} columns",
            sample_block->columns());

    const auto & result_type = sample_block->getByPosition(0).type;
    if (!isStringFamily(result_type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Table function `eval` input subquery must return a value of type `String`, `Nullable(String)`, "
            "`LowCardinality(String)`, or `LowCardinality(Nullable(String))`, got {}",
            result_type->getName());

    BlockIO io;
    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
        io = InterpreterSelectQueryAnalyzer(query, context, SelectQueryOptions{}).execute();
    else
        io = InterpreterSelectWithUnionQuery(query, context, SelectQueryOptions{}).execute();

    if (!io.pipeline.pulling())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function `eval` input subquery must return data");

    PullingPipelineExecutor executor(io.pipeline);

    Block block;
    while (block.rows() == 0 && executor.pull(block))
    {
    }

    if (block.rows() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function `eval` input subquery must return exactly one row, got 0 rows");

    if (block.rows() != 1)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Table function `eval` input subquery must return exactly one row, got more than one row");

    Block next_block;
    while (next_block.rows() == 0 && executor.pull(next_block))
    {
    }

    if (next_block.rows() != 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Table function `eval` input subquery must return exactly one row, got more than one row");

    block = materializeBlock(block);
    return extractQueryTextFromField((*block.getByPosition(0).column)[0], block.getByPosition(0).type, "input subquery");
}

void checkNoNestedEval(const ASTPtr & ast)
{
    if (const auto * function = ast->as<ASTFunction>(); function && function->name == TableFunctionEval::name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Nested table function `eval` is not supported");

    for (const auto & child : ast->children)
        checkNoNestedEval(child);
}

ASTPtr parseGeneratedQuery(const String & query_text, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    ParserQuery parser(query_text.data() + query_text.size(), /* allow_settings_after_format_in_insert = */ false);
    ASTPtr query = parseQuery(
        parser,
        query_text.data(),
        query_text.data() + query_text.size(),
        "query generated by table function `eval`",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    if (!query->as<ASTSelectWithUnionQuery>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function `eval` can only execute `SELECT` queries");

    {
        SelectIntersectExceptQueryVisitor::Data data{settings[Setting::intersect_default_mode], settings[Setting::except_default_mode]};
        SelectIntersectExceptQueryVisitor{data}.visit(query);
    }

    {
        NormalizeSelectWithUnionQueryVisitor::Data data{settings[Setting::union_default_mode]};
        NormalizeSelectWithUnionQueryVisitor{data}.visit(query);
    }

    checkNoNestedEval(query);
    return query;
}

}

void TableFunctionEval::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    if (!context->getSettingsRef()[Setting::allow_experimental_eval_table_function])
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Table function `eval` is disabled. Enable setting `allow_experimental_eval_table_function` to use it");

    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments || function->arguments->children.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function `eval` requires exactly one argument: a constant expression or a `SELECT` query");

    const auto & argument = function->arguments->children[0];
    String query_text;

    if (ASTPtr subquery = getSubqueryArgument(argument))
        query_text = evaluateSubqueryQueryText(subquery, context);
    else
        query_text = evaluateConstantQueryText(argument, context);

    create.set(create.select, parseGeneratedQuery(query_text, context));
}

ColumnsDescription TableFunctionEval::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    chassert(create.select);
    chassert(create.children.size() == 1);
    chassert(create.children[0]->as<ASTSelectWithUnionQuery>());

    SharedHeader sample_block;
    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
        sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(create.children[0], context);
    else
        sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(create.children[0], context);

    return ColumnsDescription(sample_block->getNamesAndTypesList());
}

StoragePtr TableFunctionEval::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto storage = std::make_shared<StorageView>(StorageID(getDatabaseName(), table_name), create, columns, "");
    storage->startup();
    return storage;
}

void registerTableFunctionEval(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionEval>(
        {
            .description = R"(Evaluates a constant expression or a `SELECT` query to a query string, then executes it as a single `SELECT` query.)",
            .category = FunctionDocumentation::Category::TableFunction,
        },
        {.allow_readonly = true});
}

}
