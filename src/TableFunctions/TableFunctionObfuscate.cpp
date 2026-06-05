#include <Core/Names.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/StorageObfuscate.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionObfuscate.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


const ASTSelectWithUnionQuery & TableFunctionObfuscate::getSelectQuery() const
{
    return *create.select;
}

VectorWithMemoryTracking<size_t> TableFunctionObfuscate::skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const
{
    return {0};
}

void TableFunctionObfuscate::parseArguments(const ASTPtr & ast_function, ContextPtr /*context*/)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (function)
    {
        if (auto * select = function->tryGetQueryArgument())
        {
            create.set(create.select, select->clone());
            return;
        }
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires a query argument.", getName());
}

ColumnsDescription TableFunctionObfuscate::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
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

StoragePtr TableFunctionObfuscate::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageObfuscate>(StorageID(getDatabaseName(), table_name), create, columns, "");
    res->startup();
    return res;
}

void registerTableFunctionObfuscate(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionObfuscate>(
        {
            .description = R"(
Obfuscates the result of a query, producing a table that retains some statistical properties of the source data (cardinalities, value distributions, string lengths, compression ratios, etc.) while replacing the actual values with different ones.

It is designed to publish almost real production data for usage in benchmarks. The transformation is deterministic for a given seed, controlled by the `obfuscate_*` settings. It uses some cryptographic primitives, but the result should never be considered secure.

See also the `clickhouse obfuscator` tool, which implements the same algorithm over files.
)",
            .examples{{"obfuscate", "SELECT * FROM obfuscate(SELECT number, toString(number) FROM numbers(10000)) LIMIT 10", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = true});
}

}
