#include <Core/Names.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/Sources/ObfuscateSource.h>
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

    /// Interpret the inner query with the same context adjustments as the execution
    /// in `ObfuscateSource`, in particular so that positional arguments are resolved
    /// even on secondary-query (remote-shard) contexts.
    auto inner_context = ObfuscateSource::makeInnerContext(context);

    if (inner_context->getSettingsRef()[Setting::allow_experimental_analyzer])
        sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(create.children[0], inner_context);
    else
        sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(create.children[0], inner_context);

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
Obfuscates the result of a query, producing a table that retains some statistical properties of the source data (cardinalities, value distributions, string lengths, compression ratios, etc.) while replacing most of the actual values with different ones.

It is designed to publish almost real production data for usage in benchmarks. The transformation is deterministic for a given seed, controlled by the `obfuscate_*` settings. It uses some cryptographic primitives, but the result should never be considered secure.

Some pieces of the data are intentionally preserved exactly, matching the `clickhouse obfuscator` tool: `Date` values pass through unchanged; `DateTime` values keep the exact source date component (as displayed in the column's timezone) and only obfuscate the time of day; `Nullable` columns keep the original null map, so which rows are `NULL` does not change; `Array` columns keep the original array sizes. Do not rely on this table function to hide dates or the pattern of missing values.

The table function is a repeating, effectively infinite source: it trains on the result of the inner query and then re-executes that query to generate obfuscated rows, advancing the seed on every pass. The passes form one continuous obfuscated stream, not independent re-obfuscations: the delta-based models (`Float32`/`Float64` and the time-of-day part of `DateTime`) carry their previous-value state across pass boundaries, exactly like the multi-pass mode of the `clickhouse obfuscator` tool. Always bound the output with an outer `LIMIT` (as in the example below); otherwise the query runs until cancelled.

The set of supported column types is the one the `clickhouse obfuscator` tool implements: the native-width integers (`Int8`/`Int16`/`Int32`/`Int64` and their unsigned counterparts, including `Bool`), `Float32`, `Float64`, `Date`, `DateTime`, `String`, `FixedString`, `UUID`, and `Array` and `Nullable` wrappers around any of those. Every other type - including `Date32`, `DateTime64`, `Decimal`, `Enum`, `IPv4`, `IPv6`, `Tuple`, `Map`, `LowCardinality`, and the wide integers `Int128`/`UInt128`/`Int256`/`UInt256` - is rejected with a `NOT_IMPLEMENTED` exception instead of being passed through unobfuscated. Project such columns away or cast them to a supported type before obfuscating.

See also the `clickhouse obfuscator` tool, which implements the same algorithm over files.
)",
            .examples{{"obfuscate", "SELECT * FROM obfuscate(SELECT number, toString(number) FROM numbers(10000)) LIMIT 10", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = true});
}

}
