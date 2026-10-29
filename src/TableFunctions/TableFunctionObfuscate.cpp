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
    /// in `ObfuscateSource`.
    auto inner_context = ObfuscateSource::makeInnerContext(context);

    if (inner_context->getSettingsRef()[Setting::allow_experimental_analyzer])
        sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(create.children[0], inner_context);
    else
        sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(create.children[0], inner_context);

    /// Neither the `obfuscate_*` settings nor the output types are validated here, although that would
    /// let `DESCRIBE` and `CREATE VIEW` reject a definition that throws on every read. Both checks
    /// belong to `ObfuscateStep`, which the Planner builds, and neither is sound at this point:
    ///
    /// - The types: this structure is the whole inner query, while a read builds a model only for the
    ///   columns it selects. Rejecting an unsupported type here would refuse a perfectly readable
    ///   `SELECT supported FROM obfuscate(SELECT supported, unsupported ...)` and a view over it.
    /// - The settings: `context` is not always the context the read will use. `QueryAnalyzer` hands a
    ///   table function the nearest query scope's context only when that scope is a *subquery* with a
    ///   `SETTINGS` clause, and uses the query context otherwise, so when the structure of a
    ///   `view(SELECT ... FROM obfuscate(...) SETTINGS obfuscate_markov_order = ...)` is derived, the
    ///   clause is invisible here but applied when the view is read (see
    ///   `04894_obfuscate_inner_default_settings_remote`, where such a clause repairs an invalid
    ///   session setting).
    ///
    /// `ObfuscateStep` runs as soon as the read columns and the reading context are both known, which
    /// is the earliest point at which either check answers the question a read would ask.
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

Some pieces of the data are intentionally preserved exactly, matching the `clickhouse obfuscator` tool: `Date` and `Date32` values pass through unchanged; `DateTime` and `DateTime64` values keep the exact source date component (as displayed in the column's timezone) and only obfuscate the time of day (and the sub-second part); `Nullable` columns keep the original null map, so which rows are `NULL` does not change; `Array` and `Map` columns keep the original sizes. Do not rely on this table function to hide dates or the pattern of missing values.

The table function is a repeating, effectively infinite source: it trains on the result of the inner query and then re-executes that query to generate obfuscated rows, advancing the seed on every pass. The passes form one continuous obfuscated stream, not independent re-obfuscations: the delta-based models (`BFloat16`/`Float32`/`Float64` and the time-of-day part of `DateTime`/`DateTime64`) carry their previous-value state across pass boundaries, exactly like the multi-pass mode of the `clickhouse obfuscator` tool. Always bound the output with an outer `LIMIT` (as in the example below); otherwise the query runs until cancelled.

The set of supported column types is the one the `clickhouse obfuscator` tool implements: integers of every width (including `Bool`), `Decimal`, `IPv4` and `IPv6` (a pseudorandom permutation that keeps the sign and the magnitude of the value up to a power of two), `BFloat16`, `Float32`, `Float64`, `Date`, `Date32`, `DateTime`, `DateTime64`, `String`, `FixedString`, `UUID`, `Enum8` and `Enum16` (a pseudorandom permutation of the values of the type), and the `Array`, `Nullable`, `LowCardinality`, `Tuple` and `Map` wrappers around any of those. Every other type - for example `Variant`, `Dynamic`, `JSON`, `Time`, `Time64` or `Interval` - is rejected with a `NOT_IMPLEMENTED` exception that names the type and the column, instead of being passed through unobfuscated. Project such columns away or cast them to a supported type before obfuscating.

It cannot be persisted as a table: `CREATE TABLE ... AS obfuscate(...)` is rejected, because such a table builds its storage from the context captured at DDL time and could not interpret the query argument. Define an ordinary `VIEW` over `obfuscate` instead.

See also the `clickhouse obfuscator` tool, which implements the same algorithm over files.
)",
            .examples{{"obfuscate", "SELECT * FROM obfuscate(SELECT number, toString(number) FROM numbers(10000)) LIMIT 10", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        SecretArgumentsSpec{},
        {.allow_readonly = true});
}

}
