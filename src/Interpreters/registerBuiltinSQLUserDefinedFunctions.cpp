#include <Interpreters/registerBuiltinSQLUserDefinedFunctions.h>

#include <Core/Settings.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
constexpr const char * locality_function_name = "timeSeriesMetricLocalityId";

bool timeSeriesMetricLocalityIdNameTakenByNonSqlUserDefinedFunction(const ContextPtr & context)
{
    if (UserDefinedWebAssemblyFunctionFactory::instance().has(locality_function_name))
        return true;
    if (UserDefinedExecutableFunctionFactory::instance().has(locality_function_name, context))
        return true;
    return false;
}

const String & canonicalTimeSeriesMetricLocalityIdCreateQuery()
{
    static const String query
        = "CREATE OR REPLACE FUNCTION timeSeriesMetricLocalityId AS x -> toUInt32(sipHash64(x))";
    return query;
}

ASTPtr parseCanonicalTimeSeriesMetricLocalityIdCreateQuery(const ContextPtr & context)
{
    const auto & query = canonicalTimeSeriesMetricLocalityIdCreateQuery();
    const auto & settings = context->getSettingsRef();
    ParserCreateFunctionQuery parser;
    return parseQuery(
        parser,
        query.data(),
        query.data() + query.size(),
        "registerBuiltinSQLUserDefinedFunctions",
        0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
}

/// At server startup: register canonical UDF only if the name is free. Do not validate an existing definition
/// (user may own `timeSeriesMetricLocalityId` for non-TimeSeries use); strict checks run when TimeSeries runs
/// (`ensureTimeSeriesMetricLocalityIdUserDefinedFunction`).
void registerTimeSeriesMetricLocalityIdIfAbsent(ContextMutablePtr context)
{
    ASTPtr expected_ast = parseCanonicalTimeSeriesMetricLocalityIdCreateQuery(context);
    if (!expected_ast->as<ASTCreateSQLFunctionQuery>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST for builtin UDF: {}", expected_ast->formatForLogging());

    if (UserDefinedSQLFunctionFactory::instance().has(locality_function_name))
        return;

    /// Do not use replace_if_exists: it can drop a WASM UDF with the same name (see UserDefinedSQLFunctionFactory::registerFunction).
    if (timeSeriesMetricLocalityIdNameTakenByNonSqlUserDefinedFunction(context))
        return;

    UserDefinedSQLFunctionFactory::instance().registerFunction(
        context, locality_function_name, expected_ast, /* throw_if_exists */ false, /* replace_if_exists */ false);
}

/// When TimeSeries query paths run: register if missing; if present, require normalized \c function_core AST to
/// match the canonical `x -> toUInt32(sipHash64(x))` or throw \c BAD_ARGUMENTS.
void registerOrValidateTimeSeriesMetricLocalityId(ContextMutablePtr context)
{
    ASTPtr expected_ast = parseCanonicalTimeSeriesMetricLocalityIdCreateQuery(context);
    if (!expected_ast->as<ASTCreateSQLFunctionQuery>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST for builtin UDF: {}", expected_ast->formatForLogging());

    if (!UserDefinedSQLFunctionFactory::instance().has(locality_function_name))
    {
        if (timeSeriesMetricLocalityIdNameTakenByNonSqlUserDefinedFunction(context))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "TimeSeries requires the SQL user-defined function {} with definition `x -> toUInt32(sipHash64(x))`, "
                "but a WebAssembly or executable user-defined function with that name already exists. "
                "Drop or rename the conflicting function.",
                locality_function_name);

        UserDefinedSQLFunctionFactory::instance().registerFunction(
            context, locality_function_name, expected_ast, /* throw_if_exists */ false, /* replace_if_exists */ false);
        return;
    }

    ASTPtr existing_ast = UserDefinedSQLFunctionFactory::instance().tryGet(locality_function_name);
    if (!existing_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is registered but could not be loaded", locality_function_name);

    const auto * existing_create = existing_ast->as<ASTCreateSQLFunctionQuery>();
    if (!existing_create)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Existing function {} must be a SQL UDF with definition `x -> toUInt32(sipHash64(x))` "
            "(TimeSeries metric locality compatibility). Found a non-SQL function with the same name.",
            locality_function_name);

    ASTPtr normalized_expected = normalizeCreateFunctionQuery(*expected_ast, context);
    ASTPtr normalized_existing = normalizeCreateFunctionQuery(*existing_ast, context);

    const auto * expected_q = normalized_expected->as<ASTCreateSQLFunctionQuery>();
    const auto * existing_q = normalized_existing->as<ASTCreateSQLFunctionQuery>();
    if (!expected_q || !existing_q || !expected_q->function_core || !existing_q->function_core)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST for SQL function {}", locality_function_name);

    if (expected_q->function_core->getTreeHash(/* ignore_aliases= */ true) != existing_q->function_core->getTreeHash(/* ignore_aliases= */ true))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The SQL user-defined function {} must be exactly `x -> toUInt32(sipHash64(x))` "
            "(TimeSeries metric locality). Drop it or replace it with that definition.",
            locality_function_name);
}
}

void ensureTimeSeriesMetricLocalityIdUserDefinedFunction(ContextMutablePtr context)
{
    registerOrValidateTimeSeriesMetricLocalityId(std::move(context));
}

void registerBuiltinSQLUserDefinedFunctions(ContextMutablePtr context)
{
    registerTimeSeriesMetricLocalityIdIfAbsent(std::move(context));
}

}
