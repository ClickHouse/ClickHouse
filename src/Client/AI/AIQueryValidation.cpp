#include <Client/AI/AIQueryValidation.h>

#include <Common/Exception.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTCheckDatabaseQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTDescribeCacheQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowEngineQuery.h>
#include <Parsers/ASTShowFunctionsQuery.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTShowSettingQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Parsers/Access/ASTShowAccessQuery.h>
#include <Parsers/Access/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTShowPrivilegesQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Poco/String.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename... Ts>
bool isAnyOf(const IAST & ast)
{
    return (... || (ast.as<Ts>() != nullptr));
}

/// The settings that enforce the sandbox of the read-only tool. They are applied by the
/// client before sending the query, so a SETTINGS clause of the query itself could
/// override them and must be rejected.
bool isProtectedSetting(const String & name)
{
    return name == "readonly"
        || name == "max_execution_time"
        || name == "max_execution_time_leaf"
        || name == "max_memory_usage"
        || name == "max_memory_usage_for_user";
}

void checkNoProtectedSettingChanges(const IAST & ast)
{
    if (const auto * set_query = ast.as<ASTSetQuery>())
    {
        for (const auto & change : set_query->changes)
            if (isProtectedSetting(change.name))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The query changes the setting `{}` that enforces the limits of the read-only tool. "
                    "Remove it from the SETTINGS clause, or use the run_query tool",
                    change.name);
    }

    for (const auto & child : ast.children)
        checkNoProtectedSettingChanges(*child);
}

/// Table functions that only generate data on the current server and cannot reach files,
/// the network or other external resources. `readonly = 1` blocks writes but does not confine
/// a SELECT to the current server schema: `file`, `url`, `s3`, `remote`, `executable`, `mysql`,
/// `cluster` and similar can read external resources, so they require the run_query tool with
/// the user's confirmation. The check is a conservative allowlist: an unknown table function
/// is rejected. The names are compared case-insensitively: some of the dangerous functions
/// are registered with case-insensitive names.
bool isAllowedTableFunction(const String & name)
{
    static const std::unordered_set<String> allowed
    {
        "numbers",
        "numbers_mt",
        "zeros",
        "zeros_mt",
        "generateseries",
        "generate_series",
        "generaterandom",
        "values",
        "format",
        "null",
    };
    return allowed.contains(Poco::toLower(name));
}

/// The few scalar (non-table) functions that read external resources by a path from the query.
bool isDeniedScalarFunction(const String & name)
{
    const String lower = Poco::toLower(name);
    return lower == "file" || lower == "catboostevaluate";
}

void checkNoExternalAccess(const IAST & ast)
{
    if (const auto * table_expression = ast.as<ASTTableExpression>())
    {
        if (table_expression->table_function)
        {
            const auto & function = table_expression->table_function->as<ASTFunction &>();
            if (!isAllowedTableFunction(function.name))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The table function `{}` may reach resources outside of the tables of the current server, "
                    "so it is not allowed for the read-only tool. Use the run_query tool for this query",
                    function.name);
        }
    }
    else if (const auto * function = ast.as<ASTFunction>())
    {
        if (isDeniedScalarFunction(function->name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The function `{}` reads external resources, so it is not allowed for the read-only tool. "
                "Use the run_query tool for this query",
                function->name);

        /// The right-hand side of an IN operator can be a table function: `x IN remote(...)`.
        /// Reject any function there except tuple/array literals and the allowed table functions
        /// (a false positive sends the query through run_query, which is fine).
        if (functionIsInOrGlobalInOperator(function->name) && function->arguments && function->arguments->children.size() == 2)
        {
            if (const auto * rhs = function->arguments->children[1]->as<ASTFunction>();
                rhs && rhs->name != "tuple" && rhs->name != "array" && !isAllowedTableFunction(rhs->name))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The function `{}` on the right-hand side of IN may be a table function reaching resources "
                    "outside of the tables of the current server, so it is not allowed for the read-only tool. "
                    "Use the run_query tool for this query",
                    rhs->name);
        }
    }

    for (const auto & child : ast.children)
        checkNoExternalAccess(*child);
}

}

void validateReadOnlyQueryForAIAgent(const IAST & ast)
{
    bool allowed = isAnyOf<
        ASTSelectWithUnionQuery,
        ASTExplainQuery,
        ASTDescribeQuery,
        ASTDescribeCacheQuery,
        ASTShowTablesQuery,
        ASTShowColumnsQuery,
        ASTShowIndexesQuery,
        ASTShowEnginesQuery,
        ASTShowFunctionsQuery,
        ASTShowSettingQuery,
        ASTShowProcesslistQuery,
        ASTExistsDatabaseQuery,
        ASTExistsTableQuery,
        ASTExistsViewQuery,
        ASTExistsDictionaryQuery,
        ASTShowCreateTableQuery,
        ASTShowCreateViewQuery,
        ASTShowCreateDatabaseQuery,
        ASTShowCreateDictionaryQuery,
        ASTCheckTableQuery,
        ASTCheckAllTablesQuery,
        ASTCheckDatabaseQuery,
        ASTShowAccessQuery,
        ASTShowAccessEntitiesQuery,
        ASTShowCreateAccessEntityQuery,
        ASTShowGrantsQuery,
        ASTShowPrivilegesQuery>(ast);

    if (!allowed)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Only read-only statements (SELECT, EXPLAIN, SHOW, DESCRIBE, EXISTS, CHECK) can be run "
            "without confirmation. Use the run_query tool for this query");

    if (const auto * with_output = dynamic_cast<const ASTQueryWithOutput *>(&ast); with_output && with_output->out_file)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "INTO OUTFILE is not allowed for the read-only tool because it writes to the user's files. "
            "Use the run_query tool for this query");

    checkNoProtectedSettingChanges(ast);
    checkNoExternalAccess(ast);
}

}
