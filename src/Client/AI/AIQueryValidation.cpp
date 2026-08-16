#include <Client/AI/AIQueryValidation.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTCheckDatabaseQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTDescribeCacheQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
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

/// The settings that enforce the sandbox of the read-only tool, plus `log_comment`, which marks
/// the query in the query log as one the agent ran on its own. They are applied by the
/// client before sending the query, so a SETTINGS clause of the query itself could
/// override them and must be rejected. `profile` and `compatibility` are special settings
/// that expand into changes of many other settings on the server, so they could redefine
/// the protected ones indirectly.
bool isProtectedSetting(const String & name)
{
    return name == "readonly"
        || name == "max_execution_time"
        || name == "max_execution_time_leaf"
        || name == "max_memory_usage"
        || name == "max_memory_usage_for_user"
        || name == "log_comment"
        || name == "profile"
        || name == "compatibility";
}

/// The format-schema settings have side effects beyond the validated AST: with
/// `format_schema_source = 'query'`, `FormatSchemaInfo::querySchema` executes the query from
/// `format_schema` after this validation, and the schema-source modes write cached schema
/// files. `SELECT 1 FORMAT Protobuf SETTINGS format_schema_source = 'query', format_schema =
/// 'SELECT ...'` would otherwise smuggle an unvalidated query through the read-only tool.
bool isFormatSchemaSetting(const String & name)
{
    return name == "format_schema"
        || name == "format_schema_source"
        || name == "format_schema_message_name"
        || name == "output_format_schema";
}

void checkNoProtectedSettingChanges(const IAST & ast)
{
    if (const auto * set_query = ast.as<ASTSetQuery>())
    {
        const auto reject = [](const String & name)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The query changes the setting `{}` that affects the limits of the read-only tool. "
                "Remove it from the SETTINGS clause, or use the run_query tool",
                name);
        };
        const auto reject_format_schema = [](const String & name)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The query changes the setting `{}`: the format schema can execute another query "
                "(`format_schema_source = 'query'`) or write schema files, which is outside of what "
                "the read-only tool validates. Use the run_query tool for this query",
                name);
        };
        for (const auto & change : set_query->changes)
        {
            if (isProtectedSetting(change.name))
                reject(change.name);
            if (isFormatSchemaSetting(change.name))
                reject_format_schema(change.name);
        }
        /// `SETTINGS max_execution_time = DEFAULT` lands in `default_settings`, not `changes`,
        /// and resets the limit after the sandbox has tightened it.
        for (const auto & name : set_query->default_settings)
        {
            if (isProtectedSetting(name))
                reject(name);
            if (isFormatSchemaSetting(name))
                reject_format_schema(name);
        }
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

/// The few scalar (non-table) functions that read external resources by a path from the query,
/// and the AI functions: they send the query data to an external AI provider and incur cost,
/// which must not happen without the user's confirmation.
bool isDeniedScalarFunction(const String & name)
{
    static const std::unordered_set<String> denied
    {
        "file",
        "catboostevaluate",
        "aigenerate",
        "aiclassify",
        "aiextract",
        "aitranslate",
        "aiembed",
        "aisimilarity",
    };
    const String lowercase_name = Poco::toLower(name);
    return denied.contains(lowercase_name)
        || startsWith(lowercase_name, "dictget")
        || lowercase_name == "dicthas"
        || lowercase_name == "dictisin";
}

/// A named table can be a view. Its definition is expanded only on the server, after this
/// validation, and can hide external table functions or AI functions. The client cannot inspect
/// the definition without adding another authorization boundary, so unconfirmed queries only
/// admit the server-owned `system` tables. User tables, views, and CTE names require the
/// confirmed tool. This is deliberately stricter than `readonly = 1`: it preserves the promise
/// that the unconfirmed tool cannot reach resources beyond the local server.
bool isAllowedNamedTable(const ASTTableIdentifier & table)
{
    return table.getDatabaseName() == "system";
}

/// Whether the function is a builtin known to the client. This validation runs on the raw
/// parsed AST, before the server expands SQL user-defined functions, so a UDF wrapping an
/// external table function (e.g. `CREATE FUNCTION f AS path -> file(path)`) would slip
/// through the checks above. UDFs only exist on the server, so any function the client does
/// not recognize is conservatively rejected (a false positive sends the query through
/// run_query, which is fine; so does a builtin of a newer server that this client predates).
bool isKnownBuiltinFunction(const String & name)
{
    /// Constructs that are represented as functions in the AST but are resolved specially
    /// and are not registered in the factories.
    static const std::unordered_set<String> ast_level_constructs
    {
        "lambda",
        "exists",
        "grouping",
        "untuple",
    };
    if (ast_level_constructs.contains(name))
        return true;

    return FunctionFactory::instance().hasNameOrAlias(name)
        || AggregateFunctionFactory::instance().isAggregateFunctionName(name);
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
        else if (table_expression->database_and_table_name)
        {
            const auto & table = table_expression->database_and_table_name->as<ASTTableIdentifier &>();
            if (!isAllowedNamedTable(table))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The table `{}` may be a view whose definition reaches resources outside of the tables of the current "
                    "server, so it is not allowed for the read-only tool. Use the run_query tool for this query",
                    table.name());
        }
    }
    else if (const auto * function = ast.as<ASTFunction>())
    {
        if (isDeniedScalarFunction(function->name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The function `{}` reaches external resources or services, so it is not allowed for the "
                "read-only tool. Use the run_query tool for this query",
                function->name);

        /// The allowed table functions are registered in the table-function factory, not the
        /// scalar ones, and the traversal also visits them here (as children of the table
        /// expression or of an IN operator).
        if (!isKnownBuiltinFunction(function->name) && !isAllowedTableFunction(function->name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The function `{}` is not a builtin known to the client: it may be a user-defined function "
                "reaching resources outside of the tables of the current server, which cannot be verified "
                "before execution. Use the run_query tool for this query",
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

bool changesSettingsForAIAgent(const IAST & ast)
{
    if (ast.as<ASTSetQuery>())
        return true;

    for (const auto & child : ast.children)
    {
        if (changesSettingsForAIAgent(*child))
            return true;
    }

    return false;
}

bool isReadOnlyStatementForAIAgent(const IAST & ast)
{
    return isAnyOf<
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
}

void validateReadOnlyQueryForAIAgent(const IAST & ast)
{
    if (!isReadOnlyStatementForAIAgent(ast))
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
