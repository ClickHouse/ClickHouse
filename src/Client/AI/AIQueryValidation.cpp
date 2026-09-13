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
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Parsers/Access/ASTShowAccessQuery.h>
#include <Parsers/Access/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTShowPrivilegesQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Poco/String.h>

#include <algorithm>
#include <string_view>
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
        || name == "compatibility"
        /// Masking of the credentials in `SHOW CREATE TABLE` of external-engine tables: the
        /// sandbox turns it on, and a generated `SETTINGS` clause must not turn it back off.
        || name == "format_display_secrets_in_show_and_select"
        /// Visiting a remote database or a data-lake catalog in `system.tables` /
        /// `system.columns` contacts it, so the sandbox turns both off; a generated `SETTINGS`
        /// clause must not turn them back on.
        || name == "show_remote_databases_in_system_tables"
        || name == "show_data_lake_catalogs_in_system_tables";
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
        || name == "output_format_schema"
        || name == "format_template_resultset"
        || name == "format_template_row";
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
        "aifilter",
        "airedact",
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

/// The right-hand side of an IN operator names a table or a table function without an
/// `ASTTableExpression` around it: `x IN some_view`, `x IN remote(...)`. The generic traversal
/// never sees a table there, so this position is checked separately.
///
/// Only the expression itself can name a table; the parser has already unwrapped the parentheses
/// of `x IN (some_view)`. Inside a tuple or an array the elements are ordinary expressions, and
/// the server resolves a name there as a column only - so they are deliberately not descended
/// into, which also keeps `x IN (toDate('2026-01-01'), toDate('2026-01-02'))` allowed.
///
/// A name in this position is ambiguous: the server resolves it as a column of the enclosing
/// query first and only then as a table, and the client cannot tell the two apart before
/// execution. It is collected as a table reference either way, and judged once resolved.
void checkNoTableAccessInSetExpression(const IAST & ast, bool allow_system_tables)
{
    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        /// A name of any other number of parts cannot be a table, so it leaves the database empty.
        const auto & parts = identifier->name_parts;
        const String database = parts.size() == 2 ? parts.front() : "";

        /// The schema restriction hides every database the server owns (`system` and both
        /// spellings of `information_schema`). Because of the ambiguity above, an unqualified name
        /// is rejected as well - it is usually a column, but the session database can make it a
        /// `system` table, and nothing here can tell.
        if (!allow_system_tables && (isServerOwnedDatabaseForAIAgent(database) || database.empty()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Schema access is disabled for the read-only tool. Use the run_query tool for this query");
    }
    else if (const auto * function = ast.as<ASTFunction>())
    {
        /// A tuple or an array here is a set of values rather than a table.
        if (function->name != "tuple" && function->name != "array" && !isAllowedTableFunction(function->name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The function `{}` on the right-hand side of IN may be a table function reaching resources "
                "outside of the tables of the current server, so it is not allowed for the read-only tool. "
                "Use the run_query tool for this query",
                function->name);
    }
}

/// The set expression of an IN operator, when the operator has the shape this validation can
/// reason about. `null` when it does not, in which case the generic traversal still applies.
const IAST * getSetExpressionOfInOperator(const ASTFunction & function)
{
    if (!functionIsInOrGlobalInOperator(function.name) || !function.arguments || function.arguments->children.size() != 2)
        return nullptr;
    return function.arguments->children[1].get();
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

/// Rejects the external access that is written in the query text: the table functions and the
/// scalar functions that reach outside of the current server. A plain table name is not judged
/// here - see `collectNamedTablesForAIAgent`.
///
/// Autonomous access to metadata is a separate concern, gated by `allow_schema_access`, which
/// rejects the schema-exploration statements by their type in `isSchemaExplorationStatement`.
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
        /// A plain table name is not judged here: what reading it does depends on what it
        /// resolves to on the server. `collectNamedTablesForAIAgent` gathers those names and the
        /// caller checks the engine of each one.
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

        /// The right-hand side of an IN operator can name a table or a table function without an
        /// `ASTTableExpression`: `x IN remote(...)`, `x IN some_view`.
        if (const auto * set_expression = getSetExpressionOfInOperator(*function))
            checkNoTableAccessInSetExpression(*set_expression, /*allow_system_tables=*/ true);
    }

    for (const auto & child : ast.children)
        checkNoExternalAccess(*child);
}

void checkNoSchemaAccess(const IAST & ast)
{
    if (const auto * table_expression = ast.as<ASTTableExpression>())
    {
        if (table_expression->database_and_table_name)
        {
            const auto & table = table_expression->database_and_table_name->as<ASTTableIdentifier &>();
            /// Every database the server owns, not just `system`: `information_schema` is views
            /// over `system` by design, so it is the same schema surface under another name.
            if (isServerOwnedDatabaseForAIAgent(table.getDatabaseName()))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Schema access is disabled for the read-only tool. Use the run_query tool for this query");
        }
    }
    else if (const auto * function = ast.as<ASTFunction>())
    {
        /// `x IN system.tables` reads a table without an `ASTTableExpression`, so this position
        /// needs the schema restriction applied separately.
        if (const auto * set_expression = getSetExpressionOfInOperator(*function))
            checkNoTableAccessInSetExpression(*set_expression, /*allow_system_tables=*/ false);
    }

    for (const auto & child : ast.children)
        checkNoSchemaAccess(*child);
}

bool isSchemaExplorationStatement(const IAST & ast)
{
    return isAnyOf<
        ASTDescribeQuery,
        ASTDescribeCacheQuery,
        ASTShowTablesQuery,
        ASTShowColumnsQuery,
        ASTShowIndexesQuery,
        ASTShowEnginesQuery,
        ASTShowFunctionsQuery,
        ASTShowSettingQuery,
        ASTExistsDatabaseQuery,
        ASTExistsTableQuery,
        ASTExistsViewQuery,
        ASTExistsDictionaryQuery,
        ASTShowCreateTableQuery,
        ASTShowCreateViewQuery,
        ASTShowCreateDatabaseQuery,
        ASTShowCreateDictionaryQuery>(ast);
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

bool isReadOnlyStatementForAISession(const IAST & ast)
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
        ASTShowAccessQuery,
        ASTShowAccessEntitiesQuery,
        ASTShowCreateAccessEntityQuery,
        ASTShowGrantsQuery,
        ASTShowPrivilegesQuery,
        ASTCheckTableQuery,
        ASTCheckAllTablesQuery,
        ASTCheckDatabaseQuery>(ast);
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
        ASTShowAccessQuery,
        ASTShowAccessEntitiesQuery,
        ASTShowCreateAccessEntityQuery,
        ASTShowGrantsQuery,
        ASTShowPrivilegesQuery>(ast);
}

namespace
{

/// Gathers the table names of one node into `result`, keeping the order of first appearance.
void collectNamedTables(const IAST & ast, std::vector<AIQueryTableReference> & result)
{
    auto add = [&](String database, String table)
    {
        AIQueryTableReference reference{std::move(database), std::move(table)};
        if (std::ranges::find(result, reference) == result.end())
            result.push_back(std::move(reference));
    };

    if (const auto * table_expression = ast.as<ASTTableExpression>())
    {
        if (table_expression->database_and_table_name)
        {
            const auto & table = table_expression->database_and_table_name->as<ASTTableIdentifier &>();
            add(table.getDatabaseName(), table.shortName());
        }
    }
    else if (const auto * function = ast.as<ASTFunction>())
    {
        /// `x IN some_view` reads a table without an `ASTTableExpression` around it.
        if (const auto * set_expression = getSetExpressionOfInOperator(*function))
        {
            if (const auto * identifier = set_expression->as<ASTIdentifier>())
            {
                const auto & parts = identifier->name_parts;
                if (parts.size() == 1)
                    add("", parts.front());
                else if (parts.size() == 2)
                    add(parts.front(), parts.back());
            }
        }
    }
    else if (const auto * with_table = dynamic_cast<const ASTQueryWithTableAndOutput *>(&ast))
    {
        /// `EXISTS TABLE`, `EXISTS VIEW`, `EXISTS DICTIONARY`, `SHOW CREATE TABLE`,
        /// `SHOW CREATE VIEW`, `SHOW CREATE DICTIONARY`: the target of the statement is not an
        /// `ASTTableExpression`, it is a pair of identifier children of the statement itself, and
        /// the traversal below sees them as bare identifiers it cannot tell from a column.
        /// `EXISTS DATABASE` and `SHOW CREATE DATABASE` leave `table` unset and name no table.
        if (with_table->table)
            add(with_table->getDatabase(), with_table->getTable());
    }
    else if (const auto * show_columns = ast.as<ASTShowColumnsQuery>())
    {
        /// `SHOW COLUMNS` and `SHOW INDEXES` keep their target in plain strings rather than in
        /// children, so the traversal below cannot reach it at all.
        add(show_columns->database, show_columns->table);
    }
    else if (const auto * show_indexes = ast.as<ASTShowIndexesQuery>())
    {
        add(show_indexes->database, show_indexes->table);
    }
    else if (const auto * show_tables = ast.as<ASTShowTablesQuery>())
    {
        /// `SHOW TABLES FROM db` and `SHOW DICTIONARIES FROM db` name a database and no table.
        /// It has to be collected even though `system.tables` is told not to enumerate a remote
        /// database or a data-lake catalog: `InterpreterShowTablesQuery` turns
        /// `show_remote_databases_in_system_tables` and `show_data_lake_catalogs_in_system_tables`
        /// back on for the database that was asked for, so enumerating it contacts it.
        if (!show_tables->databases && !show_tables->clusters && !show_tables->cluster && !show_tables->m_settings
            && !show_tables->merges && !show_tables->caches && !show_tables->temporary)
            add(show_tables->getFrom(), "");
    }

    for (const auto & child : ast.children)
        collectNamedTables(*child, result);
}

}

std::vector<AIQueryTableReference> collectNamedTablesForAIAgent(const IAST & ast)
{
    std::vector<AIQueryTableReference> result;
    collectNamedTables(ast, result);
    return result;
}

bool isServerOwnedDatabaseForAIAgent(const String & database)
{
    /// `information_schema` exists in both spellings, and its tables are views over `system`.
    return database == "system" || database == "information_schema" || database == "INFORMATION_SCHEMA";
}

bool isAllowedServerOwnedTableForAIAgent(const String & database, const String & table)
{
    /// The `system` tables whose read reaches beyond the local server. Most of them talk to
    /// Keeper: `zookeeper` reads znodes, `zookeeper_info` opens sockets to every configured
    /// Keeper host for the `mntr`/`isro` commands, `distributed_ddl_queue` and the queue-metadata
    /// tables read the queue state stored in Keeper, `replicas` and `database_replicas` request
    /// the replication state there, and even the connection/watch views can establish the
    /// server's Keeper session lazily. The Iceberg tables read the table metadata from the
    /// object storage.
    static const std::unordered_set<std::string_view> external_system_tables
    {
        "zookeeper",
        "zookeeper_connection",
        "zookeeper_info",
        "zookeeper_watches",
        "distributed_ddl_queue",
        "s3_queue_metadata",
        "azure_queue_metadata",
        "replicas",
        "database_replicas",
        "iceberg_history",
        "iceberg_files",
    };
    return database != "system" || !external_system_tables.contains(table);
}

bool isAllowedTableEngineForAIAgent(const String & engine)
{
    /// The whole MergeTree family, however it is prefixed: `Replicated`, `Shared`, `Replacing`,
    /// `Summing`, `Aggregating`, `Collapsing`, `Graphite`, and the combinations of those.
    if (engine.ends_with("MergeTree"))
        return true;

    static const std::unordered_set<std::string_view> allowed
    {
        /// The Log family and the simple engines: local data, no stored definition to execute.
        "Log",
        "TinyLog",
        "StripeLog",
        "Memory",
        "Null",
        "Set",
        "Join",
        "EmbeddedRocksDB",
    };
    return allowed.contains(engine);
}

bool isAllowedDatabaseEngineForAIAgent(const String & engine)
{
    static const std::unordered_set<std::string_view> allowed
    {
        "Atomic",
        "Ordinary",
        "Memory",
        "Lazy",
        "Replicated",
        "Shared",
        /// The default database of `clickhouse-local`.
        "Overlay",
    };
    return allowed.contains(engine);
}

void validateReadOnlyQueryForAIAgent(const IAST & ast, bool allow_schema_access)
{
    if (!isReadOnlyStatementForAIAgent(ast))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Only read-only statements (SELECT, EXPLAIN, SHOW, DESCRIBE, EXISTS) can be run "
            "without confirmation. Use the run_query tool for this query");

    if (const auto * with_output = dynamic_cast<const ASTQueryWithOutput *>(&ast); with_output && with_output->out_file)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "INTO OUTFILE is not allowed for the read-only tool because it writes to the user's files. "
            "Use the run_query tool for this query");

    checkNoProtectedSettingChanges(ast);
    checkNoExternalAccess(ast);
    if (!allow_schema_access)
    {
        if (isSchemaExplorationStatement(ast))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Schema access is disabled for the read-only tool. Use the run_query tool for this query");
        checkNoSchemaAccess(ast);
    }
}

}
