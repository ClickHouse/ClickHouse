#include <Client/AI/AIQueryValidation.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Defines.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTCheckDatabaseQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTDescribeCacheQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/ASTUseQuery.h>
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
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTShowPrivilegesQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

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
/// `format_avro_schema_registry_url` is the schema source of `AvroConfluent`: reading or writing
/// that format contacts the registry at the URL, which an unconfirmed query must not do.
bool isFormatSchemaSetting(const String & name)
{
    return name == "format_schema"
        || name == "format_schema_source"
        || name == "format_schema_message_name"
        || name == "output_format_schema"
        || name == "format_template_resultset"
        || name == "format_template_row"
        || name == "format_avro_schema_registry_url";
}

/// The settings that carry SQL or a table name the server applies to the query after this
/// validation, so they would hide what the query reads or calls: `select`, `filter`, `order` and
/// `sort` wrap the query in another one parsed from the setting (`select = 'file(''secret.txt'')'`),
/// `additional_table_filters` and `additional_result_filter` add filters parsed from it, and
/// `implicit_table_at_top_level` reads a table the query does not name. `database` makes the
/// unqualified names of the query resolve to another database than the one they were checked in.
bool isQueryConstructionSetting(const String & name)
{
    return name == "select"
        || name == "filter"
        || name == "order"
        || name == "sort"
        || name == "additional_table_filters"
        || name == "additional_result_filter"
        || name == "implicit_table_at_top_level"
        || name == "database";
}

/// The settings of parallel replicas: they make the server run the query on other replicas than
/// the tables it was checked on (`cluster_for_parallel_replicas`), and in the custom-key modes they
/// add a filter parsed from `parallel_replicas_custom_key` after this validation - even to a read
/// of a `Memory` table on this server, so `parallel_replicas_custom_key = 'generateSerialID(...)'`
/// would call a function the validation refuses. The sandbox turns parallel replicas off, so a
/// `SETTINGS` clause must not turn them back on or configure them.
bool isParallelReplicasSetting(const String & name)
{
    return name == "allow_experimental_parallel_reading_from_replicas"
        || name.contains("parallel_replica");
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
                "(`format_schema_source = 'query'`), write schema files or be fetched from a schema registry, "
                "which is outside of what the read-only tool validates. Use the run_query tool for this query",
                name);
        };
        const auto reject_query_construction = [](const String & name)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The query changes the setting `{}`, which the server turns into a part of the query (an expression "
                "or a table to read) after the read-only tool has validated it. Use the run_query tool for this query",
                name);
        };
        const auto reject_parallel_replicas = [](const String & name)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The query changes the setting `{}` of parallel replicas, which can run the query on other servers "
                "or add an expression to it after the read-only tool has validated it. Use the run_query tool for this query",
                name);
        };
        for (const auto & change : set_query->changes)
        {
            if (isProtectedSetting(change.name))
                reject(change.name);
            if (isFormatSchemaSetting(change.name))
                reject_format_schema(change.name);
            if (isQueryConstructionSetting(change.name))
                reject_query_construction(change.name);
            if (isParallelReplicasSetting(change.name))
                reject_parallel_replicas(change.name);
        }
        /// `SETTINGS max_execution_time = DEFAULT` lands in `default_settings`, not `changes`,
        /// and resets the limit after the sandbox has tightened it.
        for (const auto & name : set_query->default_settings)
        {
            if (isProtectedSetting(name))
                reject(name);
            if (isFormatSchemaSetting(name))
                reject_format_schema(name);
            if (isQueryConstructionSetting(name))
                reject_query_construction(name);
            if (isParallelReplicasSetting(name))
                reject_parallel_replicas(name);
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

/// The allowed table functions that take the structure of the table they generate as a string:
/// `values('x UInt8, y UInt64 ALIAS ...', 1)`, `generateRandom('x UInt8 ...')` and
/// `format(JSONEachRow, 'x UInt8 DEFAULT ...', '{}')`. A column declaration there can carry an
/// expression - `DEFAULT`, `MATERIALIZED`, `ALIAS`, `EPHEMERAL`, `TTL` - that the server parses out of
/// the string and evaluates when the table is read, after this validation, which sees only a string.
/// So every string argument that parses as a list of column declarations must declare nothing but the
/// names and the types. The parser is the one the server uses for these arguments; a string too deep
/// for its default limits fails the validation with the error of the parser rather than being taken
/// for something else.
///
/// `format` and `generateRandom` also evaluate an argument that is an expression and use the string it
/// computes as the structure, and they take an identifier for its name, so only literals and
/// identifiers are accepted as their arguments. `values` takes the structure only from a literal; the
/// other arguments are values, and the rest of this validation judges them.
void checkStructureArgumentsOfTableFunction(const ASTFunction & function)
{
    const String lowercase_name = Poco::toLower(function.name);
    const bool is_values = lowercase_name == "values";
    if ((!is_values && lowercase_name != "generaterandom" && lowercase_name != "format") || !function.arguments)
        return;

    for (const auto & argument : function.arguments->children)
    {
        /// `SETTINGS k = v` of the table function, which `checkNoProtectedSettingChanges` sees.
        if (argument->as<ASTSetQuery>())
            continue;

        String text;
        if (const auto * literal = argument->as<ASTLiteral>())
        {
            if (literal->value.getType() != Field::Types::String)
                continue;
            text = literal->value.safeGet<String>();
        }
        else if (is_values)
        {
            continue;
        }
        else if (const auto * identifier = argument->as<ASTIdentifier>())
        {
            text = identifier->name();
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "An argument of the table function `{}` is an expression, which the server evaluates into the structure "
                "of the table, so the read-only tool cannot see the columns it declares. Write the arguments as "
                "literals, or use the run_query tool for this query",
                function.name);
        }

        ParserColumnDeclarationList parser(true, true);
        const char * begin = text.data();
        const char * end = text.data() + text.size();
        String error;
        const ASTPtr columns = tryParseQuery(
            parser, begin, end, error, false, "columns declaration list", false,
            /*max_query_size=*/ 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, true);
        if (!columns)
            continue;

        for (const auto & column : columns->children)
        {
            const auto * declaration = column->as<ASTColumnDeclaration>();
            if (declaration && !declaration->getDefaultExpression() && !declaration->getTTL())
                continue;
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The structure passed to the table function `{}` declares a column with an expression (`DEFAULT`, "
                "`MATERIALIZED`, `ALIAS`, `EPHEMERAL` or `TTL`), which the server evaluates after the read-only tool "
                "has validated the query. Declare only the names and the types of the columns, or use the run_query "
                "tool for this query",
                function.name);
        }
    }
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
        /// Not external, but it returns the values of the server settings as they are, the
        /// secrets among them unmasked, and it reveals the server configuration even when
        /// the schema access of the agent is disabled.
        "getserversetting",
        /// Not external either, but it writes: it creates and advances a counter in Keeper, and
        /// `readonly` does not stop it.
        "generateserialid",
        /// The consumers of dictionaries whose names do not say so: they read the dictionary named
        /// by an argument, which loads it from its source (an HTTP or a database source) if it is
        /// not loaded yet, just like `dictGet` does. `assignCentroid` takes either a dictionary or
        /// a literal array of centroids; both forms are rejected, as the client cannot tell which
        /// one a constant expression is before the server resolves it.
        "naivebayesclassifier",
        "naivebayesclassifierwithprob",
        "naivebayesclassifierwithallprobs",
        "predictxgboost",
        "assigncentroid",
        /// The functions that resolve a table named by a string argument, which the collection of
        /// the table names cannot see: resolving a table of a `PostgreSQL` or `MySQL` database
        /// contacts it, even before the function finds out that the table does not suit it.
        "hascolumnintable",
        "joinget",
        "joingetornull",
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
        checkStructureArgumentsOfTableFunction(*function);
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
/// leaves only `SELECT` and `EXPLAIN` to the read-only tool in `validateReadOnlyQueryForAIAgent`.
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
            checkStructureArgumentsOfTableFunction(function);
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

/// A query parameter is substituted by the server after this validation, and an `Identifier`
/// parameter names a table or a database: `SELECT * FROM {t:Identifier} SETTINGS param_t = 'v'`
/// reads `v`, while the validation sees a table without a name. The value of a parameter can also
/// come from the session (`SET param_t = ...`), which the validation cannot see at all.
void checkNoQueryParameters(const IAST & ast)
{
    if (ast.as<ASTQueryParameter>())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The query has a query parameter, which the server substitutes after the read-only tool has validated "
            "the query, so it is not allowed for the read-only tool. Write the value into the query, or use the "
            "run_query tool for this query");

    for (const auto & child : ast.children)
        checkNoQueryParameters(*child);
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

}

bool changesSecretDisplayForAIAgent(const IAST & ast)
{
    if (const auto * set_query = ast.as<ASTSetQuery>())
    {
        for (const auto & change : set_query->changes)
            if (change.name == "format_display_secrets_in_show_and_select")
                return true;
        /// `SETTINGS format_display_secrets_in_show_and_select = DEFAULT` lands here instead, and
        /// the default of the server is what the masking has to override in the first place.
        for (const auto & name : set_query->default_settings)
            if (name == "format_display_secrets_in_show_and_select")
                return true;
    }

    for (const auto & child : ast.children)
        if (changesSecretDisplayForAIAgent(*child))
            return true;

    return false;
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

bool isSessionStateStatementForAISession(const IAST & ast)
{
    if (const auto * set_role = ast.as<ASTSetRoleQuery>())
        return set_role->kind != ASTSetRoleQuery::Kind::SET_DEFAULT_ROLE;

    return isAnyOf<ASTUseQuery, ASTTransactionControl>(ast);
}

bool isReadOnlyStatementForAIAgent(const IAST & ast)
{
    /// `EXISTS DICTIONARY` and `SHOW CREATE DICTIONARY` are left out: their target is a dictionary
    /// by construction, never an ordinary table of this server, so there is nothing for the engine
    /// check to admit them by. They go through `run_query`, which the user confirms.
    ///
    /// `SHOW PROCESSLIST` and the statements about the access entities (`SHOW ACCESS`,
    /// `SHOW GRANTS`, `SHOW USERS`, `SHOW CREATE USER`, ...) are left out as well: they are not the
    /// data or the schema of the user, but the texts of the queries running on the server (of other
    /// sessions too, with the secrets they hold) and the authentication and access metadata. The
    /// same goes for the `system` tables behind them, see `isAllowedServerOwnedTableForAIAgent`.
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
        ASTExistsDatabaseQuery,
        ASTExistsTableQuery,
        ASTExistsViewQuery,
        ASTShowCreateTableQuery,
        ASTShowCreateViewQuery,
        ASTShowCreateDatabaseQuery,
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
        /// The definition of a database shows the parameters of its engine (a local path of
        /// `Filesystem`, the address of `Remote` or of a catalog), so `SHOW CREATE DATABASE` names
        /// the database itself, like `SHOW TABLES FROM db` does, for its engine to be checked.
        if (with_table->table)
            add(with_table->getDatabase(), with_table->getTable());
        else if (ast.as<ASTShowCreateDatabaseQuery>())
            add(with_table->getDatabase(), "");
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
    /// `information_schema.views` is `system.tables.as_select` under another name: the stored
    /// texts of the queries of the views, which is why `system.tables` is not allowed below.
    if (database == "information_schema" || database == "INFORMATION_SCHEMA")
        return table != "views" && table != "VIEWS";

    if (database != "system")
        return true;

    /// The `system` tables whose read reaches beyond the local server. Most of them talk to
    /// Keeper: `zookeeper` reads znodes, `zookeeper_info` opens sockets to every configured
    /// Keeper host for the `mntr`/`isro` commands, `distributed_ddl_queue` and the queue-metadata
    /// tables read the queue state stored in Keeper, `replicas` and `database_replicas` request
    /// the replication state there, and even the connection/watch views can establish the
    /// server's Keeper session lazily, and `clusters` reads the replica states of the
    /// `Replicated` databases from Keeper. The `keeper_*` tables expose the internals of the
    /// Keeper embedded in the server: its data tree, Raft membership, snapshots and changelogs.
    /// The Iceberg tables read the table metadata from the
    /// object storage, and `remote_data_paths` walks the metadata of the object storage disks.
    ///
    /// Then the tables that hold the texts of queries and of errors, or access metadata, rather
    /// than the data or the schema of the user: the texts of the queries running on the server
    /// (`processes`, `asynchronous_inserts`, `query_cache`), error messages, which quote the
    /// fragment of a query or the address of a source that failed (`errors`,
    /// `distribution_queue`, `dictionaries` with its `source` and `last_exception`, `backups`
    /// with its destination), and the authentication and access metadata. The log tables (below)
    /// are the same: `query_log` has the raw `exception`, `text_log` the messages of the server.
    /// The query history of the user is available through the `read_query_log` tool, which
    /// returns only what is safe of it.
    static const std::unordered_set<std::string_view> disallowed_system_tables
    {
        /// The stored definitions of the tables and views: `create_table_query` and `as_select`
        /// hold the texts of the queries of the views, kept off the unconfirmed path just like
        /// `SHOW CREATE` of a view is. The names and the engines of the tables are available
        /// through the `list_tables` tool, and the columns in `system.columns`.
        "tables",

        "zookeeper",
        "zookeeper_connection",
        "zookeeper_info",
        "zookeeper_watches",
        "keeper_storage",
        "keeper_cluster",
        "keeper_snapshots",
        "keeper_changelogs",
        "distributed_ddl_queue",
        "s3_queue_metadata",
        "azure_queue_metadata",
        /// The caches of the metadata of the earlier reads from external sources: the paths of
        /// the processed objects and files, the Keeper paths of the queues, the exception texts,
        /// and the `source` of every inferred schema.
        "s3queue_metadata_cache",
        "azure_queue_metadata_cache",
        "schema_inference_cache",
        "replicas",
        "database_replicas",
        "clusters",
        "iceberg_history",
        "iceberg_files",
        "remote_data_paths",

        "processes",
        "user_processes",
        "asynchronous_inserts",
        "query_cache",
        "errors",
        "distribution_queue",
        /// The raw texts of mutation commands and of the errors of the background operations:
        /// `mutations` has `command` and `latest_fail_reason`, `replication_queue` and
        /// `part_moves_between_shards` the `last_exception`, `view_refreshes` and
        /// `asynchronous_loader` the `exception`. A failed mutation keeps the literals of its
        /// command, and its error quotes them, even when they never reached a table.
        "mutations",
        "replication_queue",
        "part_moves_between_shards",
        "view_refreshes",
        "asynchronous_loader",
        "dictionaries",
        "backups",
        "users",
        "roles",
        "grants",
        "role_grants",
        "enabled_roles",
        "current_roles",
        "quotas",
        "quota_limits",
        "quota_usage",
        "quotas_usage",
        "settings_profiles",
        "settings_profile_elements",
        "row_policies",
        "user_directories",
        /// The definitions of the HTTP handlers created with `CREATE HANDLER`: their URLs and the
        /// texts of the queries they run (`url`, `query`, `create_query`).
        "handlers",
        /// Shows the values of the server settings with the secrets unmasked: it does not obey
        /// `format_display_secrets_in_show_and_select`.
        "server_settings",
        /// The stored definitions of the server-global objects (`collection`, `create_query`):
        /// the named collections with their connection parameters, the SQL and Wasm user-defined
        /// functions, and the workloads and resources of the scheduler. The names of the built-in
        /// functions are in `system.documentation`, which the `consult_documentation` tool reads.
        "named_collections",
        "functions",
        /// The executable user-defined functions: their reconstructed `command` lines and the
        /// texts of their loading errors (`loading_error_message`).
        "user_defined_functions",
        "workloads",
        "resources",
    };
    if (disallowed_system_tables.contains(table))
        return false;

    /// `query_log`, `user_query_log`, `query_thread_log`, `text_log`, `session_log`, ...
    return !table.ends_with("_log");
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

bool isStoredQueryTableEngineForAIAgent(const String & engine)
{
    return engine == "View" || engine == "MaterializedView" || engine == "LiveView" || engine == "WindowView";
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

void validateStoredExpressionForAIAgent(const String & expression, std::string_view origin, bool allow_schema_access)
{
    ParserExpression parser;
    const ASTPtr ast = parseQuery(
        parser, expression, "stored expression", /*max_query_size=*/ 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    try
    {
        checkNoExternalAccess(*ast);
        if (!allow_schema_access)
            checkNoSchemaAccess(*ast);
    }
    catch (Exception & e)
    {
        e.addMessage("in {}, which the server evaluates when the table is read", origin);
        throw;
    }

    /// A subquery of the expression reads a table this validation has not resolved; resolving it
    /// would take another round of the same check, for an expression that rarely needs one.
    if (!collectNamedTablesForAIAgent(*ast).empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The {} reads other tables, which the server does when the table is read, so it is not allowed for the "
            "read-only tool. Use the run_query tool for this query",
            origin);
}

void validateReadOnlyQueryForAIAgent(const IAST & ast, bool allow_schema_access)
{
    if (isAnyOf<ASTExistsDictionaryQuery, ASTShowCreateDictionaryQuery>(ast))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "A dictionary is not an ordinary table of this server, so statements about it are not allowed for "
            "the read-only tool. Use the run_query tool for this query");

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

    checkNoQueryParameters(ast);
    checkNoProtectedSettingChanges(ast);
    checkNoExternalAccess(ast);
    if (!allow_schema_access)
    {
        /// Every other statement of the allowlist shows metadata: the schema (`SHOW TABLES`,
        /// `DESCRIBE`, `EXISTS`, `SHOW CREATE`), but also the running queries (`SHOW PROCESSLIST`)
        /// and the access entities (`SHOW GRANTS`, `SHOW CREATE USER`, `SHOW ACCESS`). Only the
        /// statements that read the tables of the user are kept, rather than listing the others.
        if (!isAnyOf<ASTSelectWithUnionQuery, ASTExplainQuery>(ast))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Schema access is disabled for the read-only tool. Use the run_query tool for this query");
        checkNoSchemaAccess(ast);
    }
}

}
