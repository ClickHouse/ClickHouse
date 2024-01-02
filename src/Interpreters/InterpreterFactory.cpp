#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTUndropQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTShowEngineQuery.h>
#include <Parsers/ASTShowFunctionsQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/ASTShowSettingQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTMoveAccessEntityQuery.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Parsers/Access/ASTShowAccessQuery.h>
#include <Parsers/Access/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTShowPrivilegesQuery.h>
#include <Parsers/ASTDescribeCacheQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterBackupQuery.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>
#include <Interpreters/InterpreterCreateIndexQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterCreateNamedCollectionQuery.h>
#include <Interpreters/InterpreterDropNamedCollectionQuery.h>
#include <Interpreters/InterpreterAlterNamedCollectionQuery.h>
#include <Interpreters/InterpreterDeleteQuery.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/InterpreterDescribeCacheQuery.h>
#include <Interpreters/InterpreterDropFunctionQuery.h>
#include <Interpreters/InterpreterDropIndexQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterUndropQuery.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Interpreters/InterpreterExplainQuery.h>
#include <Interpreters/InterpreterExternalDDLQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectIntersectExceptQuery.h>
#include <Interpreters/InterpreterKillQueryQuery.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Interpreters/InterpreterShowEngineQuery.h>
#include <Interpreters/InterpreterShowFunctionsQuery.h>
#include <Interpreters/InterpreterShowProcesslistQuery.h>
#include <Interpreters/InterpreterShowTablesQuery.h>
#include <Interpreters/InterpreterShowColumnsQuery.h>
#include <Interpreters/InterpreterShowIndexesQuery.h>
#include <Interpreters/InterpreterShowSettingQuery.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Interpreters/InterpreterWatchQuery.h>
#include <Interpreters/InterpreterTransactionControlQuery.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <Interpreters/Access/InterpreterCreateQuotaQuery.h>
#include <Interpreters/Access/InterpreterCreateRoleQuery.h>
#include <Interpreters/Access/InterpreterCreateRowPolicyQuery.h>
#include <Interpreters/Access/InterpreterCreateSettingsProfileQuery.h>
#include <Interpreters/Access/InterpreterCreateUserQuery.h>
#include <Interpreters/Access/InterpreterDropAccessEntityQuery.h>
#include <Interpreters/Access/InterpreterGrantQuery.h>
#include <Interpreters/Access/InterpreterMoveAccessEntityQuery.h>
#include <Interpreters/Access/InterpreterSetRoleQuery.h>
#include <Interpreters/Access/InterpreterShowAccessEntitiesQuery.h>
#include <Interpreters/Access/InterpreterShowAccessQuery.h>
#include <Interpreters/Access/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/Access/InterpreterShowGrantsQuery.h>
#include <Interpreters/Access/InterpreterShowPrivilegesQuery.h>

#include <Parsers/ASTSystemQuery.h>

#include <Databases/MySQL/MaterializedMySQLSyncThread.h>
#include <Parsers/ASTExternalDDLQuery.h>
#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>


namespace ProfileEvents
{
    extern const Event Query;
    extern const Event QueriesWithSubqueries;
    extern const Event SelectQuery;
    extern const Event InsertQuery;
}


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE_OF_QUERY;
}

InterpreterFactory & InterpreterFactory::instance()
{
    static InterpreterFactory interpreter_fact;
    return interpreter_fact;
}

void InterpreterFactory::registerInterpreter(const std::string & name, CreatorFn creator_fn)
{
    if(!interpreters.emplace(name, std::move(creator_fn)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "InterpreterFactory: the interpreter name '{}' is not unique", name);
}

InterpreterFactory::InterpreterPtr InterpreterFactory::get(ASTPtr & query, ContextMutablePtr context, const SelectQueryOptions & options)
{
    ProfileEvents::increment(ProfileEvents::Query);

    /// SELECT and INSERT query will handle QueriesWithSubqueries on their own.
    if (!(query->as<ASTSelectQuery>() ||
        query->as<ASTSelectWithUnionQuery>() ||
        query->as<ASTSelectIntersectExceptQuery>() ||
        query->as<ASTInsertQuery>()))
    {
        ProfileEvents::increment(ProfileEvents::QueriesWithSubqueries);
    }

    Arguments arguments {
        .query = query,
        .context = context,
        .options = options
    };
    String interpreter_name;
    if (query->as<ASTSelectQuery>())
    {
        if (context->getSettingsRef().allow_experimental_analyzer)
            interpreter_name = "InterpreterSelectQueryAnalyzer";
        /// This is internal part of ASTSelectWithUnionQuery.
        /// Even if there is SELECT without union, it is represented by ASTSelectWithUnionQuery with single ASTSelectQuery as a child.
        else interpreter_name = "InterpreterSelectQuery";
    }
    else if (query->as<ASTSelectWithUnionQuery>())
    {
        ProfileEvents::increment(ProfileEvents::SelectQuery);

        if (context->getSettingsRef().allow_experimental_analyzer)
            interpreter_name = "InterpreterSelectQueryAnalyzer";
        else interpreter_name = "InterpreterSelectWithUnionQuery";
    }
    else if (query->as<ASTSelectIntersectExceptQuery>())
    {
        interpreter_name = "InterpreterSelectIntersectExceptQuery";
    }
    else if (query->as<ASTInsertQuery>())
    {
        ProfileEvents::increment(ProfileEvents::InsertQuery);
        bool allow_materialized = static_cast<bool>(context->getSettingsRef().insert_allow_materialized_columns);
        arguments.allow_materialized = allow_materialized;
        interpreter_name = "InterpreterInsertQuery";
    }
    else if (query->as<ASTCreateQuery>())
    {
        interpreter_name = "InterpreterCreateQuery";
    }
    else if (query->as<ASTDropQuery>())
    {
        interpreter_name = "InterpreterDropQuery";
    }
    else if (query->as<ASTUndropQuery>())
    {
        interpreter_name = "InterpreterUndropQuery";
    }
    else if (query->as<ASTRenameQuery>())
    {
        interpreter_name = "InterpreterRenameQuery";
    }
    else if (query->as<ASTShowTablesQuery>())
    {
        interpreter_name = "InterpreterShowTablesQuery";
    }
    else if (query->as<ASTShowColumnsQuery>())
    {
        interpreter_name = "InterpreterShowColumnsQuery";
    }
    else if (query->as<ASTShowIndexesQuery>())
    {
        interpreter_name = "InterpreterShowIndexesQuery";
    }
    else if (query->as<ASTShowSettingQuery>())
    {
        interpreter_name = "InterpreterShowSettingQuery";
    }
    else if (query->as<ASTShowEnginesQuery>())
    {
        interpreter_name = "InterpreterShowEnginesQuery";
    }
    else if (query->as<ASTShowFunctionsQuery>())
    {
        interpreter_name = "InterpreterShowFunctionsQuery";
    }
    else if (query->as<ASTUseQuery>())
    {
        interpreter_name = "InterpreterUseQuery";
    }
    else if (query->as<ASTSetQuery>())
    {
        /// readonly is checked inside InterpreterSetQuery
        interpreter_name = "InterpreterSetQuery";
    }
    else if (query->as<ASTSetRoleQuery>())
    {
        interpreter_name = "InterpreterSetRoleQuery";
    }
    else if (query->as<ASTOptimizeQuery>())
    {
        interpreter_name = "InterpreterOptimizeQuery";
    }
    else if (query->as<ASTExistsDatabaseQuery>() || query->as<ASTExistsTableQuery>() || query->as<ASTExistsViewQuery>() || query->as<ASTExistsDictionaryQuery>())
    {
        interpreter_name = "InterpreterExistsQuery";
    }
    else if (query->as<ASTShowCreateTableQuery>() || query->as<ASTShowCreateViewQuery>() || query->as<ASTShowCreateDatabaseQuery>() || query->as<ASTShowCreateDictionaryQuery>())
    {
        interpreter_name = "InterpreterShowCreateQuery";
    }
    else if (query->as<ASTDescribeQuery>())
    {
        interpreter_name = "InterpreterDescribeQuery";
    }
    else if (query->as<ASTDescribeCacheQuery>())
    {
        interpreter_name = "InterpreterDescribeCacheQuery";
    }
    else if (query->as<ASTExplainQuery>())
    {
        interpreter_name = "InterpreterExplainQuery";
    }
    else if (query->as<ASTShowProcesslistQuery>())
    {
        interpreter_name = "InterpreterShowProcesslistQuery";
    }
    else if (query->as<ASTAlterQuery>())
    {
        interpreter_name = "InterpreterAlterQuery";
    }
    else if (query->as<ASTAlterNamedCollectionQuery>())
    {
        interpreter_name = "InterpreterAlterNamedCollectionQuery";
    }
    else if (query->as<ASTCheckTableQuery>() || query->as<ASTCheckAllTablesQuery>())
    {
        interpreter_name = "InterpreterCheckQuery";
    }
    else if (query->as<ASTKillQueryQuery>())
    {
        interpreter_name = "InterpreterKillQueryQuery";
    }
    else if (query->as<ASTSystemQuery>())
    {
        interpreter_name = "InterpreterSystemQuery";
    }
    else if (query->as<ASTWatchQuery>())
    {
        interpreter_name = "InterpreterWatchQuery";
    }
    else if (query->as<ASTCreateUserQuery>())
    {
        interpreter_name = "InterpreterCreateUserQuery";
    }
    else if (query->as<ASTCreateRoleQuery>())
    {
        interpreter_name = "InterpreterCreateRoleQuery";
    }
    else if (query->as<ASTCreateQuotaQuery>())
    {
        interpreter_name = "InterpreterCreateQuotaQuery";
    }
    else if (query->as<ASTCreateRowPolicyQuery>())
    {
        interpreter_name = "InterpreterCreateRowPolicyQuery";
    }
    else if (query->as<ASTCreateSettingsProfileQuery>())
    {
        interpreter_name = "InterpreterCreateSettingsProfileQuery";
    }
    else if (query->as<ASTDropAccessEntityQuery>())
    {
        interpreter_name = "InterpreterDropAccessEntityQuery";
    }
    else if (query->as<ASTMoveAccessEntityQuery>())
    {
        interpreter_name = "InterpreterMoveAccessEntityQuery";
    }
    else if (query->as<ASTDropNamedCollectionQuery>())
    {
        interpreter_name = "InterpreterDropNamedCollectionQuery";
    }
    else if (query->as<ASTGrantQuery>())
    {
        interpreter_name = "InterpreterGrantQuery";
    }
    else if (query->as<ASTShowCreateAccessEntityQuery>())
    {
        interpreter_name = "InterpreterShowCreateAccessEntityQuery";
    }
    else if (query->as<ASTShowGrantsQuery>())
    {
        interpreter_name = "InterpreterShowGrantsQuery";
    }
    else if (query->as<ASTShowAccessEntitiesQuery>())
    {
        interpreter_name = "InterpreterShowAccessEntitiesQuery";
    }
    else if (query->as<ASTShowAccessQuery>())
    {
        interpreter_name= "InterpreterShowAccessQuery";
    }
    else if (query->as<ASTShowPrivilegesQuery>())
    {
        interpreter_name = "InterpreterShowPrivilegesQuery";
    }
    else if (query->as<ASTExternalDDLQuery>())
    {
        interpreter_name = "InterpreterExternalDDLQuery";
    }
    else if (query->as<ASTTransactionControl>())
    {
        interpreter_name = "InterpreterTransactionControlQuery";
    }
    else if (query->as<ASTCreateFunctionQuery>())
    {
        interpreter_name = "InterpreterCreateFunctionQuery";
    }
    else if (query->as<ASTDropFunctionQuery>())
    {
        interpreter_name = "InterpreterDropFunctionQuery";
    }
    else if (query->as<ASTCreateIndexQuery>())
    {
        interpreter_name = "InterpreterCreateIndexQuery";
    }
    else if (query->as<ASTCreateNamedCollectionQuery>())
    {
        interpreter_name = "InterpreterCreateNamedCollectionQuery";
    }
    else if (query->as<ASTDropIndexQuery>())
    {
        interpreter_name = "InterpreterDropIndexQuery";
    }
    else if (query->as<ASTBackupQuery>())
    {
        interpreter_name = "InterpreterBackupQuery";
    }
    else if (query->as<ASTDeleteQuery>())
    {
        interpreter_name = "InterpreterDeleteQuery";
    }

    if(!interpreters.contains(interpreter_name))
        throw Exception(ErrorCodes::UNKNOWN_TYPE_OF_QUERY, "Unknown type of query: {}", query->getID());

    // creator_fn creates and returns a InterpreterPtr with the supplied arguments
    auto creator_fn = interpreters.at(interpreter_name);

    return creator_fn(arguments);
}
}
