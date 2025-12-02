#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTDropResourceQuery.h>
#include <Parsers/ASTDropWorkloadQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTParallelWithQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowEngineQuery.h>
#include <Parsers/ASTShowFunctionsQuery.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTShowSettingQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/ASTUndropQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/ASTDescribeCacheQuery.h>
#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTMoveAccessEntityQuery.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Parsers/Access/ASTShowAccessQuery.h>
#include <Parsers/Access/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTShowPrivilegesQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterWatchQuery.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <Core/Settings.h>
#include <Parsers/ASTSystemQuery.h>
#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>


namespace ProfileEvents
{
extern const Event Query;
extern const Event InitialQuery;
extern const Event QueriesWithSubqueries;
extern const Event SelectQuery;
extern const Event InsertQuery;
}


namespace DB
{
namespace Setting
{
extern const SettingsBool allow_experimental_analyzer;
extern const SettingsBool insert_allow_materialized_columns;
}

namespace ErrorCodes
{
extern const int UNKNOWN_TYPE_OF_QUERY;
extern const int LOGICAL_ERROR;
}

String toString(InterpreterOperation op)
{
    switch (op)
    {
        case InterpreterOperation::None:
            return "None";
        case InterpreterOperation::SelectQueryAnalyzer:
            return "SelectQueryAnalyzer";
        case InterpreterOperation::SelectQuery:
            return "SelectQuery";
        case InterpreterOperation::SelectWithUnionQuery:
            return "SelectWithUnionQuery";
        case InterpreterOperation::SelectIntersectExceptQuery:
            return "SelectIntersectExceptQuery";
        case InterpreterOperation::InsertQuery:
            return "InsertQuery";
        case InterpreterOperation::CreateQuery:
            return "CreateQuery";
        case InterpreterOperation::DropQuery:
            return "DropQuery";
        case InterpreterOperation::UndropQuery:
            return "UndropQuery";
        case InterpreterOperation::RenameQuery:
            return "RenameQuery";
        case InterpreterOperation::ShowTablesQuery:
            return "ShowTablesQuery";
        case InterpreterOperation::ShowColumnsQuery:
            return "ShowColumnsQuery";
        case InterpreterOperation::ShowIndexesQuery:
            return "ShowIndexesQuery";
        case InterpreterOperation::ShowSettingQuery:
            return "ShowSettingQuery";
        case InterpreterOperation::ShowEnginesQuery:
            return "ShowEnginesQuery";
        case InterpreterOperation::ShowFunctionsQuery:
            return "ShowFunctionsQuery";
        case InterpreterOperation::UseQuery:
            return "UseQuery";
        case InterpreterOperation::SetQuery:
            return "SetQuery";
        case InterpreterOperation::SetRoleQuery:
            return "SetRoleQuery";
        case InterpreterOperation::OptimizeQuery:
            return "OptimizeQuery";
        case InterpreterOperation::ExistsQuery:
            return "ExistsQuery";
        case InterpreterOperation::ShowCreateQuery:
            return "ShowCreateQuery";
        case InterpreterOperation::DescribeQuery:
            return "DescribeQuery";
        case InterpreterOperation::DescribeCacheQuery:
            return "DescribeCacheQuery";
        case InterpreterOperation::ExplainQuery:
            return "ExplainQuery";
        case InterpreterOperation::ShowProcesslistQuery:
            return "ShowProcesslistQuery";
        case InterpreterOperation::AlterQuery:
            return "AlterQuery";
        case InterpreterOperation::AlterNamedCollectionQuery:
            return "AlterNamedCollectionQuery";
        case InterpreterOperation::CheckQuery:
            return "CheckQuery";
        case InterpreterOperation::KillQueryQuery:
            return "KillQueryQuery";
        case InterpreterOperation::SystemQuery:
            return "SystemQuery";
        case InterpreterOperation::WatchQuery:
            return "WatchQuery";
        case InterpreterOperation::CreateUserQuery:
            return "CreateUserQuery";
        case InterpreterOperation::CreateRoleQuery:
            return "CreateRoleQuery";
        case InterpreterOperation::CreateQuotaQuery:
            return "CreateQuotaQuery";
        case InterpreterOperation::CreateRowPolicyQuery:
            return "CreateRowPolicyQuery";
        case InterpreterOperation::CreateSettingsProfileQuery:
            return "CreateSettingsProfileQuery";
        case InterpreterOperation::DropAccessEntityQuery:
            return "DropAccessEntityQuery";
        case InterpreterOperation::MoveAccessEntityQuery:
            return "MoveAccessEntityQuery";
        case InterpreterOperation::DropNamedCollectionQuery:
            return "DropNamedCollectionQuery";
        case InterpreterOperation::GrantQuery:
            return "GrantQuery";
        case InterpreterOperation::ShowCreateAccessEntityQuery:
            return "ShowCreateAccessEntityQuery";
        case InterpreterOperation::ShowGrantsQuery:
            return "ShowGrantsQuery";
        case InterpreterOperation::CheckGrantQuery:
            return "CheckGrantQuery";
        case InterpreterOperation::ShowAccessEntitiesQuery:
            return "ShowAccessEntitiesQuery";
        case InterpreterOperation::ShowAccessQuery:
            return "ShowAccessQuery";
        case InterpreterOperation::ShowPrivilegesQuery:
            return "ShowPrivilegesQuery";
        case InterpreterOperation::TransactionControlQuery:
            return "TransactionControlQuery";
        case InterpreterOperation::CreateFunctionQuery:
            return "CreateFunctionQuery";
        case InterpreterOperation::DropFunctionQuery:
            return "DropFunctionQuery";
        case InterpreterOperation::CreateWorkloadQuery:
            return "CreateWorkloadQuery";
        case InterpreterOperation::DropWorkloadQuery:
            return "DropWorkloadQuery";
        case InterpreterOperation::CreateResourceQuery:
            return "CreateResourceQuery";
        case InterpreterOperation::DropResourceQuery:
            return "DropResourceQuery";
        case InterpreterOperation::CreateIndexQuery:
            return "CreateIndexQuery";
        case InterpreterOperation::CreateNamedCollectionQuery:
            return "CreateNamedCollectionQuery";
        case InterpreterOperation::DropIndexQuery:
            return "DropIndexQuery";
        case InterpreterOperation::BackupQuery:
            return "BackupQuery";
        case InterpreterOperation::DeleteQuery:
            return "DeleteQuery";
        case InterpreterOperation::UpdateQuery:
            return "UpdateQuery";
        case InterpreterOperation::ParallelWithQuery:
            return "ParallelWithQuery";
        case InterpreterOperation::ExecuteAsQuery:
            return "ExecuteAsQuery";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown InterpreterOperation: {}", static_cast<uint32_t>(op));
}

InterpreterFactory & InterpreterFactory::instance()
{
    static InterpreterFactory interpreter_fact;
    return interpreter_fact;
}

void InterpreterFactory::registerInterpreter(InterpreterOperation type, CreatorFn creator_fn)
{
    if (!interpreters.emplace(type, std::move(creator_fn)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "InterpreterFactory: the interpreter for oparation '{}' is not unique", toString(type));
}

InterpreterFactory::InterpreterPtr InterpreterFactory::get(ASTPtr & query, ContextMutablePtr context, const SelectQueryOptions & options)
{
    ProfileEvents::increment(ProfileEvents::Query);
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        ProfileEvents::increment(ProfileEvents::InitialQuery);
    /// SELECT and INSERT query will handle QueriesWithSubqueries on their own.
    if (!(query->as<ASTSelectQuery>() || query->as<ASTSelectWithUnionQuery>() || query->as<ASTSelectIntersectExceptQuery>()
          || query->as<ASTInsertQuery>()))
    {
        ProfileEvents::increment(ProfileEvents::QueriesWithSubqueries);
    }

    Arguments arguments{.query = query, .context = context, .options = options};

    InterpreterOperation interpreter_type = InterpreterOperation::None;

    if (query->as<ASTSelectQuery>())
    {
        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
            interpreter_type = InterpreterOperation::SelectQueryAnalyzer;
        /// This is internal part of ASTSelectWithUnionQuery.
        /// Even if there is SELECT without union, it is represented by ASTSelectWithUnionQuery with single ASTSelectQuery as a child.
        else
            interpreter_type = InterpreterOperation::SelectQuery;
    }
    else if (query->as<ASTSelectWithUnionQuery>())
    {
        ProfileEvents::increment(ProfileEvents::SelectQuery);

        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
            interpreter_type = InterpreterOperation::SelectQueryAnalyzer;
        else
            interpreter_type = InterpreterOperation::SelectWithUnionQuery;
    }
    else if (query->as<ASTSelectIntersectExceptQuery>())
    {
        interpreter_type = InterpreterOperation::SelectIntersectExceptQuery;
    }
    else if (query->as<ASTInsertQuery>())
    {
        ProfileEvents::increment(ProfileEvents::InsertQuery);
        bool allow_materialized = static_cast<bool>(context->getSettingsRef()[Setting::insert_allow_materialized_columns]);
        arguments.allow_materialized = allow_materialized;
        interpreter_type = InterpreterOperation::InsertQuery;
    }
    else if (query->as<ASTCreateQuery>())
    {
        interpreter_type = InterpreterOperation::CreateQuery;
    }
    else if (query->as<ASTDropQuery>())
    {
        interpreter_type = InterpreterOperation::DropQuery;
    }
    else if (query->as<ASTUndropQuery>())
    {
        interpreter_type = InterpreterOperation::UndropQuery;
    }
    else if (query->as<ASTRenameQuery>())
    {
        interpreter_type = InterpreterOperation::RenameQuery;
    }
    else if (query->as<ASTShowTablesQuery>())
    {
        interpreter_type = InterpreterOperation::ShowTablesQuery;
    }
    else if (query->as<ASTShowColumnsQuery>())
    {
        interpreter_type = InterpreterOperation::ShowColumnsQuery;
    }
    else if (query->as<ASTShowIndexesQuery>())
    {
        interpreter_type = InterpreterOperation::ShowIndexesQuery;
    }
    else if (query->as<ASTShowSettingQuery>())
    {
        interpreter_type = InterpreterOperation::ShowSettingQuery;
    }
    else if (query->as<ASTShowEnginesQuery>())
    {
        interpreter_type = InterpreterOperation::ShowEnginesQuery;
    }
    else if (query->as<ASTShowFunctionsQuery>())
    {
        interpreter_type = InterpreterOperation::ShowFunctionsQuery;
    }
    else if (query->as<ASTUseQuery>())
    {
        interpreter_type = InterpreterOperation::UseQuery;
    }
    else if (query->as<ASTSetQuery>())
    {
        /// readonly is checked inside InterpreterSetQuery
        interpreter_type = InterpreterOperation::SetQuery;
    }
    else if (query->as<ASTSetRoleQuery>())
    {
        interpreter_type = InterpreterOperation::SetRoleQuery;
    }
    else if (query->as<ASTOptimizeQuery>())
    {
        interpreter_type = InterpreterOperation::OptimizeQuery;
    }
    else if (
        query->as<ASTExistsDatabaseQuery>() || query->as<ASTExistsTableQuery>() || query->as<ASTExistsViewQuery>()
        || query->as<ASTExistsDictionaryQuery>())
    {
        interpreter_type = InterpreterOperation::ExistsQuery;
    }
    else if (
        query->as<ASTShowCreateTableQuery>() || query->as<ASTShowCreateViewQuery>() || query->as<ASTShowCreateDatabaseQuery>()
        || query->as<ASTShowCreateDictionaryQuery>())
    {
        interpreter_type = InterpreterOperation::ShowCreateQuery;
    }
    else if (query->as<ASTDescribeQuery>())
    {
        interpreter_type = InterpreterOperation::DescribeQuery;
    }
    else if (query->as<ASTDescribeCacheQuery>())
    {
        interpreter_type = InterpreterOperation::DescribeCacheQuery;
    }
    else if (query->as<ASTExplainQuery>())
    {
        const auto kind = query->as<ASTExplainQuery>()->getKind();
        if (kind == ASTExplainQuery::ParsedAST)
            context->setSetting("allow_experimental_analyzer", false);

        interpreter_type = InterpreterOperation::ExplainQuery;
    }
    else if (query->as<ASTShowProcesslistQuery>())
    {
        interpreter_type = InterpreterOperation::ShowProcesslistQuery;
    }
    else if (query->as<ASTAlterQuery>())
    {
        interpreter_type = InterpreterOperation::AlterQuery;
    }
    else if (query->as<ASTAlterNamedCollectionQuery>())
    {
        interpreter_type = InterpreterOperation::AlterNamedCollectionQuery;
    }
    else if (query->as<ASTCheckTableQuery>() || query->as<ASTCheckAllTablesQuery>())
    {
        interpreter_type = InterpreterOperation::CheckQuery;
    }
    else if (query->as<ASTKillQueryQuery>())
    {
        interpreter_type = InterpreterOperation::KillQueryQuery;
    }
    else if (query->as<ASTSystemQuery>())
    {
        interpreter_type = InterpreterOperation::SystemQuery;
    }
    else if (query->as<ASTWatchQuery>())
    {
        interpreter_type = InterpreterOperation::WatchQuery;
    }
    else if (query->as<ASTCreateUserQuery>())
    {
        interpreter_type = InterpreterOperation::CreateUserQuery;
    }
    else if (query->as<ASTCreateRoleQuery>())
    {
        interpreter_type = InterpreterOperation::CreateRoleQuery;
    }
    else if (query->as<ASTCreateQuotaQuery>())
    {
        interpreter_type = InterpreterOperation::CreateQuotaQuery;
    }
    else if (query->as<ASTCreateRowPolicyQuery>())
    {
        interpreter_type = InterpreterOperation::CreateRowPolicyQuery;
    }
    else if (query->as<ASTCreateSettingsProfileQuery>())
    {
        interpreter_type = InterpreterOperation::CreateSettingsProfileQuery;
    }
    else if (query->as<ASTDropAccessEntityQuery>())
    {
        interpreter_type = InterpreterOperation::DropAccessEntityQuery;
    }
    else if (query->as<ASTMoveAccessEntityQuery>())
    {
        interpreter_type = InterpreterOperation::MoveAccessEntityQuery;
    }
    else if (query->as<ASTDropNamedCollectionQuery>())
    {
        interpreter_type = InterpreterOperation::DropNamedCollectionQuery;
    }
    else if (query->as<ASTGrantQuery>())
    {
        interpreter_type = InterpreterOperation::GrantQuery;
    }
    else if (query->as<ASTShowCreateAccessEntityQuery>())
    {
        interpreter_type = InterpreterOperation::ShowCreateAccessEntityQuery;
    }
    else if (query->as<ASTShowGrantsQuery>())
    {
        interpreter_type = InterpreterOperation::ShowGrantsQuery;
    }
    else if (query->as<ASTCheckGrantQuery>())
    {
        interpreter_type = InterpreterOperation::CheckGrantQuery;
    }
    else if (query->as<ASTShowAccessEntitiesQuery>())
    {
        interpreter_type = InterpreterOperation::ShowAccessEntitiesQuery;
    }
    else if (query->as<ASTShowAccessQuery>())
    {
        interpreter_type = InterpreterOperation::ShowAccessQuery;
    }
    else if (query->as<ASTShowPrivilegesQuery>())
    {
        interpreter_type = InterpreterOperation::ShowPrivilegesQuery;
    }
    else if (query->as<ASTTransactionControl>())
    {
        interpreter_type = InterpreterOperation::TransactionControlQuery;
    }
    else if (query->as<ASTCreateFunctionQuery>())
    {
        interpreter_type = InterpreterOperation::CreateFunctionQuery;
    }
    else if (query->as<ASTDropFunctionQuery>())
    {
        interpreter_type = InterpreterOperation::DropFunctionQuery;
    }
    else if (query->as<ASTCreateWorkloadQuery>())
    {
        interpreter_type = InterpreterOperation::CreateWorkloadQuery;
    }
    else if (query->as<ASTDropWorkloadQuery>())
    {
        interpreter_type = InterpreterOperation::DropWorkloadQuery;
    }
    else if (query->as<ASTCreateResourceQuery>())
    {
        interpreter_type = InterpreterOperation::CreateResourceQuery;
    }
    else if (query->as<ASTDropResourceQuery>())
    {
        interpreter_type = InterpreterOperation::DropResourceQuery;
    }
    else if (query->as<ASTCreateIndexQuery>())
    {
        interpreter_type = InterpreterOperation::CreateIndexQuery;
    }
    else if (query->as<ASTCreateNamedCollectionQuery>())
    {
        interpreter_type = InterpreterOperation::CreateNamedCollectionQuery;
    }
    else if (query->as<ASTDropIndexQuery>())
    {
        interpreter_type = InterpreterOperation::DropIndexQuery;
    }
    else if (query->as<ASTBackupQuery>())
    {
        interpreter_type = InterpreterOperation::BackupQuery;
    }
    else if (query->as<ASTDeleteQuery>())
    {
        interpreter_type = InterpreterOperation::DeleteQuery;
    }
    else if (query->as<ASTUpdateQuery>())
    {
        interpreter_type = InterpreterOperation::UpdateQuery;
    }
    else if (query->as<ASTParallelWithQuery>())
    {
        interpreter_type = InterpreterOperation::ParallelWithQuery;
    }
    else if (query->as<ASTExecuteAsQuery>())
    {
        interpreter_type = InterpreterOperation::ExecuteAsQuery;
    }

    if (!interpreters.contains(interpreter_type))
        throw Exception(ErrorCodes::UNKNOWN_TYPE_OF_QUERY, "Unknown type of query: {}", query->getID());

    context->addQueryAccessInfo(interpreter_type);

    // creator_fn creates and returns a InterpreterPtr with the supplied arguments
    auto creator_fn = interpreters.at(interpreter_type);

    return creator_fn(arguments);
}
}
