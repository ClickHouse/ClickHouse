#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>

#include <boost/noncopyable.hpp>

namespace DB
{

class Context;

enum class InterpreterOperation : uint8_t
{
    None,
    SelectQueryAnalyzer,
    SelectQuery,
    SelectWithUnionQuery,
    SelectIntersectExceptQuery,
    InsertQuery,
    CreateQuery,
    DropQuery,
    UndropQuery,
    RenameQuery,
    ShowTablesQuery,
    ShowColumnsQuery,
    ShowIndexesQuery,
    ShowSettingQuery,
    ShowEnginesQuery,
    ShowFunctionsQuery,
    UseQuery,
    SetQuery,
    SetRoleQuery,
    OptimizeQuery,
    ExistsQuery,
    ShowCreateQuery,
    DescribeQuery,
    DescribeCacheQuery,
    ExplainQuery,
    ShowProcesslistQuery,
    AlterQuery,
    AlterNamedCollectionQuery,
    CheckQuery,
    KillQueryQuery,
    SystemQuery,
    WatchQuery,
    CreateUserQuery,
    CreateRoleQuery,
    CreateQuotaQuery,
    CreateRowPolicyQuery,
    CreateSettingsProfileQuery,
    DropAccessEntityQuery,
    MoveAccessEntityQuery,
    DropNamedCollectionQuery,
    GrantQuery,
    ShowCreateAccessEntityQuery,
    ShowGrantsQuery,
    CheckGrantQuery,
    ShowAccessEntitiesQuery,
    ShowAccessQuery,
    ShowPrivilegesQuery,
    TransactionControlQuery,
    CreateFunctionQuery,
    DropFunctionQuery,
    CreateWorkloadQuery,
    DropWorkloadQuery,
    CreateResourceQuery,
    DropResourceQuery,
    CreateIndexQuery,
    CreateNamedCollectionQuery,
    DropIndexQuery,
    BackupQuery,
    DeleteQuery,
    UpdateQuery,
    ParallelWithQuery,
    ExecuteAsQuery
};

String toString(InterpreterOperation op);

class InterpreterFactory : private boost::noncopyable
{
public:
    static InterpreterFactory & instance();

    struct Arguments
    {
        ASTPtr & query;
        ContextMutablePtr context;
        const SelectQueryOptions & options;
        bool allow_materialized = false;
    };

    using InterpreterPtr = std::unique_ptr<IInterpreter>;

     InterpreterPtr get(
        ASTPtr & query,
        ContextMutablePtr context,
        const SelectQueryOptions & options = {});

    using CreatorFn = std::function<InterpreterPtr(const Arguments & arguments)>;

    using Interpreters = std::unordered_map<InterpreterOperation, CreatorFn>;

    void registerInterpreter(InterpreterOperation type, CreatorFn creator_fn);

private:
    Interpreters interpreters;
};

}
