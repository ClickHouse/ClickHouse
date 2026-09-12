#include <Interpreters/InterpreterSQLClusterQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Common/SQLClusters/SQLClusterFactory.h>
#include <Parsers/ASTSQLClusterQuery.h>


namespace DB
{

BlockIO InterpreterCreateSQLClusterQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTCreateSQLClusterQuery &>();

    current_context->checkAccess(AccessType::CREATE_SQL_CLUSTER);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    SQLClusterFactory::instance().createFromSQL(query);
    return {};
}

BlockIO InterpreterAlterSQLClusterQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTAlterSQLClusterQuery &>();

    current_context->checkAccess(AccessType::ALTER_SQL_CLUSTER);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    SQLClusterFactory::instance().alterFromSQL(query);
    return {};
}

BlockIO InterpreterDropSQLClusterQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTDropSQLClusterQuery &>();

    current_context->checkAccess(AccessType::DROP_SQL_CLUSTER);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    SQLClusterFactory::instance().dropFromSQL(query);
    return {};
}

void registerInterpreterCreateSQLClusterQuery(InterpreterFactory & factory);
void registerInterpreterAlterSQLClusterQuery(InterpreterFactory & factory);
void registerInterpreterDropSQLClusterQuery(InterpreterFactory & factory);

void registerInterpreterCreateSQLClusterQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterCreateSQLClusterQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterCreateSQLClusterQuery>(args.query, args.context); });
}

void registerInterpreterAlterSQLClusterQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterAlterSQLClusterQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterAlterSQLClusterQuery>(args.query, args.context); });
}

void registerInterpreterDropSQLClusterQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterDropSQLClusterQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterDropSQLClusterQuery>(args.query, args.context); });
}

}
