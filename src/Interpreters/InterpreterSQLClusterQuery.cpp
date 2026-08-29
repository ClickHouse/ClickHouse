#include <Interpreters/InterpreterSQLClusterQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Access/ContextAccess.h>
#include <Common/SQLClusters/SQLClusterFactory.h>
#include <Parsers/ASTSQLClusterQuery.h>


namespace DB
{

BlockIO InterpreterCreateSQLClusterQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateSQLClusterQuery &>();
    getContext()->checkAccess(AccessType::CREATE_SQL_CLUSTER);
    SQLClusterFactory::instance().createFromSQL(query);
    return {};
}

BlockIO InterpreterAlterSQLClusterQuery::execute()
{
    const auto & query = query_ptr->as<const ASTAlterSQLClusterQuery &>();
    getContext()->checkAccess(AccessType::ALTER_SQL_CLUSTER);
    SQLClusterFactory::instance().alterFromSQL(query);
    return {};
}

BlockIO InterpreterDropSQLClusterQuery::execute()
{
    const auto & query = query_ptr->as<const ASTDropSQLClusterQuery &>();
    getContext()->checkAccess(AccessType::DROP_SQL_CLUSTER);
    SQLClusterFactory::instance().dropFromSQL(query);
    return {};
}

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
