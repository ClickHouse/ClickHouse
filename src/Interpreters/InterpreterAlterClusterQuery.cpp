#include <Interpreters/InterpreterAlterClusterQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAlterClusterQuery.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

BlockIO InterpreterAlterClusterQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTAlterClusterQuery &>();

    current_context->checkAccess(AccessType::CREATE_CLUSTER);

    switch (query.command)
    {
        case AlterClusterCommand::AddShard:
            break;
        case AlterClusterCommand::DropShard:
            break;
        case AlterClusterCommand::ReplaceClusterMembers:
            if (!query.cluster_definition_properties.empty())
                validateClusterLevelPropertyPatchAssignments(query.cluster_definition_properties);
            break;
        case AlterClusterCommand::ModifyShard:
        case AlterClusterCommand::RenameShard:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER CLUSTER: this variant is not implemented yet");
    }

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    auto global_context = current_context->getGlobalContext();

    bool altered = false;
    switch (query.command)
    {
        case AlterClusterCommand::AddShard:
            altered = ClusterFactory::instance().addClusterMembersFromSQL(query);
            break;
        case AlterClusterCommand::DropShard:
            altered = ClusterFactory::instance().dropClusterMembersFromSQL(query);
            break;
        case AlterClusterCommand::ReplaceClusterMembers:
            altered = ClusterFactory::instance().replaceClusterMembersFromSQL(query);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ALTER CLUSTER: unsupported command after validation");
    }

    if (!altered)
        return {};

    auto cluster = ClusterFactory::instance().tryMaterializeCluster(query.cluster_name, global_context);
    if (!cluster)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to materialize SQL CLUSTER `{}` after alter", query.cluster_name);

    global_context->setCluster(query.cluster_name, cluster);
    return {};
}

void registerInterpreterAlterClusterQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterAlterClusterQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterAlterClusterQuery>(args.query, args.context); });
}

}
