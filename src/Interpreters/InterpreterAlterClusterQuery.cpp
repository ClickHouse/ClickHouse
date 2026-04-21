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

using namespace SQLClusterCatalog;

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
                PropertyValidation::Cluster::validate(query.cluster_definition_properties);
            break;
        case AlterClusterCommand::ModifyShard:
        case AlterClusterCommand::RenameShard:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER CLUSTER: this variant is not implemented yet");
    }

    /// Same member contract as `CREATE CLUSTER`: new whole-shard members resolve via named collections.
    /// `hasShard` uses an unlocked snapshot, so member classification here is best-effort — see the matching
    /// comment in `InterpreterCreateClusterCatalogQuery::execute`. The factory re-validates existence and name
    /// ambiguity under its lock; this pre-check only rejects unauthorised NC references early.
    switch (query.command)
    {
        case AlterClusterCommand::AddShard:
            for (const auto & member : query.add_shard_members)
            {
                if (!ClusterFactory::instance().hasShard(member))
                    current_context->checkAccess(AccessType::NAMED_COLLECTION, member);
            }
            break;
        case AlterClusterCommand::ReplaceClusterMembers:
            for (const auto & clause : query.member_replace_clauses)
            {
                for (const auto & to_name : clause.to_members)
                {
                    if (!ClusterFactory::instance().hasShard(to_name))
                        current_context->checkAccess(AccessType::NAMED_COLLECTION, to_name);
                }
            }
            break;
        default:
            break;
    }

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    switch (query.command)
    {
        case AlterClusterCommand::AddShard:
            ClusterFactory::instance().addClusterMembersFromSQL(query);
            break;
        case AlterClusterCommand::DropShard:
            ClusterFactory::instance().dropClusterMembersFromSQL(query);
            break;
        case AlterClusterCommand::ReplaceClusterMembers:
            ClusterFactory::instance().replaceClusterMembersFromSQL(query);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ALTER CLUSTER: unsupported command after validation");
    }

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
