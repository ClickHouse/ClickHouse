#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropResourceQuery.h>

#include <Access/ContextAccess.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTDropResourceQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

BlockIO InterpreterDropResourceQuery::execute()
{
    ASTDropResourceQuery & drop_resource_query = query_ptr->as<ASTDropResourceQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_RESOURCE);

    auto current_context = getContext();

    if (!drop_resource_query.cluster.empty())
    {
        if (current_context->getWorkloadEntityStorage().isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because workload entities are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    bool throw_if_not_exists = !drop_resource_query.if_exists;

    current_context->getWorkloadEntityStorage().removeEntity(
        current_context,
        WorkloadEntityType::Resource,
        drop_resource_query.resource_name,
        throw_if_not_exists);

    return {};
}

void registerInterpreterDropResourceQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropResourceQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropResourceQuery", create_fn);
}

}
