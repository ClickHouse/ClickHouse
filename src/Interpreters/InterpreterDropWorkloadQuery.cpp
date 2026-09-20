#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropWorkloadQuery.h>

#include <Access/ContextAccess.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTDropWorkloadQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

BlockIO InterpreterDropWorkloadQuery::execute()
{
    ASTDropWorkloadQuery & drop_workload_query = query_ptr->as<ASTDropWorkloadQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_WORKLOAD);

    auto current_context = getContext();
    /// Hold a shared_ptr to keep the storage alive for the duration of this call, in case of concurrent shutdown.
    auto workload_entity_storage = current_context->getWorkloadEntityStoragePtr();

    if (!drop_workload_query.cluster.empty())
    {
        if (workload_entity_storage->isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because workload entities are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    bool throw_if_not_exists = !drop_workload_query.if_exists;

    workload_entity_storage->removeEntity(
        current_context,
        WorkloadEntityType::Workload,
        drop_workload_query.workload_name,
        throw_if_not_exists);

    return {};
}

void registerInterpreterDropWorkloadQuery(InterpreterFactory & factory);
void registerInterpreterDropWorkloadQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropWorkloadQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropWorkloadQuery", create_fn);
}

}
