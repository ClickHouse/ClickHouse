#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateWorkloadQuery.h>

#include <Access/ContextAccess.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTCreateWorkloadQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

BlockIO InterpreterCreateWorkloadQuery::execute()
{
    ASTCreateWorkloadQuery & create_workload_query = query_ptr->as<ASTCreateWorkloadQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_WORKLOAD);

    if (create_workload_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_WORKLOAD);

    auto current_context = getContext();
    /// Hold a shared_ptr to keep the storage alive for the duration of this call, in case of concurrent shutdown.
    auto workload_entity_storage = current_context->getWorkloadEntityStoragePtr();

    if (!create_workload_query.cluster.empty())
    {
        if (workload_entity_storage->isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because workload entities are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    auto workload_name = create_workload_query.getWorkloadName();
    bool throw_if_exists = !create_workload_query.if_not_exists && !create_workload_query.or_replace;
    bool replace_if_exists = create_workload_query.or_replace;

    workload_entity_storage->storeEntity(
        current_context,
        WorkloadEntityType::Workload,
        workload_name,
        query_ptr,
        throw_if_exists,
        replace_if_exists,
        current_context->getSettingsRef());

    return {};
}

void registerInterpreterCreateWorkloadQuery(InterpreterFactory & factory);
void registerInterpreterCreateWorkloadQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateWorkloadQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateWorkloadQuery", create_fn);
}

}
