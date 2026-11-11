#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateResourceQuery.h>

#include <Access/ContextAccess.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTCreateResourceQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

BlockIO InterpreterCreateResourceQuery::execute()
{
    ASTCreateResourceQuery & create_resource_query = query_ptr->as<ASTCreateResourceQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_RESOURCE);

    if (create_resource_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_RESOURCE);

    auto current_context = getContext();
    /// Hold a shared_ptr to keep the storage alive for the duration of this call, in case of concurrent shutdown.
    auto workload_entity_storage = current_context->getWorkloadEntityStoragePtr();

    if (!create_resource_query.cluster.empty())
    {
        if (workload_entity_storage->isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because workload entities are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    auto resource_name = create_resource_query.getResourceName();
    bool throw_if_exists = !create_resource_query.if_not_exists && !create_resource_query.or_replace;
    bool replace_if_exists = create_resource_query.or_replace;

    workload_entity_storage->storeEntity(
        current_context,
        WorkloadEntityType::Resource,
        resource_name,
        query_ptr,
        throw_if_exists,
        replace_if_exists,
        current_context->getSettingsRef());

    return {};
}

void registerInterpreterCreateResourceQuery(InterpreterFactory & factory);
void registerInterpreterCreateResourceQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateResourceQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateResourceQuery", create_fn);
}

}
