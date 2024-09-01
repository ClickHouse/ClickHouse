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

    if (!create_resource_query.cluster.empty())
    {
        if (current_context->getWorkloadEntityStorage().isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because workload entities are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    auto resource_name = create_resource_query.getResourceName();
    //bool throw_if_exists = !create_resource_query.if_not_exists && !create_resource_query.or_replace;
    //bool replace_if_exists = create_resource_query.or_replace;

    // TODO(serxa): validate and register entity

    return {};
}

void registerInterpreterCreateResourceQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateResourceQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateResourceQuery", create_fn);
}

}
