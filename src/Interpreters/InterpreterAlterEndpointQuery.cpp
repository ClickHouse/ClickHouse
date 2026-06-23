#include <Interpreters/InterpreterAlterEndpointQuery.h>

#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterMetadataManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTAlterEndpointQuery.h>


namespace DB
{

BlockIO InterpreterAlterEndpointQuery::execute()
{
    auto current_context = getContext();
    const auto & query = query_ptr->as<const ASTAlterEndpointQuery &>();

    current_context->checkAccess(AccessType::ALTER_ENDPOINT);

    ClusterMetadataManager::instance().alterEndpoint(query.endpoint_name, query.properties);
    return {};
}

void registerInterpreterAlterEndpointQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterAlterEndpointQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterAlterEndpointQuery>(args.query, args.context); });
}

}
