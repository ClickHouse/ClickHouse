#include <Interpreters/InterpreterDropEndpointQuery.h>

#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterMetadataManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTDropEndpointQuery.h>


namespace DB
{

BlockIO InterpreterDropEndpointQuery::execute()
{
    auto current_context = getContext();
    const auto & query = query_ptr->as<const ASTDropEndpointQuery &>();

    current_context->checkAccess(AccessType::DROP_ENDPOINT);

    ClusterMetadataManager::instance().dropEndpoint(query.endpoint_name, query.if_exists);
    return {};
}

void registerInterpreterDropEndpointQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterDropEndpointQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterDropEndpointQuery>(args.query, args.context); });
}

}
