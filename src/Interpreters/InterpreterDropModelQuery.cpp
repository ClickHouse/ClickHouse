#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropModelQuery.h>
#include <Parsers/ASTDropModelQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>

#include <Models/ModelRegistry.h>

namespace DB
{

BlockIO InterpreterDropModelQuery::execute()
{
    auto current_context = getContext();

    const auto & drop_model_query = query_ptr->as<const ASTDropModelQuery &>();

    const String model_name = drop_model_query.model_name->as<ASTIdentifier>()->name();

    if (!drop_model_query.if_exists || ModelRegistry::instance().hasModel(model_name))
        ModelRegistry::instance().unregisterModel(model_name);

    return {};
}

void registerInterpreterDropModelQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropModelQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropModelQuery", create_fn);
}

}
