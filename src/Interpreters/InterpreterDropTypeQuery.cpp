#include <Interpreters/InterpreterDropTypeQuery.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTDropTypeQuery.h>

namespace DB
{

BlockIO InterpreterDropTypeQuery::execute()
{
    const auto & drop_query = query_ptr->as<const ASTDropTypeQuery &>();

    auto current_context = getContext();
    current_context->checkAccess(AccessType::DROP_TYPE);

    bool throw_if_not_exists = !drop_query.if_exists;
    UserDefinedTypeFactory::instance().unregisterType(current_context, drop_query.type_name, throw_if_not_exists);

    return {};
}

void registerInterpreterDropTypeQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropTypeQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropTypeQuery", create_fn);
}

}
