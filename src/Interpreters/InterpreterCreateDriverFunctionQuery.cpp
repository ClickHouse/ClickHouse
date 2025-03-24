#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateDriverFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateDriverFunctionQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

BlockIO InterpreterCreateDriverFunctionQuery::execute()
{
    ASTCreateDriverFunctionQuery & create_driver_function = query_ptr->as<ASTCreateDriverFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FUNCTION);

    if (create_driver_function.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    auto function_name = create_driver_function.getFunctionName();
    bool throw_if_exists = !create_driver_function.if_not_exists && !create_driver_function.or_replace;
    bool replace_if_exists = create_driver_function.or_replace;

    // TODO: UserDefinedDriverFunctionFactory::instance().registerFunction(current_context, function_name, query_ptr, throw_if_exists, replace_if_exists);

    return {};
}

void registerInterpreterCreateDriverFunctionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateDriverFunctionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateDriverFunctionQuery", create_fn);
}

}
