#include <Parsers/ASTDropFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/UserDefinedExecutableFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterDropFunctionQuery.h>
#include <Interpreters/UserDefinedSQLObjectsLoader.h>
#include <Interpreters/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
    extern const int CANNOT_DROP_FUNCTION;
}

BlockIO InterpreterDropFunctionQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    ASTDropFunctionQuery & drop_function_query = query_ptr->as<ASTDropFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    auto current_context = getContext();

    if (!drop_function_query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, current_context, access_rights_elements);

    current_context->checkAccess(access_rights_elements);

    const auto & function_name = drop_function_query.function_name;

    if (FunctionFactory::instance().hasNameOrAlias(function_name) ||
        AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, current_context))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop user defined executable function '{}'", function_name);

    std::shared_ptr<UserDefinedSQLFunctionFactory> session_user_defined_function_factory;

    if (current_context->hasSessionContext())
        session_user_defined_function_factory = current_context->getSessionContext()->getUserDefinedSQLFunctionFactory();

    auto & global_user_defined_function_factory = UserDefinedSQLFunctionFactory::instance();

    UserDefinedSQLFunctionFactory::Lock global_factory_lock = global_user_defined_function_factory.getLock();
    UserDefinedSQLFunctionFactory::Lock session_factory_lock;

    if (session_user_defined_function_factory)
        session_factory_lock = session_user_defined_function_factory->getLock();

    if (session_user_defined_function_factory && session_user_defined_function_factory->has(session_factory_lock, function_name))
        session_user_defined_function_factory->unregisterFunction(session_factory_lock, current_context, function_name);
    else if (global_user_defined_function_factory.has(global_factory_lock, function_name))
        global_user_defined_function_factory.unregisterFunction(global_factory_lock, current_context, function_name);
    else if (!drop_function_query.if_exists)
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "The function '{}' is not registered", function_name);

    return {};
}

}
