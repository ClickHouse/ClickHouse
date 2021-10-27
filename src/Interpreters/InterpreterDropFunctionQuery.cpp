#include <Parsers/ASTDropFunctionQuery.h>

#include <Access/ContextAccess.h>
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
    extern const int UNSUPPORTED_METHOD;
}

BlockIO InterpreterDropFunctionQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    auto * drop_function_query = query_ptr->as<ASTDropFunctionQuery>();

    if (!drop_function_query)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Expected DROP FUNCTION query");

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    if (!drop_function_query->cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext(), access_rights_elements);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    auto & user_defined_functions_factory = UserDefinedSQLFunctionFactory::instance();

    if (drop_function_query->if_exists && !user_defined_functions_factory.has(drop_function_query->function_name))
        return {};

    UserDefinedSQLFunctionFactory::instance().unregisterFunction(drop_function_query->function_name);
    UserDefinedSQLObjectsLoader::instance().removeObject(current_context, UserDefinedSQLObjectType::Function, drop_function_query->function_name);

    return {};
}

}
