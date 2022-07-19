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

BlockIO InterpreterDropFunctionQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    ASTDropFunctionQuery & drop_function_query = query_ptr->as<ASTDropFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    if (!drop_function_query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    UserDefinedSQLFunctionFactory::instance().unregisterFunction(current_context, drop_function_query.function_name, drop_function_query.if_exists);

    return {};
}

}
