#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/ASTDropFunctionQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

BlockIO InterpreterDropFunctionQuery::execute()
{
    FunctionNameNormalizer::visit(query_ptr.get());

    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    ASTDropFunctionQuery & drop_function_query = updated_query_ptr->as<ASTDropFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    auto current_context = getContext();

    if (!drop_function_query.cluster.empty())
    {
        if (current_context->getUserDefinedSQLObjectsStorage().isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because used-defined functions are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(updated_query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    bool throw_if_not_exists = !drop_function_query.if_exists;

    UserDefinedSQLFunctionFactory::instance().unregisterFunction(current_context, drop_function_query.function_name, throw_if_not_exists);

    return {};
}

void registerInterpreterDropFunctionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropFunctionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropFunctionQuery", create_fn);
}

}
