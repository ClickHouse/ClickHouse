#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTCreateFunctionQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

BlockIO InterpreterCreateFunctionQuery::execute()
{
    FunctionNameNormalizer::visit(query_ptr.get());
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    ASTCreateFunctionQuery & create_function_query = updated_query_ptr->as<ASTCreateFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FUNCTION);

    if (create_function_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    auto current_context = getContext();

    if (!create_function_query.cluster.empty())
    {
        if (current_context->getUserDefinedSQLObjectsStorage().isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because used-defined functions are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(updated_query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    auto function_name = create_function_query.getFunctionName();
    bool throw_if_exists = !create_function_query.if_not_exists && !create_function_query.or_replace;
    bool replace_if_exists = create_function_query.or_replace;

    UserDefinedSQLFunctionFactory::instance().registerFunction(current_context, function_name, updated_query_ptr, throw_if_exists, replace_if_exists);

    return {};
}

void registerInterpreterCreateFunctionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateFunctionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateFunctionQuery", create_fn);
}

}
