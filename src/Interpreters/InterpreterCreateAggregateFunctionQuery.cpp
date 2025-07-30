#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateAggregateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedSQLAggregateFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTCreateAggregateFunctionQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

BlockIO InterpreterCreateAggregateFunctionQuery::execute()
{
    FunctionNameNormalizer::visit(query_ptr.get());
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    ASTCreateAggregateFunctionQuery & create_function_query = updated_query_ptr->as<ASTCreateAggregateFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_AGGREGATE_FUNCTION);

    if (create_function_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_AGGREGATE_FUNCTION);

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

    UserDefinedSQLAggregateFunctionFactory::instance().registerFunction(current_context, function_name, updated_query_ptr, throw_if_exists, replace_if_exists);

    return {};
}

void registerInterpreterCreateAggregateFunctionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateAggregateFunctionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateAggregateFunctionQuery", create_fn);
}

}
