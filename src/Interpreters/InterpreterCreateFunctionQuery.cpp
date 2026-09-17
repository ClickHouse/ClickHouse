#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

template <typename T>
std::optional<BlockIO> tryExecute(const ASTPtr & query_ptr, ContextMutablePtr current_context)
{
    if (std::is_same_v<ASTCreateSQLFunctionQuery, T>)
    {
        /// Normalize function names in substituted SQL expression
        FunctionNameNormalizer::visit(query_ptr.get());
    }
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, current_context);
    auto * create_function_query = updated_query_ptr->as<T>();
    if (!create_function_query)
        return std::nullopt;

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FUNCTION);

    if (create_function_query->or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);


    if (!create_function_query->cluster.empty())
    {
        if (current_context->getUserDefinedSQLObjectsStorage().isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because used-defined functions are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(updated_query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    auto function_name = create_function_query->getFunctionName();
    bool throw_if_exists = !create_function_query->if_not_exists && !create_function_query->or_replace;
    bool replace_if_exists = create_function_query->or_replace;

    UserDefinedSQLFunctionFactory::instance().registerFunction(current_context, function_name, updated_query_ptr, throw_if_exists, replace_if_exists);

    return BlockIO();
}


BlockIO InterpreterCreateFunctionQuery::execute()
{
    if (auto res = tryExecute<ASTCreateSQLFunctionQuery>(query_ptr, getContext()))
        return std::move(res.value());

    if (auto res = tryExecute<ASTCreateWasmFunctionQuery>(query_ptr, getContext()))
        return std::move(res.value());

    throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot execute query, got unexpected AST type: {}", query_ptr->getID());
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
