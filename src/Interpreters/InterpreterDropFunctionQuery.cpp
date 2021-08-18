#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterDropFunctionQuery.h>
#include <Interpreters/UserDefinedObjectsLoader.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTDropFunctionQuery.h>


namespace DB
{

BlockIO InterpreterDropFunctionQuery::execute()
{
    getContext()->checkAccess(AccessType::DROP_FUNCTION);
    FunctionNameNormalizer().visit(query_ptr.get());
    auto & drop_function_query = query_ptr->as<ASTDropFunctionQuery &>();
    FunctionFactory::instance().unregisterUserDefinedFunction(drop_function_query.function_name);
    UserDefinedObjectsLoader::instance().removeObject(getContext(), UserDefinedObjectType::Function, drop_function_query.function_name);

    return {};
}

}
