#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterDropFunctionQuery.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTDropFunctionQuery.h>

namespace DB
{

BlockIO InterpreterDropFunctionQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    auto & drop_function_query = query_ptr->as<ASTDropFunctionQuery &>();
    FunctionFactory::instance().unregisterUserDefinedFunction(drop_function_query.function_name);
    return {};
}

}
