#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

BlockIO InterpreterCreateFunctionQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    auto & create_function_query = query_ptr->as<ASTCreateFunctionQuery &>();
    FunctionFactory::instance().registerUserDefinedFunction(create_function_query);
    return {};
}

}
