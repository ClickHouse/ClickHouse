#include <Functions/FunctionRegexpQuoteMeta.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
void registerFunctionRegexpQuoteMeta(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRegexpQuoteMeta>();
}
}
