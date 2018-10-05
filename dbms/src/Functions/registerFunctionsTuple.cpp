#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFunctionTuple(FunctionFactory &);
void registerFunctionTupleElement(FunctionFactory &);

void registerFunctionsTuple(FunctionFactory & factory)
{
    registerFunctionTuple(factory);
    registerFunctionTupleElement(factory);
}

}
