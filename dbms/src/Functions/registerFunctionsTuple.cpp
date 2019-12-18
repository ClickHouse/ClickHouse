#include "registerFunctions.h"
namespace DB
{
void registerFunctionsTuple(FunctionFactory & factory)
{
    registerFunctionTuple(factory);
    registerFunctionTupleElement(factory);
}

}
