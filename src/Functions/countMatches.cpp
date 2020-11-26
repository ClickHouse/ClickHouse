#include "FunctionFactory.h"
#include "countMatchesImpl.h"


namespace DB
{

void registerFunctionCountMatches(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountMatches>(FunctionFactory::CaseInsensitive);
}

}
