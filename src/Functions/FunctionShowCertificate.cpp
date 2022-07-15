#include "FunctionShowCertificate.h"
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFunctionShowCertificate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionShowCertificate>();
}

}
