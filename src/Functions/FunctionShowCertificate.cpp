#include "FunctionShowCertificate.h"
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ShowCertificate)
{
    factory.registerFunction<FunctionShowCertificate>();
}

}
