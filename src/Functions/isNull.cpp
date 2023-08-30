
#include <Functions/isNull.h>

namespace DB
{

REGISTER_FUNCTION(IsNull)
{
    factory.registerFunction<FunctionIsNull>({}, FunctionFactory::CaseInsensitive);
}

}
