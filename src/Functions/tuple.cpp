#include <Functions/tuple.h>

namespace DB
{

REGISTER_FUNCTION(Tuple)
{
    factory.registerFunction<FunctionTuple>();
}

}
