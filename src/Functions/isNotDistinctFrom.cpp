#include <Functions/isNotDistinctFrom.h>


namespace DB
{

REGISTER_FUNCTION(IsNotDistinctFrom)
{
    factory.registerFunction<FunctionIsNotDistinctFrom>();
}

}
