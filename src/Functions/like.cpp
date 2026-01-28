#include "like.h"
#include "FunctionFactory.h"


namespace DB
{

REGISTER_FUNCTION(Like)
{
    factory.registerFunction<FunctionLike>();
}

}
