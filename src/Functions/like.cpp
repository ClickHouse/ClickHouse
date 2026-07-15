#include <Functions/like.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(Like)
{
    factory.registerFunction<FunctionLike>();
}

}
