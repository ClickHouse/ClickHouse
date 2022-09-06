#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "like.h"


namespace DB
{

REGISTER_FUNCTION(Like)
{
    factory.registerFunction<FunctionLike>();
}

}
