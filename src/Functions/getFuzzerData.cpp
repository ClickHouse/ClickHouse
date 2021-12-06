#include <Functions/getFuzzerData.h>

namespace DB
{

void registerFunctionGetFuzzerData(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetFuzzerData>();
}

}
