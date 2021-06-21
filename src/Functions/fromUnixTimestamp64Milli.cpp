#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct TransformFromMilli
{
    static constexpr auto name = "fromUnixTimestamp64Milli";
    static constexpr auto target_scale = 3;
    using SourceDataType = DataTypeInt64;
    using ResultDataType = DataTypeDateTime64;
};

void registerFromUnixTimestamp64Milli(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUnixTimestamp64<TransformFromMilli>>();
}

}
