#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct TransformFromNano
{
    static constexpr auto name = "fromUnixTimestamp64Nano";
    static constexpr auto target_scale = 9;
    using SourceDataType = DataTypeInt64;
    using ResultDataType = DataTypeDateTime64;
};

void registerFromUnixTimestamp64Nano(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUnixTimestamp64<TransformFromNano>>();
}

}
