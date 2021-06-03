#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct TransformFromMicro
{
    static constexpr auto name = "fromUnixTimestamp64Micro";
    static constexpr auto target_scale = 6;
    using SourceDataType = DataTypeInt64;
    using ResultDataType = DataTypeDateTime64;
};

void registerFromUnixTimestamp64Micro(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUnixTimestamp64<TransformFromMicro>>();
}

}
