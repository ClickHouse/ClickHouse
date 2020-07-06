#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct TransformToMicro
{
    static constexpr auto name = "toUnixTimestamp64Micro";
    static constexpr auto target_scale = 6;
    using SourceDataType = DataTypeDateTime64;
    using ResultDataType = DataTypeInt64;
};

void registerToUnixTimestamp64Micro(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUnixTimestamp64<TransformToMicro>>();
}

}
