#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct TransformToMilli
{
    static constexpr auto name = "toUnixTimestamp64Milli";
    static constexpr auto target_scale = 3;
    using SourceDataType = DataTypeDateTime64;
    using ResultDataType = DataTypeInt64;
};

}

void registerToUnixTimestamp64Milli(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUnixTimestamp64<TransformToMilli>>();
}

}
