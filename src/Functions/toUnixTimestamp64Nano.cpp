#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct TransformToNano
{
    static constexpr auto name = "toUnixTimestamp64Nano";
    static constexpr auto target_scale = 9;
    using SourceDataType = DataTypeDateTime64;
    using ResultDataType = DataTypeInt64;
};

}

void registerToUnixTimestamp64Nano(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUnixTimestamp64<TransformToNano>>();
}

}
