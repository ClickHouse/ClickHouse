#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>


namespace DB
{

namespace
{
    struct Impl
    {
        static constexpr auto name = "formatReadableSize";

        static void format(double value, DB::WriteBuffer & out)
        {
            formatReadableSizeWithBinarySuffix(value, out);
        }
    };
}

void registerFunctionFormatReadableSize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFormatReadable<Impl>>();
}

}
