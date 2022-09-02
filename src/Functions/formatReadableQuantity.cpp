#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>


namespace DB
{

namespace
{
    struct Impl
    {
        static constexpr auto name = "formatReadableQuantity";

        static void format(double value, DB::WriteBuffer & out)
        {
            formatReadableQuantity(value, out);
        }
    };
}

void registerFunctionFormatReadableQuantity(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFormatReadable<Impl>>();
}

}
