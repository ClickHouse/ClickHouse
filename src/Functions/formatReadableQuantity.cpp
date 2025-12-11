#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>
#include <Common/formatReadable.h>


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

REGISTER_FUNCTION(FormatReadableQuantity)
{
    factory.registerFunction<FunctionFormatReadable<Impl>>();
}

}
