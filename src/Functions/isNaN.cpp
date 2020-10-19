#include <Functions/FunctionNumericPredicate.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

struct IsNaNImpl
{
    static constexpr auto name = "isNaN";
    template <typename T>
    static bool execute(const T t)
    {
        /// Suppression for PVS-Studio.
        return t != t;  //-V501
    }
};

using FunctionIsNaN = FunctionNumericPredicate<IsNaNImpl>;

}

void registerFunctionIsNaN(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsNaN>();
}

}
