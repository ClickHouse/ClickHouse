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
        return t != t;
    }
};

using FunctionIsNaN = FunctionNumericPredicate<IsNaNImpl>;

}

REGISTER_FUNCTION(IsNaN)
{
    factory.registerFunction<FunctionIsNaN>();
}

}
