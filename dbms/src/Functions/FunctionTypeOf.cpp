#include <Functions/FunctionTypeOf.h>

#include <Functions/FunctionFactory.h>


namespace DB
{

struct NameTypeOf {
    static constexpr auto name {"typeOf"};
};

void registerFunctionTypeOf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTypeOf<NameTypeOf>>(
        FunctionFactory::CaseInsensitive
    );
}

}
