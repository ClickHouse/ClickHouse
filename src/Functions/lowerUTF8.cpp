#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperUTF8Impl.h>
#include <Functions/FunctionFactory.h>
#include <Poco/Unicode.h>


namespace DB
{

struct NameLowerUTF8
{
    static constexpr auto name = "lowerUTF8";
};

using FunctionLowerUTF8 = FunctionStringToString<LowerUpperUTF8Impl<'A', 'Z', Poco::Unicode::toLower, UTF8CyrillicToCase<true>>, NameLowerUTF8>;

void registerFunctionLowerUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLowerUTF8>();
}

}
