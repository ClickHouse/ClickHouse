#include <Functions/FunctionStringToString.h>
#include <Functions/lemmatizeImpl.h>
#include <Functions/FunctionFactory.h>
//#include <Poco/Unicode.h>


namespace DB
{
namespace
{

struct NameLemmatize
{
    static constexpr auto name = "stem_en";
};

using FunctionLemmatize = FunctionStringToString<LemmatizeImpl, NameLemmatize>;

}

void registerFunctionLemmatize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLemmatize>();
}

}
