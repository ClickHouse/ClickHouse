#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
}

FunctionFactory::FunctionFactory()
{
}


FunctionPtr FunctionFactory::get(
    const std::string & name,
    const Context & context) const
{
    auto res = tryGet(name, context);
    if (!res)
        throw Exception("Unknown function " + name, ErrorCodes::UNKNOWN_FUNCTION);
    return res;
}


FunctionPtr FunctionFactory::tryGet(
    const std::string & name,
    const Context & context) const
{
    auto it = functions.find(name);
    if (functions.end() != it)
        return it->second(context);
    else
        return {};
}

}
