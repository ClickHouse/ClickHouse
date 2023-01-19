#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DataTypePtr FunctionArrayStringConcat::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() != 1 && arguments.size() != 2)
        throw Exception(
            "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                + ", should be 1 or 2.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be an array.", getName());

    if (arguments.size() == 2 && !isString(arguments[1]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be constant string.", getName());

    return std::make_shared<DataTypeString>();
}

REGISTER_FUNCTION(StringArray)
{
    factory.registerFunction<FunctionExtractAll>();

    factory.registerFunction<FunctionSplitByAlpha>();
    factory.registerAlias("splitByAlpha", FunctionSplitByAlpha::name);
    factory.registerFunction<FunctionSplitByNonAlpha>();
    factory.registerFunction<FunctionSplitByWhitespace>();
    factory.registerFunction<FunctionSplitByChar>();
    factory.registerFunction<FunctionSplitByString>();
    factory.registerFunction<FunctionSplitByRegexp>();
    factory.registerFunction<FunctionArrayStringConcat>();
}

}
