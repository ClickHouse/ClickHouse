#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringArray.h>

namespace
{
bool isNullableStringOrNullableNothing(DB::DataTypePtr type)
{
    if (type->isNullable())
    {
        const auto & nested_type = assert_cast<const DB::DataTypeNullable &>(*type).getNestedType();
        if (isString(nested_type) || isNothing(nested_type))
            return true;
    }
    return false;
}

}

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
    // An array consisting of only Null-s has type Array(Nullable(Nothing))
    if (!array_type || !(isString(array_type->getNestedType()) || isNullableStringOrNullableNothing(array_type->getNestedType())))
        throw Exception(
            "First argument for function " + getName() + " must be an array of String-s or Nullable(String)-s.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (arguments.size() == 2 && !isString(arguments[1]))
        throw Exception("Second argument for function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeString>();
}

void registerFunctionsStringArray(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractAll>();
    factory.registerFunction<FunctionAlphaTokens>();
    factory.registerFunction<FunctionSplitByNonAlpha>();
    factory.registerFunction<FunctionSplitByWhitespace>();
    factory.registerFunction<FunctionSplitByChar>();
    factory.registerFunction<FunctionSplitByString>();
    factory.registerFunction<FunctionSplitByRegexp>();
    factory.registerFunction<FunctionArrayStringConcat>();
}

}
